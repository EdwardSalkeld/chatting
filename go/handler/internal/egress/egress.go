package egress

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/dispatch"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

type TaskRecord = sqlitestate.TaskLedgerRecord

type StagedRecord = sqlitestate.StagedEgressRecord

type State interface {
	GetTask(ctx context.Context, taskID string) (*TaskRecord, error)
	IsTaskCompleted(ctx context.Context, taskID string, envelopeID string) (bool, error)
	MarkTaskCompleted(ctx context.Context, taskID string, envelopeID string, traceID string) error
	HasDispatchedEventID(ctx context.Context, taskID string, eventID string) (bool, error)
	MarkDispatchedEventID(ctx context.Context, taskID string, eventID string) error
	StageEgressEvent(ctx context.Context, message contracts.EgressQueueMessage) error
	ExpectedSequence(ctx context.Context, taskID string) (int, error)
	GetStagedEventBySequence(ctx context.Context, taskID string, sequence int) (*StagedRecord, error)
	MarkStagedEventDispatched(ctx context.Context, taskID string, eventID string, sequence int) error
}

type Dispatcher interface {
	Dispatch(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error)
}

type DispatcherFunc func(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error)

func (fn DispatcherFunc) Dispatch(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error) {
	return fn(ctx, message, envelope)
}

type Engine struct {
	// mu serializes handling so the BBMB drain loop and the synchronous HTTP
	// submit endpoint can share one engine without racing on the state store.
	mu              sync.Mutex
	state           State
	dispatcher      Dispatcher
	allowedChannels map[string]bool
	onCompletion    func(context.Context, contracts.EgressQueueMessage) error
	onDrop          func(context.Context, contracts.EgressQueueMessage, string)
}

type Config struct {
	AllowedChannels []string
	// OnCompletion runs once a task's completion event has been applied.
	OnCompletion func(context.Context, contracts.EgressQueueMessage) error
	// OnDrop is invoked whenever the engine drops an egress message for an
	// error-class reason (bad payload, unknown task, disallowed channel, dispatch
	// failure). Dropping is treated as an operational error, so the engine always
	// logs a loud line; the hook is responsible for the operator alert (email). It
	// is NOT called for idempotency drops (a late/duplicate event after task
	// completion).
	OnDrop func(context.Context, contracts.EgressQueueMessage, string)
}

func New(state State, dispatcher Dispatcher, config Config) (*Engine, error) {
	if state == nil {
		return nil, errors.New("state is required")
	}
	if dispatcher == nil {
		return nil, errors.New("dispatcher is required")
	}
	allowedChannels := make(map[string]bool, len(config.AllowedChannels))
	for _, channel := range config.AllowedChannels {
		allowedChannels[channel] = true
	}
	return &Engine{
		state:           state,
		dispatcher:      dispatcher,
		allowedChannels: allowedChannels,
		onCompletion:    config.OnCompletion,
		onDrop:          config.OnDrop,
	}, nil
}

type Result struct {
	Status   string
	Reason   string
	Metadata map[string]any
}

const (
	StatusDispatched = "dispatched"
	StatusStaged     = "staged"
	StatusCompleted  = "completed"
	StatusDropped    = "dropped"
	StatusDeduped    = "deduped"
)

// HandleRaw decodes a raw egress payload and dispatches it. Safe for concurrent
// callers: handling is serialized so the BBMB drain loop and the HTTP submit
// endpoint can share one engine.
func (engine *Engine) HandleRaw(ctx context.Context, raw []byte) (Result, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	message, err := contracts.DecodeEgressQueueMessage(raw)
	if err != nil {
		return engine.surfaceDrop(ctx, message, "invalid_payload")
	}
	return engine.handleLocked(ctx, message)
}

// Handle validates and dispatches a decoded egress message, serialized against
// other callers.
func (engine *Engine) Handle(ctx context.Context, message contracts.EgressQueueMessage) (Result, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.handleLocked(ctx, message)
}

func (engine *Engine) handleLocked(ctx context.Context, message contracts.EgressQueueMessage) (Result, error) {
	if err := message.Validate(); err != nil {
		return engine.surfaceDrop(ctx, message, "invalid_payload")
	}

	completed, err := engine.state.IsTaskCompleted(ctx, message.TaskID, message.EnvelopeID)
	if err != nil {
		return Result{}, err
	}
	if completed {
		// Idempotency, not an error: a late or duplicate event after the task already
		// completed. Log plainly and do not raise the operator alert.
		log.Printf("egress_dropped_after_completion task_id=%s event_id=%s", message.TaskID, message.EventID)
		return Result{Status: StatusDropped, Reason: "completed_task"}, nil
	}

	task, err := engine.state.GetTask(ctx, message.TaskID)
	if err != nil {
		return Result{}, err
	}
	if task == nil || task.EnvelopeID != message.EnvelopeID {
		return engine.surfaceDrop(ctx, message, "unknown_task")
	}

	if !engine.channelAllowed(message, task) {
		return engine.surfaceDrop(ctx, message, "disallowed_channel")
	}

	dispatched, err := engine.state.HasDispatchedEventID(ctx, message.TaskID, message.EventID)
	if err != nil {
		return Result{}, err
	}
	if dispatched {
		return Result{Status: StatusDeduped}, nil
	}

	if message.Sequence == nil {
		if message.EventKind == "completion" {
			return engine.surfaceDrop(ctx, message, "invalid_payload")
		}
		dispatchedMessage, err := engine.dispatchAndMark(ctx, task, message)
		if err != nil {
			if reason, ok := dispatchFailureReason(err); ok {
				return engine.dropFailedDispatch(ctx, message, reason)
			}
			return Result{}, err
		}
		return Result{Status: StatusDispatched, Metadata: outboundMetadata(dispatchedMessage)}, nil
	}

	if err := engine.state.StageEgressEvent(ctx, message); err != nil {
		return Result{}, err
	}
	result, err := engine.Flush(ctx, message.TaskID)
	if err != nil {
		return Result{}, err
	}
	if result.Status == "" {
		return Result{Status: StatusStaged}, nil
	}
	return result, nil
}

func (engine *Engine) Flush(ctx context.Context, taskID string) (Result, error) {
	task, err := engine.state.GetTask(ctx, taskID)
	if err != nil {
		return Result{}, err
	}
	if task == nil {
		return Result{}, nil
	}

	var last Result
	for {
		expected, err := engine.state.ExpectedSequence(ctx, taskID)
		if err != nil {
			return Result{}, err
		}
		staged, err := engine.state.GetStagedEventBySequence(ctx, taskID, expected)
		if err != nil {
			return Result{}, err
		}
		if staged == nil {
			return last, nil
		}

		dispatched, err := engine.state.HasDispatchedEventID(ctx, taskID, staged.EventID)
		if err != nil {
			return Result{}, err
		}
		if dispatched {
			if err := engine.state.MarkStagedEventDispatched(ctx, taskID, staged.EventID, staged.Sequence); err != nil {
				return Result{}, err
			}
			last = Result{Status: StatusDeduped}
			continue
		}

		message := staged.EgressMessage
		if message.EventKind == "completion" {
			if err := engine.state.MarkStagedEventDispatched(ctx, taskID, staged.EventID, staged.Sequence); err != nil {
				return Result{}, err
			}
			if err := engine.state.MarkDispatchedEventID(ctx, taskID, staged.EventID); err != nil {
				return Result{}, err
			}
			if err := engine.state.MarkTaskCompleted(ctx, message.TaskID, message.EnvelopeID, message.TraceID); err != nil {
				return Result{}, err
			}
			if engine.onCompletion != nil {
				if err := engine.onCompletion(ctx, message); err != nil {
					return Result{}, err
				}
			}
			return Result{Status: StatusCompleted}, nil
		}

		if !engine.channelAllowed(message, task) {
			// A disallowed channel is a loud drop, not a fatal error: surface it and
			// advance past the staged event so the flush is not stuck crash-looping.
			result, err := engine.surfaceDrop(ctx, message, "disallowed_channel")
			if err != nil {
				return Result{}, err
			}
			if err := engine.state.MarkStagedEventDispatched(ctx, taskID, staged.EventID, staged.Sequence); err != nil {
				return Result{}, err
			}
			last = result
			continue
		}
		dispatchedMessage, err := engine.dispatchAndMark(ctx, task, message)
		if err != nil {
			reason, ok := dispatchFailureReason(err)
			if !ok {
				return Result{}, err
			}
			if _, err := engine.dropFailedDispatch(ctx, message, reason); err != nil {
				return Result{}, err
			}
			if err := engine.state.MarkStagedEventDispatched(ctx, taskID, staged.EventID, staged.Sequence); err != nil {
				return Result{}, err
			}
			last = Result{Status: StatusDropped, Reason: "dispatch_failed"}
			continue
		}
		if err := engine.state.MarkStagedEventDispatched(ctx, taskID, staged.EventID, staged.Sequence); err != nil {
			return Result{}, err
		}
		last = Result{Status: StatusDispatched, Metadata: outboundMetadata(dispatchedMessage)}
	}
}

// surfaceDrop makes an error-class drop loud: it emits a structured ERROR log line
// (with a short body preview when present) and invokes the drop hook so the operator is
// alerted. Every error-class drop must go through here so the engine logs uniformly for
// all of them. It always reports the event as dropped and never returns an error.
func (engine *Engine) surfaceDrop(ctx context.Context, message contracts.EgressQueueMessage, reason string) (Result, error) {
	log.Printf("egress_message_dropped task_id=%s event_id=%s channel=%s target=%s event_kind=%s reason=%s%s",
		message.TaskID, message.EventID, message.Message.Channel, message.Message.Target, message.EventKind, reason, egressBodyPreview(message))
	if engine.onDrop != nil {
		engine.onDrop(ctx, message, reason)
	}
	return Result{Status: StatusDropped, Reason: reason}, nil
}

// egressBodyPreview returns a truncated, single-line preview of the message body for
// logging, or "" when there is no body. It never appears without a leading space.
func egressBodyPreview(message contracts.EgressQueueMessage) string {
	if message.Message.Body == nil {
		return ""
	}
	body := strings.TrimSpace(*message.Message.Body)
	if body == "" {
		return ""
	}
	body = strings.Join(strings.Fields(body), " ")
	const maxLen = 120
	if runes := []rune(body); len(runes) > maxLen {
		body = string(runes[:maxLen]) + "..."
	}
	return fmt.Sprintf(" body=%q", body)
}

// dropFailedDispatch handles a permanent per-message dispatch failure: it surfaces the
// drop (loud log + operator alert via the hook), records the event as dispatched so it
// is not retried, and reports the event as dropped. The egress message is acked by the
// caller, so a single bad event no longer crash-loops the handler. reasonCode carries
// the upstream API description for the log/alert; the Result reason stays "dispatch_failed"
// so metrics bucket dispatch failures together.
func (engine *Engine) dropFailedDispatch(ctx context.Context, message contracts.EgressQueueMessage, reasonCode string) (Result, error) {
	if _, err := engine.surfaceDrop(ctx, message, reasonCode); err != nil {
		return Result{}, err
	}
	if err := engine.state.MarkDispatchedEventID(ctx, message.TaskID, message.EventID); err != nil {
		return Result{}, err
	}
	return Result{Status: StatusDropped, Reason: "dispatch_failed"}, nil
}

// dispatchFailureReason reports whether err is a per-message dispatch failure (as
// opposed to an infrastructure error that should still abort the run) and returns the
// reason code, which carries any upstream API description.
func dispatchFailureReason(err error) (string, bool) {
	var dispatchErr dispatch.MessageDispatchError
	if errors.As(err, &dispatchErr) {
		return dispatchErr.ReasonCode, true
	}
	return "", false
}

func (engine *Engine) dispatchAndMark(ctx context.Context, task *TaskRecord, message contracts.EgressQueueMessage) (*contracts.OutboundMessage, error) {
	dispatched, err := engine.dispatcher.Dispatch(ctx, message.Message, task.TaskMessage.Envelope)
	if err != nil {
		return nil, err
	}
	if err := engine.state.MarkDispatchedEventID(ctx, message.TaskID, message.EventID); err != nil {
		return nil, err
	}
	return dispatched, nil
}

func outboundMetadata(message *contracts.OutboundMessage) map[string]any {
	if message == nil || len(message.Metadata) == 0 {
		return nil
	}
	return message.Metadata
}

func (engine *Engine) channelAllowed(message contracts.EgressQueueMessage, task *TaskRecord) bool {
	if message.EventKind == "completion" {
		return true
	}
	if task != nil && heartbeat.IsLogPong(message.Message, task.TaskMessage.Envelope) {
		return true
	}
	if task != nil && message.Message.Channel == "final" {
		return engine.allowedChannels[task.TaskMessage.Envelope.ReplyChannel.Type]
	}
	return engine.allowedChannels[message.Message.Channel]
}
