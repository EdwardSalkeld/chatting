package egress

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/dispatch"
)

type TaskRecord struct {
	TaskID      string
	EnvelopeID  string
	TraceID     string
	TaskMessage contracts.TaskQueueMessage
}

type StagedRecord struct {
	TaskID        string
	EventID       string
	Sequence      int
	EgressMessage contracts.EgressQueueMessage
}

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

type TelegramConversationState interface {
	AppendConversationTurn(ctx context.Context, channel string, target string, role string, content string, runID string) error
}

type Dispatcher interface {
	Dispatch(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error)
}

type DispatcherFunc func(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error)

func (fn DispatcherFunc) Dispatch(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error) {
	return fn(ctx, message, envelope)
}

type Engine struct {
	state             State
	dispatcher        Dispatcher
	allowedChannels   map[string]bool
	onCompletion      func(context.Context, contracts.EgressQueueMessage) error
	onDispatchFailure func(context.Context, contracts.EgressQueueMessage, string)
}

type Option func(*Engine)

func WithAllowedChannels(channels []string) Option {
	return func(engine *Engine) {
		engine.allowedChannels = make(map[string]bool, len(channels))
		for _, channel := range channels {
			engine.allowedChannels[channel] = true
		}
	}
}

func WithCompletionHook(hook func(context.Context, contracts.EgressQueueMessage) error) Option {
	return func(engine *Engine) {
		engine.onCompletion = hook
	}
}

// WithDispatchFailureHook registers a callback invoked when a single egress event
// fails to dispatch. The event is dropped (acked) rather than crashing the handler,
// so the hook is responsible for making the failure visible (log + operator email).
func WithDispatchFailureHook(hook func(context.Context, contracts.EgressQueueMessage, string)) Option {
	return func(engine *Engine) {
		engine.onDispatchFailure = hook
	}
}

func New(state State, dispatcher Dispatcher, options ...Option) (*Engine, error) {
	if state == nil {
		return nil, errors.New("state is required")
	}
	if dispatcher == nil {
		return nil, errors.New("dispatcher is required")
	}
	engine := &Engine{
		state:           state,
		dispatcher:      dispatcher,
		allowedChannels: map[string]bool{},
	}
	for _, option := range options {
		option(engine)
	}
	return engine, nil
}

type Result struct {
	Status string
	Reason string
}

const (
	StatusDispatched = "dispatched"
	StatusStaged     = "staged"
	StatusCompleted  = "completed"
	StatusDropped    = "dropped"
	StatusDeduped    = "deduped"
)

func (engine *Engine) HandleRaw(ctx context.Context, raw []byte) (Result, error) {
	message, err := contracts.DecodeEgressQueueMessage(raw)
	if err != nil {
		return Result{Status: StatusDropped, Reason: "invalid_payload"}, nil
	}
	return engine.Handle(ctx, message)
}

func (engine *Engine) Handle(ctx context.Context, message contracts.EgressQueueMessage) (Result, error) {
	if err := message.Validate(); err != nil {
		return Result{Status: StatusDropped, Reason: "invalid_payload"}, nil
	}

	completed, err := engine.state.IsTaskCompleted(ctx, message.TaskID, message.EnvelopeID)
	if err != nil {
		return Result{}, err
	}
	if completed {
		return Result{Status: StatusDropped, Reason: "completed_task"}, nil
	}

	task, err := engine.state.GetTask(ctx, message.TaskID)
	if err != nil {
		return Result{}, err
	}
	if task == nil || task.EnvelopeID != message.EnvelopeID {
		return Result{Status: StatusDropped, Reason: "unknown_task"}, nil
	}

	if !engine.channelAllowed(message, task) {
		return Result{Status: StatusDropped, Reason: "disallowed_channel"}, nil
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
			return Result{Status: StatusDropped, Reason: "invalid_payload"}, nil
		}
		if err := engine.dispatchAndMark(ctx, task, message); err != nil {
			if reason, ok := dispatchFailureReason(err); ok {
				return engine.dropFailedDispatch(ctx, message, reason)
			}
			return Result{}, err
		}
		return Result{Status: StatusDispatched}, nil
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
			return Result{}, fmt.Errorf("staged event %s has disallowed channel %q", staged.EventID, message.Message.Channel)
		}
		if err := engine.dispatchAndMark(ctx, task, message); err != nil {
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
		last = Result{Status: StatusDispatched}
	}
}

// dropFailedDispatch handles a permanent per-message dispatch failure: it surfaces
// the failure (log + operator email via the hook), records the event as dispatched so
// it is not retried, and reports the event as dropped. The egress message is acked by
// the caller, so a single bad event no longer crash-loops the handler.
func (engine *Engine) dropFailedDispatch(ctx context.Context, message contracts.EgressQueueMessage, reasonCode string) (Result, error) {
	if engine.onDispatchFailure != nil {
		engine.onDispatchFailure(ctx, message, reasonCode)
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

func (engine *Engine) dispatchAndMark(ctx context.Context, task *TaskRecord, message contracts.EgressQueueMessage) error {
	dispatched, err := engine.dispatcher.Dispatch(ctx, message.Message, task.TaskMessage.Envelope)
	if err != nil {
		return err
	}
	if memoryState, ok := engine.state.(TelegramConversationState); ok {
		if err := maybeRecordTelegramConversationTurn(ctx, memoryState, task, dispatched, message.TaskID); err != nil {
			return err
		}
	}
	return engine.state.MarkDispatchedEventID(ctx, message.TaskID, message.EventID)
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

func maybeRecordTelegramConversationTurn(ctx context.Context, state TelegramConversationState, task *TaskRecord, dispatched *contracts.OutboundMessage, runID string) error {
	if task == nil || dispatched == nil {
		return nil
	}
	if task.TaskMessage.Envelope.ReplyChannel.Type != "telegram" {
		return nil
	}
	if dispatched.Channel != "telegram" || dispatched.Target != task.TaskMessage.Envelope.ReplyChannel.Target {
		return nil
	}
	content, ok := telegramConversationContent(*dispatched)
	if !ok {
		return nil
	}
	return state.AppendConversationTurn(ctx, "telegram", dispatched.Target, "assistant", content, runID)
}

func telegramConversationContent(message contracts.OutboundMessage) (string, bool) {
	if message.Body != nil {
		trimmed := strings.TrimSpace(*message.Body)
		if trimmed != "" {
			return trimmed, true
		}
	}
	if message.Attachment == nil {
		return "", false
	}
	name := ""
	if message.Attachment.Name != nil {
		name = strings.TrimSpace(*message.Attachment.Name)
	}
	if name == "" {
		name = filepath.Base(message.Attachment.URI)
	}
	if name == "" || name == "." || name == "/" {
		name = message.Attachment.URI
	}
	return "[Attachment sent: " + name + "]", true
}
