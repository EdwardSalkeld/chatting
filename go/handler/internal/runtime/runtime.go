package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
)

const (
	TaskQueueName = "chatting.tasks.v1"
	// Attachments a task never claimed are reclaimed after this long.
	telegramAttachmentMaxAge = 30 * 24 * time.Hour
)

type Broker interface {
	EnsureQueue(ctx context.Context, queueName string) error
	PublishJSON(ctx context.Context, queueName string, payload map[string]any) (string, error)
}

type IngressState interface {
	Seen(ctx context.Context, source string, dedupeKey string) (bool, error)
	MarkSeen(ctx context.Context, source string, dedupeKey string) error
	RecordTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) error
	AssignTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) (contracts.TaskQueueMessage, error)
	RecordTelegramTaskAttachments(ctx context.Context, taskMessage contracts.TaskQueueMessage, attachmentRootDir string) (int, error)
	CleanupTelegramAttachmentsForRuntime(ctx context.Context, attachmentRootDir string, notAfter time.Time, maxAgeCutoff time.Time) error
}

type Connector interface {
	Poll(ctx context.Context) ([]contracts.TaskEnvelope, error)
}

type AckingConnector interface {
	Connector
	AckEnvelope(ctx context.Context, envelopeID string) error
}

type Runner struct {
	config       handlerconfig.Config
	broker       Broker
	ingressState IngressState
	connectors   []Connector
	metrics      *metrics.Recorder
	now          func() time.Time
	sleep        func(context.Context, time.Duration) error
}

// NewRunner builds the ingress loop. state may be nil only when there are no
// connectors; recorder may be nil to skip metrics.
func NewRunner(config handlerconfig.Config, broker Broker, state IngressState, connectors []Connector, recorder *metrics.Recorder) (*Runner, error) {
	if broker == nil {
		return nil, errors.New("broker is required")
	}
	if len(connectors) > 0 && state == nil {
		return nil, errors.New("ingress state is required when connectors are configured")
	}
	for _, connector := range connectors {
		if connector == nil {
			return nil, errors.New("connector is required")
		}
	}
	return &Runner{
		config:       config,
		broker:       broker,
		ingressState: state,
		connectors:   connectors,
		metrics:      recorder,
		now:          func() time.Time { return time.Now().UTC() },
		sleep:        sleep,
	}, nil
}

func (runner *Runner) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := runner.broker.EnsureQueue(ctx, TaskQueueName); err != nil {
		return err
	}

	loopCount := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		loopCount++
		published, err := runner.PublishIngress(ctx)
		if err != nil {
			if isGracefulContextStop(ctx, err) {
				return nil
			}
			return err
		}
		if err := runner.cleanupTelegramAttachments(ctx); err != nil {
			if isGracefulContextStop(ctx, err) {
				return nil
			}
			return err
		}
		if runner.metrics != nil {
			runner.metrics.RecordLoop(published, runner.now())
		}
		if runner.config.MaxLoops > 0 && loopCount >= runner.config.MaxLoops {
			return nil
		}
		if err := runner.sleep(ctx, durationFromSeconds(runner.config.PollIntervalSeconds)); err != nil {
			return nil
		}
	}
}

func isGracefulContextStop(ctx context.Context, err error) bool {
	if err == nil || ctx == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (runner *Runner) cleanupTelegramAttachments(ctx context.Context) error {
	if !runner.config.TelegramEnabled || runner.ingressState == nil {
		return nil
	}
	now := runner.now()
	return runner.ingressState.CleanupTelegramAttachmentsForRuntime(
		ctx,
		runner.config.TelegramAttachmentDir,
		now,
		now.Add(-telegramAttachmentMaxAge),
	)
}

func (runner *Runner) PublishIngress(ctx context.Context) (int, error) {
	if len(runner.connectors) == 0 {
		return 0, nil
	}
	published := 0
	for _, connector := range runner.connectors {
		envelopes, err := connector.Poll(ctx)
		if err != nil {
			return published, err
		}
		for _, envelope := range envelopes {
			seen, err := runner.ingressState.Seen(ctx, envelope.Source, envelope.DedupeKey)
			if err != nil {
				return published, err
			}
			if seen {
				if err := ackEnvelope(ctx, connector, envelope.ID); err != nil {
					return published, err
				}
				continue
			}
			taskMessage := contracts.NewTaskQueueMessage(
				envelope,
				"trace:"+envelope.ID,
				runner.now(),
			)
			taskMessage, err = runner.ingressState.AssignTask(ctx, taskMessage)
			if err != nil {
				return published, err
			}
			if err := runner.ingressState.RecordTask(ctx, taskMessage); err != nil {
				return published, err
			}
			if envelope.ReplyChannel.Type == "telegram" && len(envelope.Attachments) > 0 {
				if _, err := runner.ingressState.RecordTelegramTaskAttachments(ctx, taskMessage, runner.config.TelegramAttachmentDir); err != nil {
					return published, err
				}
			}
			payload, err := taskMessageMap(taskMessage)
			if err != nil {
				return published, err
			}
			if _, err := runner.broker.PublishJSON(ctx, TaskQueueName, payload); err != nil {
				return published, err
			}
			if err := runner.ingressState.MarkSeen(ctx, envelope.Source, envelope.DedupeKey); err != nil {
				return published, err
			}
			if err := ackEnvelope(ctx, connector, envelope.ID); err != nil {
				return published, err
			}
			published++
		}
	}
	return published, nil
}

func ackEnvelope(ctx context.Context, connector Connector, envelopeID string) error {
	acking, ok := connector.(AckingConnector)
	if !ok {
		return nil
	}
	return acking.AckEnvelope(ctx, envelopeID)
}

func taskMessageMap(taskMessage contracts.TaskQueueMessage) (map[string]any, error) {
	encoded, err := json.Marshal(taskMessage)
	if err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func durationFromSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}
