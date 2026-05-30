package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
)

const (
	TaskQueueName           = "chatting.tasks.v1"
	EgressQueueName         = "chatting.egress.v1"
	egressPickupWaitSeconds = 5
	egressDrainWaitSeconds  = 0
)

type Broker interface {
	EnsureQueue(ctx context.Context, queueName string) error
	PublishJSON(ctx context.Context, queueName string, payload map[string]any) (string, error)
	PickupJSON(ctx context.Context, queueName string, timeoutSeconds int, waitSeconds int) (*bbmb.PickedMessage, error)
	Ack(ctx context.Context, queueName string, guid string) error
}

type EgressHandler interface {
	HandleRaw(ctx context.Context, raw []byte) error
}

type IngressState interface {
	Seen(ctx context.Context, source string, dedupeKey string) (bool, error)
	MarkSeen(ctx context.Context, source string, dedupeKey string) error
	RecordTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) error
}

type TelegramAttachmentIngressState interface {
	RecordTelegramTaskAttachments(ctx context.Context, taskMessage contracts.TaskQueueMessage, attachmentRootDir string) (int, error)
}

type TelegramAttachmentCleanupState interface {
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
	egress       EgressHandler
	ingressState IngressState
	connectors   []Connector
	metrics      *metrics.Recorder
	now          func() time.Time
	sleep        func(context.Context, time.Duration) error
}

type Option func(*Runner)

func WithIngress(state IngressState, connectors ...Connector) Option {
	return func(runner *Runner) {
		runner.ingressState = state
		runner.connectors = append(runner.connectors, connectors...)
	}
}

func WithNow(now func() time.Time) Option {
	return func(runner *Runner) {
		if now != nil {
			runner.now = now
		}
	}
}

func WithMetrics(recorder *metrics.Recorder) Option {
	return func(runner *Runner) {
		runner.metrics = recorder
	}
}

func NewRunner(config handlerconfig.Config, broker Broker, egress EgressHandler, options ...Option) (*Runner, error) {
	if broker == nil {
		return nil, errors.New("broker is required")
	}
	if egress == nil {
		return nil, errors.New("egress handler is required")
	}
	runner := &Runner{
		config: config,
		broker: broker,
		egress: egress,
		now:    func() time.Time { return time.Now().UTC() },
		sleep:  sleep,
	}
	for _, option := range options {
		option(runner)
	}
	if len(runner.connectors) > 0 && runner.ingressState == nil {
		return nil, errors.New("ingress state is required when connectors are configured")
	}
	for _, connector := range runner.connectors {
		if connector == nil {
			return nil, errors.New("connector is required")
		}
	}
	return runner, nil
}

func (runner *Runner) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := runner.broker.EnsureQueue(ctx, TaskQueueName); err != nil {
		return err
	}
	if err := runner.broker.EnsureQueue(ctx, EgressQueueName); err != nil {
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
			return err
		}
		if _, err := runner.DrainEgress(ctx); err != nil {
			return err
		}
		if err := runner.cleanupTelegramAttachments(ctx); err != nil {
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

func (runner *Runner) cleanupTelegramAttachments(ctx context.Context) error {
	if !runner.config.TelegramEnabled {
		return nil
	}
	cleanupState, ok := runner.ingressState.(TelegramAttachmentCleanupState)
	if !ok {
		return nil
	}
	now := runner.now()
	return cleanupState.CleanupTelegramAttachmentsForRuntime(
		ctx,
		runner.config.TelegramAttachmentDir,
		now,
		now.Add(-time.Duration(runner.config.TelegramAttachmentMaxAgeSeconds)*time.Second),
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
			if err := runner.ingressState.RecordTask(ctx, taskMessage); err != nil {
				return published, err
			}
			if attachmentState, ok := runner.ingressState.(TelegramAttachmentIngressState); ok && envelope.ReplyChannel.Type == "telegram" && len(envelope.Attachments) > 0 {
				if _, err := attachmentState.RecordTelegramTaskAttachments(ctx, taskMessage, runner.config.TelegramAttachmentDir); err != nil {
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

func (runner *Runner) DrainEgress(ctx context.Context) (int, error) {
	drained := 0
	waitSeconds := egressPickupWaitSeconds
	for {
		picked, err := runner.broker.PickupJSON(
			ctx,
			EgressQueueName,
			runner.config.PollTimeoutSeconds,
			waitSeconds,
		)
		if err != nil {
			return drained, err
		}
		if picked == nil {
			if runner.metrics != nil {
				runner.metrics.RecordEgressLoop(runner.now())
			}
			return drained, nil
		}
		raw, err := json.Marshal(picked.Payload)
		if err != nil {
			return drained, fmt.Errorf("marshal egress payload guid=%s: %w", picked.GUID, err)
		}
		if err := runner.egress.HandleRaw(ctx, raw); err != nil {
			return drained, err
		}
		if err := runner.broker.Ack(ctx, EgressQueueName, picked.GUID); err != nil {
			return drained, err
		}
		drained++
		waitSeconds = egressDrainWaitSeconds
	}
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
