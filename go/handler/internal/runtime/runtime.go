package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
)

const (
	TaskQueueName           = "chatting.tasks.v1"
	EgressQueueName         = "chatting.egress.v1"
	egressPickupWaitSeconds = 5
	egressDrainWaitSeconds  = 0
)

type Broker interface {
	EnsureQueue(ctx context.Context, queueName string) error
	PickupJSON(ctx context.Context, queueName string, timeoutSeconds int, waitSeconds int) (*bbmb.PickedMessage, error)
	Ack(ctx context.Context, queueName string, guid string) error
}

type EgressHandler interface {
	HandleRaw(ctx context.Context, raw []byte) error
}

type Runner struct {
	config handlerconfig.Config
	broker Broker
	egress EgressHandler
	sleep  func(context.Context, time.Duration) error
}

func NewRunner(config handlerconfig.Config, broker Broker, egress EgressHandler) (*Runner, error) {
	if broker == nil {
		return nil, errors.New("broker is required")
	}
	if egress == nil {
		return nil, errors.New("egress handler is required")
	}
	return &Runner{
		config: config,
		broker: broker,
		egress: egress,
		sleep:  sleep,
	}, nil
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
		if _, err := runner.DrainEgress(ctx); err != nil {
			return err
		}
		if runner.config.MaxLoops > 0 && loopCount >= runner.config.MaxLoops {
			return nil
		}
		if err := runner.sleep(ctx, durationFromSeconds(runner.config.PollIntervalSeconds)); err != nil {
			return nil
		}
	}
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
