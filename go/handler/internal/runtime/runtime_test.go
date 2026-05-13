package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
)

func TestRunEnsuresQueuesAndExitsAfterMaxLoops(t *testing.T) {
	broker := &fakeBroker{}
	handler := &fakeEgressHandler{}
	config := handlerconfig.Defaults()
	config.MaxLoops = 1
	config.PollIntervalSeconds = 100
	runner, err := NewRunner(config, broker, handler)
	if err != nil {
		t.Fatal(err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got, want := broker.ensured, []string{TaskQueueName, EgressQueueName}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ensured = %#v, want %#v", got, want)
	}
	if len(broker.pickups) != 1 {
		t.Fatalf("pickup calls = %#v", broker.pickups)
	}
	if len(handler.rawPayloads) != 0 {
		t.Fatalf("handled payloads = %#v", handler.rawPayloads)
	}
}

func TestDrainEgressHandlesAndAcksUntilEmpty(t *testing.T) {
	broker := &fakeBroker{
		picked: []*bbmb.PickedMessage{
			{GUID: "guid-1", Payload: map[string]any{"message_type": "chatting.egress.v2", "task_id": "task:1"}},
			{GUID: "guid-2", Payload: map[string]any{"message_type": "chatting.egress.v2", "task_id": "task:2"}},
		},
	}
	handler := &fakeEgressHandler{}
	config := handlerconfig.Defaults()
	config.PollTimeoutSeconds = 7
	runner, err := NewRunner(config, broker, handler)
	if err != nil {
		t.Fatal(err)
	}

	drained, err := runner.DrainEgress(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if drained != 2 {
		t.Fatalf("drained = %d", drained)
	}
	if got, want := broker.pickups, []pickupCall{
		{queue: EgressQueueName, timeoutSeconds: 7, waitSeconds: egressPickupWaitSeconds},
		{queue: EgressQueueName, timeoutSeconds: 7, waitSeconds: egressDrainWaitSeconds},
		{queue: EgressQueueName, timeoutSeconds: 7, waitSeconds: egressDrainWaitSeconds},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pickups = %#v, want %#v", got, want)
	}
	if got, want := broker.acks, []ackCall{
		{queue: EgressQueueName, guid: "guid-1"},
		{queue: EgressQueueName, guid: "guid-2"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acks = %#v, want %#v", got, want)
	}
	if len(handler.rawPayloads) != 2 {
		t.Fatalf("handled payloads = %#v", handler.rawPayloads)
	}
	for index, raw := range handler.rawPayloads {
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("payload %d is invalid JSON: %v", index, err)
		}
	}
}

func TestDrainEgressDoesNotAckWhenHandlerFails(t *testing.T) {
	expected := errors.New("egress failed")
	broker := &fakeBroker{
		picked: []*bbmb.PickedMessage{{GUID: "guid-1", Payload: map[string]any{"message_type": "chatting.egress.v2"}}},
	}
	handler := &fakeEgressHandler{err: expected}
	runner, err := NewRunner(handlerconfig.Defaults(), broker, handler)
	if err != nil {
		t.Fatal(err)
	}

	drained, err := runner.DrainEgress(context.Background())
	if !errors.Is(err, expected) {
		t.Fatalf("err = %v, want %v", err, expected)
	}
	if drained != 0 {
		t.Fatalf("drained = %d", drained)
	}
	if len(broker.acks) != 0 {
		t.Fatalf("acks = %#v", broker.acks)
	}
}

func TestRunReturnsWhenContextIsCancelledDuringSleep(t *testing.T) {
	broker := &fakeBroker{}
	handler := &fakeEgressHandler{}
	config := handlerconfig.Defaults()
	config.MaxLoops = 0
	runner, err := NewRunner(config, broker, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runner.sleep = func(context.Context, time.Duration) error {
		cancel()
		return context.Canceled
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

type pickupCall struct {
	queue          string
	timeoutSeconds int
	waitSeconds    int
}

type ackCall struct {
	queue string
	guid  string
}

type fakeBroker struct {
	ensured []string
	picked  []*bbmb.PickedMessage
	pickups []pickupCall
	acks    []ackCall
}

func (broker *fakeBroker) EnsureQueue(ctx context.Context, queueName string) error {
	broker.ensured = append(broker.ensured, queueName)
	return nil
}

func (broker *fakeBroker) PickupJSON(ctx context.Context, queueName string, timeoutSeconds int, waitSeconds int) (*bbmb.PickedMessage, error) {
	broker.pickups = append(broker.pickups, pickupCall{
		queue:          queueName,
		timeoutSeconds: timeoutSeconds,
		waitSeconds:    waitSeconds,
	})
	if len(broker.picked) == 0 {
		return nil, nil
	}
	picked := broker.picked[0]
	broker.picked = broker.picked[1:]
	return picked, nil
}

func (broker *fakeBroker) Ack(ctx context.Context, queueName string, guid string) error {
	broker.acks = append(broker.acks, ackCall{queue: queueName, guid: guid})
	return nil
}

type fakeEgressHandler struct {
	rawPayloads [][]byte
	err         error
}

func (handler *fakeEgressHandler) HandleRaw(ctx context.Context, raw []byte) error {
	handler.rawPayloads = append(handler.rawPayloads, raw)
	return handler.err
}
