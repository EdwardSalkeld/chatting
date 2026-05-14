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
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
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

func TestRunPublishesInternalHeartbeatTask(t *testing.T) {
	broker := &fakeBroker{}
	state := newFakeIngressState()
	handler := &fakeEgressHandler{}
	config := handlerconfig.Defaults()
	config.MaxLoops = 1
	runner, err := NewRunner(
		config,
		broker,
		handler,
		WithIngress(state, heartbeat.New(func() time.Time {
			return mustTime(t, "2026-03-09T12:00:00Z")
		})),
		WithNow(func() time.Time {
			return mustTime(t, "2026-03-09T12:00:01Z")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(broker.published) != 1 {
		t.Fatalf("published = %#v", broker.published)
	}
	published := broker.published[0]
	if published.queue != TaskQueueName {
		t.Fatalf("publish queue = %q", published.queue)
	}
	raw, err := json.Marshal(published.payload)
	if err != nil {
		t.Fatal(err)
	}
	taskMessage, err := contracts.DecodeTaskQueueMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if taskMessage.Envelope.Source != "internal" {
		t.Fatalf("source = %q", taskMessage.Envelope.Source)
	}
	if taskMessage.Envelope.ReplyChannel.Type != "internal" || taskMessage.Envelope.ReplyChannel.Target != "heartbeat" {
		t.Fatalf("reply channel = %#v", taskMessage.Envelope.ReplyChannel)
	}
	if _, ok := state.tasks[taskMessage.TaskID]; !ok {
		t.Fatalf("task was not recorded: %#v", state.tasks)
	}
	if !state.seen[taskMessage.Envelope.Source][taskMessage.Envelope.DedupeKey] {
		t.Fatalf("dedupe key was not marked seen: %#v", state.seen)
	}
}

func TestPublishIngressSkipsSeenEnvelope(t *testing.T) {
	broker := &fakeBroker{}
	state := newFakeIngressState()
	connector := heartbeat.New(func() time.Time {
		return mustTime(t, "2026-03-09T12:00:00Z")
	})
	envelope := heartbeat.BuildEnvelope(1, mustTime(t, "2026-03-09T12:00:00Z"))
	state.seen[envelope.Source] = map[string]bool{envelope.DedupeKey: true}
	runner, err := NewRunner(
		handlerconfig.Defaults(),
		broker,
		&fakeEgressHandler{},
		WithIngress(state, connector),
	)
	if err != nil {
		t.Fatal(err)
	}

	published, err := runner.PublishIngress(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if published != 0 {
		t.Fatalf("published = %d", published)
	}
	if len(broker.published) != 0 {
		t.Fatalf("broker published = %#v", broker.published)
	}
}

func TestPublishIngressAcksAckingConnectorAfterPublish(t *testing.T) {
	broker := &fakeBroker{}
	state := newFakeIngressState()
	envelope := heartbeat.BuildEnvelope(1, mustTime(t, "2026-03-09T12:00:00Z"))
	connector := &fakeAckingConnector{envelopes: []contracts.TaskEnvelope{envelope}}
	runner, err := NewRunner(
		handlerconfig.Defaults(),
		broker,
		&fakeEgressHandler{},
		WithIngress(state, connector),
	)
	if err != nil {
		t.Fatal(err)
	}

	published, err := runner.PublishIngress(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if published != 1 {
		t.Fatalf("published = %d", published)
	}
	if got, want := connector.acked, []string{envelope.ID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acked = %#v, want %#v", got, want)
	}
}

func TestPublishIngressAcksSeenEnvelopeFromAckingConnector(t *testing.T) {
	broker := &fakeBroker{}
	state := newFakeIngressState()
	envelope := heartbeat.BuildEnvelope(1, mustTime(t, "2026-03-09T12:00:00Z"))
	state.seen[envelope.Source] = map[string]bool{envelope.DedupeKey: true}
	connector := &fakeAckingConnector{envelopes: []contracts.TaskEnvelope{envelope}}
	runner, err := NewRunner(
		handlerconfig.Defaults(),
		broker,
		&fakeEgressHandler{},
		WithIngress(state, connector),
	)
	if err != nil {
		t.Fatal(err)
	}

	published, err := runner.PublishIngress(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if published != 0 {
		t.Fatalf("published = %d", published)
	}
	if len(broker.published) != 0 {
		t.Fatalf("broker published = %#v", broker.published)
	}
	if got, want := connector.acked, []string{envelope.ID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acked = %#v, want %#v", got, want)
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

func TestRunRecordsLoopAndEgressMetrics(t *testing.T) {
	broker := &fakeBroker{
		picked: []*bbmb.PickedMessage{
			{GUID: "guid-1", Payload: map[string]any{"message_type": "chatting.egress.v2", "task_id": "task:1"}},
		},
	}
	handler := &fakeEgressHandler{}
	recorder := metrics.New(mustTime(t, "2026-03-09T12:00:00Z"), &fakeMetricsClock{now: mustTime(t, "2026-03-09T12:00:03Z")})
	config := handlerconfig.Defaults()
	config.MaxLoops = 1
	config.PollIntervalSeconds = 100
	runner, err := NewRunner(
		config,
		broker,
		handler,
		WithMetrics(recorder),
		WithNow(func() time.Time {
			return mustTime(t, "2026-03-09T12:00:02Z")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	snapshot := recorder.Snapshot()
	if snapshot["loops_total"] != 1 {
		t.Fatalf("loops_total = %v", snapshot["loops_total"])
	}
	if snapshot["egress_loops_total"] != 1 {
		t.Fatalf("egress_loops_total = %v", snapshot["egress_loops_total"])
	}
	if snapshot["last_loop_completed_timestamp_seconds"] != float64(mustTime(t, "2026-03-09T12:00:02Z").Unix()) {
		t.Fatalf("last_loop_completed_timestamp_seconds = %v", snapshot["last_loop_completed_timestamp_seconds"])
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

type publishCall struct {
	queue   string
	payload map[string]any
}

type fakeBroker struct {
	ensured   []string
	published []publishCall
	picked    []*bbmb.PickedMessage
	pickups   []pickupCall
	acks      []ackCall
}

func (broker *fakeBroker) EnsureQueue(ctx context.Context, queueName string) error {
	broker.ensured = append(broker.ensured, queueName)
	return nil
}

func (broker *fakeBroker) PublishJSON(ctx context.Context, queueName string, payload map[string]any) (string, error) {
	broker.published = append(broker.published, publishCall{queue: queueName, payload: payload})
	return "guid-published", nil
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

type fakeIngressState struct {
	seen  map[string]map[string]bool
	tasks map[string]contracts.TaskQueueMessage
}

func newFakeIngressState() *fakeIngressState {
	return &fakeIngressState{
		seen:  map[string]map[string]bool{},
		tasks: map[string]contracts.TaskQueueMessage{},
	}
}

func (state *fakeIngressState) Seen(ctx context.Context, source string, dedupeKey string) (bool, error) {
	return state.seen[source][dedupeKey], nil
}

func (state *fakeIngressState) MarkSeen(ctx context.Context, source string, dedupeKey string) error {
	if state.seen[source] == nil {
		state.seen[source] = map[string]bool{}
	}
	state.seen[source][dedupeKey] = true
	return nil
}

func (state *fakeIngressState) RecordTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) error {
	state.tasks[taskMessage.TaskID] = taskMessage
	return nil
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

type fakeAckingConnector struct {
	envelopes []contracts.TaskEnvelope
	acked     []string
}

func (connector *fakeAckingConnector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	return connector.envelopes, nil
}

func (connector *fakeAckingConnector) AckEnvelope(ctx context.Context, envelopeID string) error {
	connector.acked = append(connector.acked, envelopeID)
	return nil
}

type fakeMetricsClock struct {
	now time.Time
}

func (clock *fakeMetricsClock) Now() time.Time {
	return clock.now
}

func (clock *fakeMetricsClock) Since(start time.Time) time.Duration {
	return clock.now.Sub(start)
}
