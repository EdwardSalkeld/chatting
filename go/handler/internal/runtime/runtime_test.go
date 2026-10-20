package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
)

func TestRunEnsuresQueuesAndExitsAfterMaxLoops(t *testing.T) {
	broker := &fakeBroker{}
	config := handlerconfig.Defaults()
	config.MaxLoops = 1
	config.PollIntervalSeconds = 100
	runner, err := NewRunner(config, broker)
	if err != nil {
		t.Fatal(err)
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got, want := broker.ensured, []string{TaskQueueName}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ensured = %#v, want %#v", got, want)
	}
}

func TestRunPublishesInternalHeartbeatTask(t *testing.T) {
	broker := &fakeBroker{}
	state := newFakeIngressState()
	config := handlerconfig.Defaults()
	config.MaxLoops = 1
	runner, err := NewRunner(
		config,
		broker,
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
	config := handlerconfig.Defaults()
	config.MaxLoops = 0
	runner, err := NewRunner(config, broker)
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

func TestRunReturnsWhenContextIsCancelledDuringConnectorPoll(t *testing.T) {
	broker := &fakeBroker{}
	config := handlerconfig.Defaults()
	config.MaxLoops = 0
	ctx, cancel := context.WithCancel(context.Background())
	connector := &fakeCancelingConnector{onPoll: cancel}
	runner, err := NewRunner(
		config,
		broker,
		WithIngress(newFakeIngressState(), connector),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunRecordsLoopMetrics(t *testing.T) {
	broker := &fakeBroker{}
	recorder := metrics.New(mustTime(t, "2026-03-09T12:00:00Z"), &fakeMetricsClock{now: mustTime(t, "2026-03-09T12:00:03Z")})
	config := handlerconfig.Defaults()
	config.MaxLoops = 1
	config.PollIntervalSeconds = 100
	runner, err := NewRunner(
		config,
		broker,
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
	if snapshot["last_loop_completed_timestamp_seconds"] != float64(mustTime(t, "2026-03-09T12:00:02Z").Unix()) {
		t.Fatalf("last_loop_completed_timestamp_seconds = %v", snapshot["last_loop_completed_timestamp_seconds"])
	}
}

type publishCall struct {
	queue   string
	payload map[string]any
}

type fakeBroker struct {
	ensured   []string
	published []publishCall
}

func (broker *fakeBroker) EnsureQueue(ctx context.Context, queueName string) error {
	broker.ensured = append(broker.ensured, queueName)
	return nil
}

func (broker *fakeBroker) PublishJSON(ctx context.Context, queueName string, payload map[string]any) (string, error) {
	broker.published = append(broker.published, publishCall{queue: queueName, payload: payload})
	return "guid-published", nil
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

type fakeCancelingConnector struct {
	onPoll func()
}

func (connector *fakeCancelingConnector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if connector.onPoll != nil {
		connector.onPoll()
	}
	return nil, context.Canceled
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
