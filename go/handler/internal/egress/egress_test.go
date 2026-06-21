package egress

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestHandleRawDropsInvalidPayload(t *testing.T) {
	engine := newTestEngine(t, newFakeState(), nil)

	result, err := engine.HandleRaw(context.Background(), []byte(`{"message_type":"chatting.egress.v1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDropped || result.Reason != "invalid_payload" {
		t.Fatalf("result = %#v", result)
	}
}

func TestHandleDropsUnknownTask(t *testing.T) {
	engine := newTestEngine(t, newFakeState(), nil)

	result, err := engine.Handle(context.Background(), testEgressMessage(t, testTaskMessage(t), nil, "evt:1", "incremental"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDropped || result.Reason != "unknown_task" {
		t.Fatalf("result = %#v", result)
	}
}

func TestHandleDropsCompletedTask(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.completed[task.TaskID] = task.Envelope.ID
	engine := newTestEngine(t, state, nil)

	result, err := engine.Handle(context.Background(), testEgressMessage(t, task, nil, "evt:1", "incremental"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDropped || result.Reason != "completed_task" {
		t.Fatalf("result = %#v", result)
	}
}

func TestHandleDropsDisallowedChannel(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	engine := newTestEngine(t, state, nil)
	message := testEgressMessage(t, task, nil, "evt:1", "incremental")
	message.Message.Channel = "telegram"

	result, err := engine.Handle(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDropped || result.Reason != "disallowed_channel" {
		t.Fatalf("result = %#v", result)
	}
}

func TestHandleAllowsFinalChannelWhenEnvelopeReplyChannelAllowed(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	dispatcher := &recordingDispatcher{}
	engine := newTestEngine(t, state, dispatcher)
	message := testEgressMessage(t, task, nil, "evt:final:1", "incremental")
	message.Message.Channel = "final"
	message.Message.Target = "unused"

	result, err := engine.Handle(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDispatched {
		t.Fatalf("result = %#v", result)
	}
	if len(dispatcher.messages) != 1 {
		t.Fatalf("dispatch count = %d", len(dispatcher.messages))
	}
}

func TestHandleAllowsInternalHeartbeatLogPongWhenLogDisallowed(t *testing.T) {
	task := contracts.NewTaskQueueMessage(
		heartbeat.BuildEnvelope(1, mustTime(t, "2026-03-09T12:00:00Z")),
		"trace:internal-heartbeat:1",
		mustTime(t, "2026-03-09T12:00:01Z"),
	)
	state := newFakeState()
	state.addTask(task)
	dispatcher := &recordingDispatcher{}
	engine := newTestEngine(t, state, dispatcher)
	message := testEgressMessage(t, task, intPtr(0), "evt:heartbeat:0", "message")
	message.Message.Channel = "log"
	message.Message.Target = "heartbeat"
	message.Message.Body = stringPtr(`{"kind":"heartbeat_pong"}`)

	result, err := engine.Handle(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDispatched {
		t.Fatalf("result = %#v", result)
	}
	if len(dispatcher.messages) != 1 {
		t.Fatalf("dispatch count = %d", len(dispatcher.messages))
	}
}

func TestHandleDispatchesUnsequencedIncrementalAndDedupesReplay(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	dispatcher := &recordingDispatcher{}
	engine := newTestEngine(t, state, dispatcher)
	message := testEgressMessage(t, task, nil, "evt:incremental:1", "incremental")

	result, err := engine.Handle(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDispatched {
		t.Fatalf("result = %#v", result)
	}
	if len(dispatcher.messages) != 1 {
		t.Fatalf("dispatch count = %d", len(dispatcher.messages))
	}

	result, err = engine.Handle(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDeduped {
		t.Fatalf("replay result = %#v", result)
	}
	if len(dispatcher.messages) != 1 {
		t.Fatalf("replay dispatch count = %d", len(dispatcher.messages))
	}
}

func TestHandlePersistsTelegramAssistantTurnAfterDispatch(t *testing.T) {
	task := telegramTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	dispatcher := &recordingDispatcher{
		result: &contracts.OutboundMessage{
			Channel: "telegram",
			Target:  "12345",
			Body:    stringPtr("reply-1"),
		},
	}
	engine := newTestEngineWithState(t, state, dispatcher, WithAllowedChannels([]string{"telegram"}))
	message := testEgressMessage(t, task, nil, "evt:incremental:1", "incremental")
	message.Message.Channel = "telegram"
	message.Message.Target = "12345"

	result, err := engine.Handle(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDispatched {
		t.Fatalf("result = %#v", result)
	}
	if got, want := state.turns["12345"], []conversationTurnRecord{{Role: "assistant", Content: "reply-1", RunID: task.TaskID}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("turns = %#v, want %#v", got, want)
	}
}

func TestHandlePersistsTelegramAttachmentOnlyDispatchAsAssistantTurn(t *testing.T) {
	task := telegramTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	dispatchName := "menu.pdf"
	dispatcher := &recordingDispatcher{
		result: &contracts.OutboundMessage{
			Channel: "telegram",
			Target:  "12345",
			Attachment: &contracts.AttachmentRef{
				URI:  "file:///tmp/menu.pdf",
				Name: &dispatchName,
			},
		},
	}
	engine := newTestEngineWithState(t, state, dispatcher, WithAllowedChannels([]string{"telegram"}))
	message := testEgressMessage(t, task, nil, "evt:incremental:1", "incremental")
	message.Message.Channel = "telegram"
	message.Message.Target = "12345"

	if _, err := engine.Handle(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if got, want := state.turns["12345"], []conversationTurnRecord{{Role: "assistant", Content: "[Attachment sent: menu.pdf]", RunID: task.TaskID}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("turns = %#v, want %#v", got, want)
	}
}

func TestHandleStagesAndFlushesSequencedEventsInOrder(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	dispatcher := &recordingDispatcher{}
	engine := newTestEngine(t, state, dispatcher)
	second := testEgressMessage(t, task, intPtr(1), "evt:1", "message")
	second.Message.Body = stringPtr("second")
	first := testEgressMessage(t, task, intPtr(0), "evt:0", "message")
	first.Message.Body = stringPtr("first")

	result, err := engine.Handle(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusStaged {
		t.Fatalf("out-of-order result = %#v", result)
	}
	if len(dispatcher.messages) != 0 {
		t.Fatalf("out-of-order dispatch count = %d", len(dispatcher.messages))
	}

	result, err = engine.Handle(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDispatched {
		t.Fatalf("flush result = %#v", result)
	}
	if got := dispatcher.bodies(); got != "first|second" {
		t.Fatalf("dispatch order = %q", got)
	}
}

func TestHandleAppliesCompletionAfterEarlierSequencedMessages(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	dispatcher := &recordingDispatcher{}
	engine := newTestEngine(t, state, dispatcher)
	first := testEgressMessage(t, task, intPtr(0), "evt:0", "message")
	completion := testEgressMessage(t, task, intPtr(1), "evt:completion", "completion")

	result, err := engine.Handle(context.Background(), completion)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusStaged {
		t.Fatalf("completion staged result = %#v", result)
	}
	result, err = engine.Handle(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusCompleted {
		t.Fatalf("completion flush result = %#v", result)
	}
	if len(dispatcher.messages) != 1 {
		t.Fatalf("dispatch count = %d", len(dispatcher.messages))
	}
	if state.completed[task.TaskID] != task.Envelope.ID {
		t.Fatalf("completed envelope = %q", state.completed[task.TaskID])
	}
}

func TestHandleReturnsDispatchErrorWithoutMarkingEvent(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	engine := newTestEngine(t, state, &recordingDispatcher{err: errors.New("send failed")})

	_, err := engine.Handle(context.Background(), testEgressMessage(t, task, nil, "evt:incremental:1", "incremental"))
	if err == nil {
		t.Fatal("expected dispatch error")
	}
	if state.dispatched[task.TaskID]["evt:incremental:1"] {
		t.Fatal("failed dispatch was marked dispatched")
	}
}

func TestSQLiteBackedEnginePersistsStagingAndDedupe(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestate.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTaskMessage(t)
	if err := store.RecordTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingDispatcher{}
	engine := newTestEngineWithState(t, NewSQLiteState(store), dispatcher)
	second := testEgressMessage(t, task, intPtr(1), "evt:1", "message")
	second.Message.Body = stringPtr("second")
	first := testEgressMessage(t, task, intPtr(0), "evt:0", "message")
	first.Message.Body = stringPtr("first")

	result, err := engine.Handle(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusStaged {
		t.Fatalf("out-of-order result = %#v", result)
	}
	result, err = engine.Handle(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusDispatched {
		t.Fatalf("flush result = %#v", result)
	}
	if got := dispatcher.bodies(); got != "first|second" {
		t.Fatalf("dispatch order = %q", got)
	}

	replayResult, err := engine.Handle(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if replayResult.Status != StatusDeduped {
		t.Fatalf("replay result = %#v", replayResult)
	}
	if got := dispatcher.bodies(); got != "first|second" {
		t.Fatalf("replay dispatch order = %q", got)
	}
}

type fakeState struct {
	tasks      map[string]TaskRecord
	completed  map[string]string
	dispatched map[string]map[string]bool
	staged     map[string]map[int]StagedRecord
	expected   map[string]int
	turns      map[string][]conversationTurnRecord
}

type conversationTurnRecord struct {
	Role    string
	Content string
	RunID   string
}

func newFakeState() *fakeState {
	return &fakeState{
		tasks:      map[string]TaskRecord{},
		completed:  map[string]string{},
		dispatched: map[string]map[string]bool{},
		staged:     map[string]map[int]StagedRecord{},
		expected:   map[string]int{},
		turns:      map[string][]conversationTurnRecord{},
	}
}

func (state *fakeState) addTask(task contracts.TaskQueueMessage) {
	state.tasks[task.TaskID] = TaskRecord{
		TaskID:      task.TaskID,
		EnvelopeID:  task.Envelope.ID,
		TraceID:     task.TraceID,
		TaskMessage: task,
	}
}

func (state *fakeState) GetTask(_ context.Context, taskID string) (*TaskRecord, error) {
	record, ok := state.tasks[taskID]
	if !ok {
		return nil, nil
	}
	return &record, nil
}

func (state *fakeState) IsTaskCompleted(_ context.Context, taskID string, envelopeID string) (bool, error) {
	return state.completed[taskID] == envelopeID, nil
}

func (state *fakeState) MarkTaskCompleted(_ context.Context, taskID string, envelopeID string, _ string) error {
	state.completed[taskID] = envelopeID
	delete(state.tasks, taskID)
	delete(state.staged, taskID)
	delete(state.expected, taskID)
	return nil
}

func (state *fakeState) HasDispatchedEventID(_ context.Context, taskID string, eventID string) (bool, error) {
	return state.dispatched[taskID][eventID], nil
}

func (state *fakeState) MarkDispatchedEventID(_ context.Context, taskID string, eventID string) error {
	if state.dispatched[taskID] == nil {
		state.dispatched[taskID] = map[string]bool{}
	}
	state.dispatched[taskID][eventID] = true
	return nil
}

func (state *fakeState) StageEgressEvent(_ context.Context, message contracts.EgressQueueMessage) error {
	if state.staged[message.TaskID] == nil {
		state.staged[message.TaskID] = map[int]StagedRecord{}
	}
	state.staged[message.TaskID][*message.Sequence] = StagedRecord{
		TaskID:        message.TaskID,
		EventID:       message.EventID,
		Sequence:      *message.Sequence,
		EgressMessage: message,
	}
	return nil
}

func (state *fakeState) ExpectedSequence(_ context.Context, taskID string) (int, error) {
	return state.expected[taskID], nil
}

func (state *fakeState) GetStagedEventBySequence(_ context.Context, taskID string, sequence int) (*StagedRecord, error) {
	record, ok := state.staged[taskID][sequence]
	if !ok {
		return nil, nil
	}
	return &record, nil
}

func (state *fakeState) MarkStagedEventDispatched(_ context.Context, taskID string, _ string, sequence int) error {
	delete(state.staged[taskID], sequence)
	if state.expected[taskID] <= sequence {
		state.expected[taskID] = sequence + 1
	}
	return nil
}

func (state *fakeState) AppendConversationTurn(_ context.Context, channel string, target string, role string, content string, runID string) error {
	if channel != "telegram" {
		return nil
	}
	state.turns[target] = append(state.turns[target], conversationTurnRecord{
		Role:    role,
		Content: content,
		RunID:   runID,
	})
	return nil
}

type recordingDispatcher struct {
	messages []contracts.OutboundMessage
	err      error
	result   *contracts.OutboundMessage
}

func (dispatcher *recordingDispatcher) Dispatch(_ context.Context, message contracts.OutboundMessage, _ contracts.TaskEnvelope) (*contracts.OutboundMessage, error) {
	if dispatcher.err != nil {
		return nil, dispatcher.err
	}
	dispatcher.messages = append(dispatcher.messages, message)
	if dispatcher.result != nil {
		return dispatcher.result, nil
	}
	return &message, nil
}

func (dispatcher *recordingDispatcher) bodies() string {
	result := ""
	for index, message := range dispatcher.messages {
		if index > 0 {
			result += "|"
		}
		if message.Body != nil {
			result += *message.Body
		}
	}
	return result
}

func newTestEngine(t *testing.T, state *fakeState, dispatcher *recordingDispatcher) *Engine {
	t.Helper()
	if dispatcher == nil {
		dispatcher = &recordingDispatcher{}
	}
	return newTestEngineWithState(t, state, dispatcher)
}

func newTestEngineWithState(t *testing.T, state State, dispatcher *recordingDispatcher, options ...Option) *Engine {
	t.Helper()
	if len(options) == 0 {
		options = []Option{WithAllowedChannels([]string{"email"})}
	}
	engine, err := New(state, dispatcher, options...)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func testTaskMessage(t *testing.T) contracts.TaskQueueMessage {
	t.Helper()
	actor := "alice@example.com"
	return contracts.NewTaskQueueMessage(contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "email:1",
		Source:        "email",
		ReceivedAt:    contracts.NewTimestamp(mustTime(t, "2026-03-06T11:00:00Z")),
		Actor:         &actor,
		Content:       "hello from egress tests",
		ReplyChannel: contracts.ReplyChannel{
			Type:   "email",
			Target: "alice@example.com",
		},
		DedupeKey: "email:1",
	}, "trace:email:1", mustTime(t, "2026-03-06T11:00:01Z"))
}

func telegramTaskMessage(t *testing.T) contracts.TaskQueueMessage {
	t.Helper()
	actor := "8605042448"
	return contracts.NewTaskQueueMessage(contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "telegram:1",
		Source:        "im",
		ReceivedAt:    contracts.NewTimestamp(mustTime(t, "2026-03-06T11:00:00Z")),
		Actor:         &actor,
		Content:       "hello from telegram",
		ReplyChannel: contracts.ReplyChannel{
			Type:   "telegram",
			Target: "12345",
		},
		DedupeKey: "telegram:1",
	}, "trace:telegram:1", mustTime(t, "2026-03-06T11:00:01Z"))
}

func testEgressMessage(t *testing.T, task contracts.TaskQueueMessage, sequence *int, eventID string, eventKind string) contracts.EgressQueueMessage {
	t.Helper()
	body := "hello"
	return contracts.EgressQueueMessage{
		SchemaVersion: contracts.SchemaVersion,
		MessageType:   contracts.EgressMessageTypeV2,
		TaskID:        task.TaskID,
		EnvelopeID:    task.Envelope.ID,
		TraceID:       task.TraceID,
		EventID:       eventID,
		EventKind:     eventKind,
		EmittedAt:     contracts.NewTimestamp(mustTime(t, "2026-03-06T12:00:00Z")),
		Message: contracts.OutboundMessage{
			Channel: "email",
			Target:  "alice@example.com",
			Body:    &body,
		},
		Sequence: sequence,
	}
}

func intPtr(value int) *int {
	return &value
}

func stringPtr(value string) *string {
	return &value
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
