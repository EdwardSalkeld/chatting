package reminder

import (
	"context"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type fakeStore struct {
	reminders []Reminder
	fired     []string
}

func (store *fakeStore) DueReminders(context.Context, time.Time) ([]Reminder, error) {
	return append([]Reminder{}, store.reminders...), nil
}

func (store *fakeStore) MarkFired(_ context.Context, reminderID string, revision int, _ time.Time) (bool, error) {
	store.fired = append(store.fired, reminderID)
	store.reminders = nil
	return true, nil
}

func TestPollEmitsStableEnvelopeAndAckMarksFired(t *testing.T) {
	now := time.Date(2026, 8, 15, 11, 30, 0, 0, time.UTC)
	store := &fakeStore{reminders: []Reminder{{
		ReminderID: "rem_abc", Revision: 2, RunAt: now.Add(-time.Hour), Prompt: "Do it",
		ContextRefs: []string{"repo:/workspace"}, PromptContext: []string{"Be concise."},
		ReplyChannel: contracts.ReplyChannel{Type: "telegram", Target: "-123", Metadata: map[string]any{"message_id": 4}},
	}}}
	connector, err := New(store, []string{"global"}, []string{"reminder source"}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	first, err := connector.Poll(context.Background())
	if err != nil || len(first) != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	envelope := first[0]
	if envelope.ID != "reminder:rem_abc:2" || envelope.DedupeKey != envelope.ID || envelope.Source != Source {
		t.Fatalf("envelope=%+v", envelope)
	}
	if envelope.ReceivedAt.Time != now || envelope.ReplyChannel.Target != "-123" {
		t.Fatalf("envelope=%+v", envelope)
	}
	if got := envelope.PromptContext.AssembledInstructions(); len(got) != 3 {
		t.Fatalf("prompt context=%v", got)
	}

	second, err := connector.Poll(context.Background())
	if err != nil || len(second) != 1 || len(store.fired) != 0 {
		t.Fatalf("before ack second=%+v fired=%v err=%v", second, store.fired, err)
	}
	if err := connector.AckEnvelope(context.Background(), envelope.ID); err != nil {
		t.Fatal(err)
	}
	if len(store.fired) != 1 || store.fired[0] != "rem_abc" {
		t.Fatalf("fired=%v", store.fired)
	}
	after, err := connector.Poll(context.Background())
	if err != nil || len(after) != 0 {
		t.Fatalf("after=%+v err=%v", after, err)
	}
}

func TestAckRejectsUnknownEnvelope(t *testing.T) {
	connector, err := New(&fakeStore{}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := connector.AckEnvelope(context.Background(), "missing"); err == nil {
		t.Fatal("expected error")
	}
}
