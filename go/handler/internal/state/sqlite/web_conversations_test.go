package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestWebIngressReusesLaneAndSurvivesRetry(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	origin := testTaskMessage(t)
	origin.Envelope.Source = "im"
	origin.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": 7, "chat_title": "Test chat"}}
	assigned, err := store.AssignTask(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	conversations, err := store.ListWebConversations(ctx)
	if err != nil || len(conversations) != 1 {
		t.Fatalf("conversations=%v err=%v", conversations, err)
	}
	c := conversations[0]
	if c.TopicID == nil || *c.TopicID != 7 {
		t.Fatalf("lost topic: %#v", c)
	}
	first, err := store.QueueWebMessage(ctx, c.ID, "abcdefghijklmnop", "hello from web", []string{"repo:/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.QueueWebMessage(ctx, c.ID, "abcdefghijklmnop", "hello from web", nil)
	if err != nil || again.TaskID != first.TaskID {
		t.Fatalf("idempotency: %#v %v", again, err)
	}
	if _, err = store.QueueWebMessage(ctx, c.ID, "abcdefghijklmnop", "changed", nil); !errors.Is(err, ErrWebConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if _, err = store.QueueWebMessage(ctx, "unknown", "abcdefghijklmnop", "hello", nil); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown route=%v", err)
	}
	pending, err := store.PendingWebEnvelopes(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	task := contracts.NewTaskQueueMessage(pending[0], "trace:web", pending[0].ReceivedAt.Time)
	task, err = store.AssignTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if task.WorkItemID != assigned.WorkItemID || task.Envelope.ReplyChannel.Type != "web" || len(task.Envelope.ConversationChannels) != 1 {
		t.Fatalf("lost existing context: %#v", task)
	}
	if err = store.RecordTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkWebPublished(ctx, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	pending, err = store.PendingWebEnvelopes(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("not acknowledged: %v %v", pending, err)
	}
	submissions, err := store.WebSubmissions(ctx, c.ID)
	if err != nil || len(submissions) != 1 {
		t.Fatalf("lost accepted message: %v %v", submissions, err)
	}
	if err = store.MarkTaskCompleted(ctx, task.TaskID, task.Envelope.ID, task.TraceID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.ExecContext(ctx, "DELETE FROM completed_task_ledger"); err != nil {
		t.Fatal(err)
	}
	submissions, err = store.WebSubmissions(ctx, c.ID)
	if err != nil || len(submissions) != 0 {
		t.Fatalf("settled receipts must not reappear as queued after ledger pruning: %v %v", submissions, err)
	}

}

func TestWebIngressRemainsPendingAfterStoreRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "handler.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	task := testTaskMessage(t)
	task.Envelope.Source = "im"
	task.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-200"}
	if _, err = store.AssignTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	conversations, err := store.ListWebConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.QueueWebMessage(ctx, conversations[0].ID, "abcdefghijklmnop", "durable request", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	pending, err := reopened.PendingWebEnvelopes(ctx)
	if err != nil || len(pending) != 1 || "task:"+pending[0].ID != accepted.TaskID {
		t.Fatalf("restart lost submission: %#v %v", pending, err)
	}
}

func TestWebTaskKeepsSelectedTopicWhenWorkspaceHasSeveralRoutes(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	origin := testTaskMessage(t)
	origin.Envelope.Source = "im"
	origin.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": 7}}
	assigned, err := store.AssignTask(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	key := `{"chat":"-100","topic":8}`
	if _, err = store.db.ExecContext(ctx, "INSERT INTO work_item_routes VALUES('telegram',?,?)", key, assigned.WorkItemID); err != nil {
		t.Fatal(err)
	}
	conversations, err := store.ListWebConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, conversation := range conversations {
		if conversation.TopicID == nil || *conversation.TopicID != 8 {
			continue
		}
		_, err = store.QueueWebMessage(ctx, conversation.ID, "abcdefghijklmnop", "the selected topic", nil)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := store.PendingWebEnvelopes(ctx)
		if err != nil {
			t.Fatal(err)
		}
		task := contracts.NewTaskQueueMessage(pending[0], "trace:web", pending[0].ReceivedAt.Time)
		task, err = store.AssignTask(ctx, task)
		if err != nil {
			t.Fatal(err)
		}
		if len(task.Envelope.ConversationChannels) != 1 || task.Envelope.ConversationChannels[0].Metadata["message_thread_id"] != float64(8) {
			t.Fatalf("selected topic replaced by origin: %#v", task.Envelope.ConversationChannels)
		}
		return
	}
	t.Fatal("missing second topic")
}
