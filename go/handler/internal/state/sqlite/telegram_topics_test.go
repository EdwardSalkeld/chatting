package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/workitems"
)

type topicCreator struct {
	calls    atomic.Int32
	rejected bool
	err      error
	entered  chan struct{}
	release  chan struct{}
}

func (creator *topicCreator) CreateForumTopic(context.Context, string, string) (int64, bool, error) {
	creator.calls.Add(1)
	if creator.entered != nil {
		close(creator.entered)
		<-creator.release
	}
	return 42, creator.rejected, creator.err
}

func topicOwner(t *testing.T, store *Store, target string) contracts.TaskQueueMessage {
	t.Helper()
	task := testTaskMessage(t)
	task.TaskID = "task:" + target
	task.Envelope.ID = "email:" + target
	task.Envelope.Source = "email"
	task.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "email", Target: target}
	assigned, err := store.AssignTask(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	return assigned
}

func TestTelegramTopicBindsIncomingRepliesToExistingWorkspace(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	owner := topicOwner(t, store, "alice")
	creator := &topicCreator{}
	service := workitems.TelegramTopics{Store: store, Creator: creator, DefaultChatID: "-100", AllowedChatIDs: []string{"-100"}}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/work-items/telegram-topic", strings.NewReader(`{"task_id":"`+owner.TaskID+`","name":"Email work"}`)))
	if response.Code != 200 {
		t.Fatalf("%d %s", response.Code, response.Body.String())
	}
	var payload struct {
		Conversation contracts.ReplyChannel `json:"conversation"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	first := payload.Conversation
	repeat, err := service.Ensure(ctx, owner.TaskID, "", "A different title", 0)
	if err != nil {
		t.Fatal(err)
	}
	if creator.calls.Load() != 1 || first.Target != "-100" || repeat.Metadata["message_thread_id"] != int64(42) {
		t.Fatalf("did not reuse topic: %+v %+v", first, repeat)
	}
	incoming := testTaskMessage(t)
	incoming.TaskID = "task:topic-reply"
	incoming.Envelope.ID = "telegram:reply"
	incoming.Envelope.Source = "im"
	incoming.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": int64(42)}}
	reply, err := store.AssignTask(ctx, incoming)
	if err != nil {
		t.Fatal(err)
	}
	if reply.WorkItemID != owner.WorkItemID {
		t.Fatalf("reply changed workspace: %s -> %s", owner.WorkItemID, reply.WorkItemID)
	}
	if len(reply.Envelope.ConversationChannels) != 1 || reply.Envelope.ConversationChannels[0].Metadata["message_thread_id"] != int64(42) {
		t.Fatalf("missing associated topic: %+v", reply.Envelope.ConversationChannels)
	}
	incoming.TaskID = "task:other-topic"
	incoming.Envelope.ID = "telegram:other-topic"
	incoming.Envelope.ReplyChannel.Metadata["message_thread_id"] = int64(43)
	other, err := store.AssignTask(ctx, incoming)
	if err != nil {
		t.Fatal(err)
	}
	if other.WorkItemID == owner.WorkItemID {
		t.Fatal("unrelated topic joined owner")
	}
	if err := store.BindTelegramTopic(ctx, owner.TaskID, "-100", "conflict", 43); err == nil {
		t.Fatal("stole an existing topic")
	}
	again, err := store.AssignTask(ctx, incoming)
	if err != nil || again.WorkItemID != other.WorkItemID {
		t.Fatal("conflict changed existing ownership")
	}
}

func TestTelegramTopicUncertainCreationRequiresRecovery(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	owner := topicOwner(t, store, "alice")
	creator := &topicCreator{err: fmt.Errorf("response lost")}
	service := workitems.TelegramTopics{Store: store, Creator: creator, DefaultChatID: "-100", AllowedChatIDs: []string{"-100"}}
	if _, err := service.Ensure(ctx, owner.TaskID, "", "work", 0); err == nil {
		t.Fatal("accepted lost response")
	}
	if _, err := service.Ensure(ctx, owner.TaskID, "", "work", 0); err == nil {
		t.Fatal("retried uncertain creation")
	}
	if creator.calls.Load() != 1 {
		t.Fatal("created duplicate after uncertain response")
	}
	recovered, err := service.Ensure(ctx, owner.TaskID, "-100", "Recovered", 42)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Metadata["message_thread_id"] != int64(42) {
		t.Fatal("failed recovery")
	}
}

func TestTelegramTopicConcurrentEnsureCreatesOnlyOnce(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	owner := topicOwner(t, store, "alice")
	creator := &topicCreator{entered: make(chan struct{}), release: make(chan struct{})}
	service := workitems.TelegramTopics{Store: store, Creator: creator, DefaultChatID: "-100", AllowedChatIDs: []string{"-100"}}
	done := make(chan error, 1)
	go func() { _, err := service.Ensure(ctx, owner.TaskID, "", "work", 0); done <- err }()
	<-creator.entered
	if _, err := service.Ensure(ctx, owner.TaskID, "", "work", 0); err == nil {
		t.Fatal("pending creation appeared ready")
	}
	close(creator.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if creator.calls.Load() != 1 {
		t.Fatal("concurrent duplicate")
	}
}

func TestTelegramTopicReusesExistingConversationAndValidatesCreation(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	task := testTaskMessage(t)
	task.Envelope.Source = "im"
	task.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100"}
	owner, err := store.AssignTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	creator := &topicCreator{rejected: true, err: fmt.Errorf("missing bot permission")}
	service := workitems.TelegramTopics{Store: store, Creator: creator, DefaultChatID: "-100", AllowedChatIDs: []string{"-100"}}
	conversation, err := service.Ensure(ctx, owner.TaskID, "", "", 0)
	if err != nil || conversation.Target != "-100" || creator.calls.Load() != 0 {
		t.Fatal("created topic for workspace already attached to Telegram")
	}
	email := topicOwner(t, store, "alice")
	if _, err := service.Ensure(ctx, email.TaskID, "-200", "work", 0); err == nil {
		t.Fatal("created in unallowed group")
	}
	for i := 0; i < 2; i++ {
		if _, err := service.Ensure(ctx, email.TaskID, "", "work", 0); err == nil {
			t.Fatal("accepted rejection")
		}
	}
	if creator.calls.Load() != 2 {
		t.Fatal("definite rejection blocked corrected retry")
	}
}
