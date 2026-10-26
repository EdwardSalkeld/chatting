package webhooks

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	handlerruntime "github.com/EdwardSalkeld/chatting/go/handler/internal/runtime"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestAtomicReceiptAndPendingRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "handler.db")
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"action":"created","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"issue":{"number":1},"comment":{"id":99,"body":"@BillyAcachofa please inspect this"}}`
	receive := func(delivery string, live bool) {
		t.Helper()
		f := &Filter{Store: store, Login: "BillyAcachofa", DeliveryEnabled: live, AssignTask: store.AssignTask}
		request := httptest.NewRequest(http.MethodPost, GitHubPath, strings.NewReader(body))
		mac := hmac.New(sha256.New, []byte("secret"))
		_, _ = mac.Write([]byte(body))
		request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		request.Header.Set("X-GitHub-Event", "issue_comment")
		request.Header.Set("X-GitHub-Delivery", delivery)
		response := httptest.NewRecorder()
		Handler(log.New(io.Discard, "", 0), []byte("secret"), f).ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatal(response.Code, response.Body.String())
		}
	}
	receive("one", true)
	pending, err := store.PendingGitHub(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if strings.Contains(pending[0].Content, "please inspect") || !strings.Contains(pending[0].Content, `"comment_id":99`) {
		t.Fatal("notification must carry IDs without interpreting/copying the body", pending[0].Content)
	}
	id := pending[0].ID
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	receive("modified-unsigned-header", true)
	pending, err = store.PendingGitHub(ctx)
	if err != nil || len(pending) != 1 || pending[0].ID != id {
		t.Fatal("retry lost/duplicated pending event", err)
	}
	if err := (Connector{Store: store}).AckEnvelope(ctx, id); err != nil {
		t.Fatal(err)
	}
	pending, err = store.PendingGitHub(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatal("ack did not clear pending event", err)
	}
	// Default/logging-only receipt never creates a pending executable task.
	body = strings.ReplaceAll(body, `"number":1`, `"number":2`)
	receive("two", false)
	pending, err = store.PendingGitHub(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatal("logging-only delivery queued work", err)
	}
}

type retryBroker struct {
	fail  bool
	tasks []map[string]any
}

func (b *retryBroker) EnsureQueue(context.Context, string) error { return nil }
func (b *retryBroker) PublishJSON(_ context.Context, _ string, task map[string]any) (string, error) {
	if b.fail {
		return "", errors.New("broker unavailable")
	}
	b.tasks = append(b.tasks, task)
	return "published", nil
}

func TestPendingDeliveryRetriesThroughOrdinaryIngress(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	filter := &Filter{Store: store, Login: "BillyAcachofa", DeliveryEnabled: true}
	body := []byte(`{"action":"created","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"issue":{"number":1},"comment":{"id":5,"body":"@BillyAcachofa help"}}`)
	d, notification, err := ReceiveGitHub(ctx, store, filter, body, "issue_comment", "delivery", time.Now())
	if err != nil || d.Disposition != "keep" {
		t.Fatalf("receipt: %+v %v", d, err)
	}
	broker := &retryBroker{fail: true}
	newRunner := func() *handlerruntime.Runner {
		runner, err := handlerruntime.NewRunner(handlerconfig.Defaults(), broker, store, []handlerruntime.Connector{Connector{Store: store}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return runner
	}
	if _, err := newRunner().PublishIngress(ctx); err == nil {
		t.Fatal("expected broker failure")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	broker.fail = false
	if n, err := newRunner().PublishIngress(ctx); err != nil || n != 1 {
		t.Fatal("pending delivery lost after restart", n, err)
	}
	if n, err := newRunner().PublishIngress(ctx); err != nil || n != 0 {
		t.Fatal("delivery repeated", n, err)
	}
	encoded, _ := json.Marshal(broker.tasks[0])
	task, err := contracts.DecodeTaskQueueMessage(encoded)
	if err != nil || task.WorkItemID == "" || task.TaskID != notification.TaskID || task.Envelope.ReplyChannel.Target != "https://github.com/o/r/issues/1" {
		t.Fatalf("bad routed task: %+v %v", task, err)
	}
}

func TestAssignmentDeliveryAndFollowupShareOwner(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "handler.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	filter := &Filter{Store: store, Login: "BillyAcachofa", DeliveryEnabled: true}
	body := []byte(`{"action":"assigned","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"assignee":{"login":"BillyAcachofa"},"issue":{"number":42}}`)
	d, _, err := ReceiveGitHub(ctx, store, filter, body, "issues", "assignment", time.Now())
	if err != nil || d.Disposition != "keep" || d.Reason != "assigned_to_us" {
		t.Fatalf("assignment: %+v %v", d, err)
	}
	pending, err := store.PendingGitHub(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending assignment: %v %v", pending, err)
	}
	task, err := store.AssignTask(ctx, contracts.NewTaskQueueMessage(pending[0], "assignment", time.Now()))
	if err != nil || task.WorkItemID == "" || task.Envelope.ReplyChannel.Target != "https://github.com/o/r/issues/42" {
		t.Fatalf("assignment route: %+v %v", task, err)
	}
	followup := []byte(`{"action":"created","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"issue":{"number":42},"comment":{"id":99,"body":"more information"}}`)
	d, notification, err := ReceiveGitHub(ctx, store, filter, followup, "issue_comment", "followup", time.Now())
	if err != nil || d.Disposition != "keep" || notification == nil {
		t.Fatalf("followup: %+v %v", d, err)
	}
	routed, err := store.AssignTask(ctx, *notification)
	if err != nil || routed.WorkItemID != task.WorkItemID {
		t.Fatalf("followup lost assignment owner: %+v %v", routed, err)
	}
}

func TestNotificationEventIdentifiers(t *testing.T) {
	for _, tc := range []struct{ event, extra, want string }{
		{"pull_request_review", `"review":{"id":4}`, `"review_id":4`},
		{"pull_request_review_comment", `"comment":{"id":5}`, `"comment_id":5`},
		{"pull_request_review_thread", `"thread":{"id":6}`, `"review_thread_id":6`},
		{"workflow_run", `"workflow_run":{"id":7,"run_attempt":2,"head_sha":"abc","pull_requests":[{"number":9}]}`, `"run_id":7`},
	} {
		body := []byte(`{"repository":{"full_name":"o/r"},"pull_request":{"number":9},` + tc.extra + `}`)
		task, err := GitHubNotification(body, tc.event, "delivery", time.Now())
		if err != nil || !strings.Contains(task.Envelope.Content, tc.want) {
			t.Fatalf("%s: %v %s", tc.event, err, task.Envelope.Content)
		}
		encoded, _ := json.Marshal(task)
		if !json.Valid(encoded) {
			t.Fatal("invalid task")
		}
	}
}
