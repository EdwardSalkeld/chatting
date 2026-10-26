package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/webhooks"
)

func TestCIWebhookOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, registration, sha, conclusion, want, reason string
		ambiguous                                         bool
	}{
		{"fork PR without link", "", "abc", "failure", "keep", "workflow_failure", false},
		{"ambiguous commit", "workflow", "abc", "failure", "needs_context", "workflow_ambiguous_commit", true},
		{"unmatched commit", "", "unknown", "failure", "needs_context", "workflow_missing_pr_context", false},
		{"routine PR success", "", "abc", "success", "drop", "routine_workflow_result", false},
		{"PR owner before workflow default", "workflow", "abc", "failure", "keep", "workflow_failure", false},
		{"explicit run success", "run", "abc", "success", "keep", "registered_run_completed", false},
		{"explicit run cancellation", "run", "abc", "cancelled", "keep", "registered_run_completed", true},
		{"workflow failure", "workflow", "merge", "failure", "keep", "workflow_failure", false},
		{"workflow routine success", "workflow", "merge", "success", "drop", "routine_workflow_result", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "handler.db")
			store, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { store.Close() }()
			origin := testTaskMessage(t)
			origin.Envelope.Source = "im"
			origin.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100"}
			origin, err = store.AssignTask(ctx, origin)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.RegisterPR(ctx, origin.TaskID, "https://github.com/o/r/pull/9"); err != nil {
				t.Fatal(err)
			}
			if err := store.PutWebhookState(ctx, "watch", "billyacachofa/o/r#9", `{"head_sha":"abc"}`); err != nil {
				t.Fatal(err)
			}
			// The same commit in another repo/login must never be considered.
			for _, key := range []string{"billyacachofa/o/other#11", "other/o/r#12"} {
				if err := store.PutWebhookState(ctx, "watch", key, `{"head_sha":"abc"}`); err != nil {
					t.Fatal(err)
				}
			}
			if tc.ambiguous {
				if err := store.PutWebhookState(ctx, "watch", "billyacachofa/o/r#10", `{"head_sha":"abc"}`); err != nil {
					t.Fatal(err)
				}
			}
			// Register run/workflow destinations to a DIFFERENT owner to test
			// precedence over commit matching and preservation of its channel.
			other := testTaskMessage(t)
			other.TaskID += ":other"
			other.Envelope.ID += ":other"
			other.Envelope.Source = "im"
			other.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-200"}
			other, err = store.AssignTask(ctx, other)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.registration {
			case "run":
				err = store.RegisterRun(ctx, other.TaskID, "https://github.com/O/R/actions/runs/7")
				if err == nil && store.RegisterRun(ctx, origin.TaskID, "https://github.com/o/r/actions/runs/7") == nil {
					t.Fatal("conflicting run owner accepted")
				}
			case "workflow":
				err = store.RegisterWorkflow(ctx, other.TaskID, "https://github.com/o/r/actions/workflows/20")
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			filter := &webhooks.Filter{Store: store, Login: "BillyAcachofa", DeliveryEnabled: true}
			body := []byte(fmt.Sprintf(`{"action":"completed","repository":{"full_name":"o/r"},"sender":{"login":"BillyAcachofa"},"workflow_run":{"id":7,"workflow_id":20,"run_number":1,"run_attempt":1,"head_sha":%q,"conclusion":%q,"pull_requests":[]}}`, tc.sha, tc.conclusion))
			d, notification, err := webhooks.ReceiveGitHub(ctx, store, filter, body, "workflow_run", "one", time.Now())
			if err != nil || d.Disposition != tc.want || d.Reason != tc.reason {
				t.Fatalf("receipt: %+v %v", d, err)
			}
			pending, err := store.PendingGitHub(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if tc.want != "keep" {
				if len(pending) != 0 {
					t.Fatal("non-kept event queued work")
				}
				return
			}
			if len(pending) != 1 || notification == nil {
				t.Fatal("notification not persisted")
			}
			if (tc.registration == "run" || (tc.registration == "workflow" && tc.sha != "abc")) && notification.Envelope.ReplyChannel.Type != "log" {
				t.Fatal("Actions URL must not become an issue comment destination")
			}
			routed, err := store.AssignTask(ctx, *notification)
			wantOwner := origin
			if tc.registration == "run" || (tc.registration == "workflow" && tc.sha != "abc") {
				wantOwner = other
			}
			if err != nil || routed.WorkItemID != wantOwner.WorkItemID || routed.Envelope.ReplyChannel.Target != wantOwner.Envelope.ReplyChannel.Target {
				t.Fatalf("route: %+v %v", routed, err)
			}
			if !strings.Contains(notification.Envelope.Content, `"run_id":7`) {
				t.Fatal("lost run identity")
			}
			// Original signed-body identity survives resolution and changed
			// delivery headers; retries cannot create another executable task.
			d, _, err = webhooks.ReceiveGitHub(ctx, store, filter, body, "workflow_run", "different-header", time.Now())
			if err != nil || d.Reason != "duplicate" {
				t.Fatalf("redelivery: %+v %v", d, err)
			}
		})
	}
}

func TestWorkflowRecoveryAndRegistrationAfterHold(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	origin := testTaskMessage(t)
	origin, err := store.AssignTask(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	f := &webhooks.Filter{Store: store, Login: "BillyAcachofa", DeliveryEnabled: true}
	body := func(attempt int, conclusion string) []byte {
		b, _ := json.Marshal(map[string]any{"action": "completed", "repository": map[string]string{"full_name": "o/r"}, "workflow_run": map[string]any{"id": 7, "workflow_id": 20, "run_number": 1, "run_attempt": attempt, "head_sha": "merge", "conclusion": conclusion}})
		return b
	}
	d, _, err := webhooks.ReceiveGitHub(ctx, store, f, body(1, "failure"), "workflow_run", "one", time.Now())
	if err != nil || d.Disposition != "needs_context" {
		t.Fatalf("unregistered: %+v %v", d, err)
	}
	if err := store.RegisterWorkflow(ctx, origin.TaskID, "https://github.com/o/r/actions/workflows/20"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		attempt            int
		conclusion, reason string
	}{{1, "failure", "workflow_failure"}, {2, "success", "workflow_recovered"}, {3, "success", "routine_workflow_result"}, {1, "failure", "duplicate"}} {
		d, _, err := webhooks.ReceiveGitHub(ctx, store, f, body(tc.attempt, tc.conclusion), "workflow_run", fmt.Sprint(tc.attempt), time.Now())
		if err != nil || d.Reason != tc.reason {
			t.Fatalf("retry/recovery: %+v %v", d, err)
		}
	}
	var holds int
	if err := store.db.QueryRow(`SELECT count(*) FROM github_webhook_held`).Scan(&holds); err != nil || holds != 0 {
		t.Fatalf("hold not cleared: %d %v", holds, err)
	}
}
