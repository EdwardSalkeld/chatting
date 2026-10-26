package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/webhooks"
)

func ciOwner(t *testing.T, store *Store, target string) contracts.TaskQueueMessage {
	t.Helper()
	origin := testTaskMessage(t)
	origin.TaskID += target
	origin.Envelope.ID += target
	origin.Envelope.Source = "im"
	origin.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: target}
	origin, err := store.AssignTask(context.Background(), origin)
	if err != nil {
		t.Fatal(err)
	}
	return origin
}

func ciBody(id, number, attempt int, sha, conclusion, event, branch string) []byte {
	b, _ := json.Marshal(map[string]any{"action": "completed", "repository": map[string]string{"full_name": "O/R"}, "workflow_run": map[string]any{
		"id": id, "workflow_id": 20, "run_number": number, "run_attempt": attempt, "head_sha": sha, "conclusion": conclusion, "event": event, "head_branch": branch,
	}})
	return b
}

func ciReceive(t *testing.T, store *Store, body []byte, reason string) *contracts.TaskQueueMessage {
	t.Helper()
	f := &webhooks.Filter{Store: store, Login: "BillyAcachofa", DeliveryEnabled: true}
	d, n, err := webhooks.ReceiveGitHub(context.Background(), store, f, body, "workflow_run", fmt.Sprintf("delivery:%x", sha256.Sum256(body)), time.Now())
	if err != nil || d.Reason != reason {
		t.Fatalf("receipt: %+v %v (want %s)", d, err, reason)
	}
	return n
}

func TestLateRunRegistrationReconcilesSeenCompletion(t *testing.T) {
	for _, conclusion := range []string{"success", "cancelled"} {
		t.Run(conclusion, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.db")
			store, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { store.Close() }()
			origin := ciOwner(t, store, "-100")
			if err := store.RegisterPR(ctx, origin.TaskID, "https://github.com/o/r/pull/9"); err != nil {
				t.Fatal(err)
			}
			if err := store.PutWebhookState(ctx, "watch", "billyacachofa/o/r#9", `{"head_sha":"abc"}`); err != nil {
				t.Fatal(err)
			}
			body := ciBody(7, 1, 1, "abc", conclusion, "pull_request", "feature")
			ciReceive(t, store, body, "routine_workflow_result")
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			runURL := "https://github.com/o/r/actions/runs/7"
			if err := store.RegisterRun(ctx, origin.TaskID, runURL); err != nil {
				t.Fatal(err)
			}
			pending, err := store.PendingGitHub(ctx)
			if err != nil || len(pending) != 1 {
				t.Fatalf("late completion missing: %v %v", pending, err)
			}
			routed, err := store.AssignTask(ctx, contracts.NewTaskQueueMessage(pending[0], "late", time.Now()))
			if err != nil || routed.WorkItemID != origin.WorkItemID {
				t.Fatalf("owner: %+v %v", routed, err)
			}
			if err := store.AckGitHub(ctx, pending[0].ID); err != nil {
				t.Fatal(err)
			}
			if err := store.RegisterRun(ctx, origin.TaskID, runURL); err != nil {
				t.Fatal(err)
			}
			ciReceive(t, store, body, "duplicate")
			pending, err = store.PendingGitHub(ctx)
			if err != nil || len(pending) != 0 {
				t.Fatalf("completion repeated: %v %v", pending, err)
			}
		})
	}
}

func TestRegisteredRunAdvancesSharedOrdering(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	origin := ciOwner(t, store, "-100")
	if err := store.RegisterPR(ctx, origin.TaskID, "https://github.com/o/r/pull/9"); err != nil {
		t.Fatal(err)
	}
	if err := store.PutWebhookState(ctx, "watch", "billyacachofa/o/r#9", `{"head_sha":"abc"}`); err != nil {
		t.Fatal(err)
	}
	ciReceive(t, store, ciBody(1, 1, 1, "abc", "failure", "pull_request", "feature"), "workflow_failure")
	if err := store.RegisterRun(ctx, origin.TaskID, "https://github.com/o/r/actions/runs/2"); err != nil {
		t.Fatal(err)
	}
	ciReceive(t, store, ciBody(2, 2, 1, "abc", "success", "pull_request", "feature"), "registered_run_completed")
	ciReceive(t, store, ciBody(1, 1, 2, "abc", "failure", "pull_request", "feature"), "superseded_workflow")
	// Explicitly awaiting an older run still delivers its result, but must
	// not move the shared watermark back to that run.
	if err := store.RegisterRun(ctx, origin.TaskID, "https://github.com/o/r/actions/runs/3"); err != nil {
		t.Fatal(err)
	}
	ciReceive(t, store, ciBody(3, 1, 1, "abc", "failure", "pull_request", "feature"), "registered_run_completed")
	ciReceive(t, store, ciBody(1, 1, 3, "abc", "failure", "pull_request", "feature"), "superseded_workflow")
}

func TestNonPRExecutionDoesNotInferOldPROwner(t *testing.T) {
	store := openTestStore(t)
	if err := store.PutWebhookState(context.Background(), "watch", "billyacachofa/o/r#9", `{"head_sha":"abc"}`); err != nil {
		t.Fatal(err)
	}
	ciReceive(t, store, ciBody(1, 1, 1, "abc", "failure", "push", "main"), "workflow_missing_pr_context")
}

func TestLateRunRegistrationReconcilesHeldCompletionAtomically(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	origin := ciOwner(t, store, "-100")
	ciReceive(t, store, ciBody(7, 1, 1, "abc", "success", "push", "main"), "workflow_missing_pr_context")
	if _, err := store.db.Exec(`CREATE TRIGGER fail_run_completion BEFORE INSERT ON github_webhook_pending BEGIN SELECT RAISE(FAIL,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	runURL := "https://github.com/o/r/actions/runs/7"
	if err := store.RegisterRun(ctx, origin.TaskID, runURL); err == nil {
		t.Fatal("expected staging failure")
	}
	var registered int
	if err := store.db.QueryRow(`SELECT count(*) FROM work_item_artifacts WHERE kind='github_run'`).Scan(&registered); err != nil || registered != 0 {
		t.Fatalf("partial registration: %d %v", registered, err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER fail_run_completion`); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterRun(ctx, origin.TaskID, runURL); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingGitHub(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("completion missing after retry: %v %v", pending, err)
	}
	var held int
	if err := store.db.QueryRow(`SELECT count(*) FROM github_webhook_held`).Scan(&held); err != nil || held != 0 {
		t.Fatalf("reconciled completion still held: %d %v", held, err)
	}
}

func TestWorkflowDestinationRecoversAcrossCommits(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	prOwner, ops := ciOwner(t, store, "-100"), ciOwner(t, store, "-200")
	if err := store.RegisterPR(ctx, prOwner.TaskID, "https://github.com/o/r/pull/9"); err != nil {
		t.Fatal(err)
	}
	if err := store.PutWebhookState(ctx, "watch", "billyacachofa/o/r#9", `{"head_sha":"abc"}`); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterWorkflow(ctx, ops.TaskID, "https://github.com/o/r/actions/workflows/20"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id                                     int
		sha, conclusion, event, branch, reason string
		owner                                  contracts.TaskQueueMessage
	}{
		{1, "abc", "failure", "push", "main", "workflow_failure", ops},
		{2, "def", "success", "push", "other", "routine_workflow_result", ops},
		{3, "def", "success", "push", "main", "workflow_recovered", ops},
		{4, "abc", "failure", "pull_request", "feature", "workflow_failure", prOwner},
	} {
		n := ciReceive(t, store, ciBody(tc.id, tc.id, 1, tc.sha, tc.conclusion, tc.event, tc.branch), tc.reason)
		if n != nil {
			r, err := store.AssignTask(ctx, *n)
			if err != nil || r.WorkItemID != tc.owner.WorkItemID {
				t.Fatalf("run %d owner: %+v %v", tc.id, r, err)
			}
		}
	}
	// A normal failure already delivered to this owner must not generate a
	// second completion when that same owner later registers its run.
	before, _ := store.PendingGitHub(ctx)
	if err := store.RegisterRun(ctx, prOwner.TaskID, fmt.Sprintf("https://github.com/o/r/actions/runs/%d", 4)); err != nil {
		t.Fatal(err)
	}
	after, _ := store.PendingGitHub(ctx)
	if len(after) != len(before) {
		t.Fatal("owner notified twice for the same attempt")
	}
}
