package webhooks

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestTagWatchLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "handler.db")
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	f := &Filter{Store: store, Login: "BillyAcachofa", SelfLogins: []string{"billybotapp[bot]"}}
	evaluate := func(event, body, disposition, reason string) Decision {
		t.Helper()
		d, err := f.Evaluate(ctx, []byte(body), event, "")
		if err != nil || d.Disposition != disposition || d.Reason != reason {
			t.Fatalf("%s: %+v %v", body, d, err)
		}
		return d
	}
	comment := func(repo, actor, text string, number int) string {
		return fmt.Sprintf(`{"action":"created","repository":{"full_name":%q},"sender":{"login":%q},"issue":{"number":%d,"body":"@BillyAcachofa old mention"},"comment":{"body":%q}}`, repo, actor, number, text)
	}
	evaluate("issue_comment", comment("o/r", "alice", "hello", 1), "drop", "not_tagged_or_watched")
	evaluate("issue_comment", comment("o/r", "BillyAcachofa", "@BillyAcachofa", 1), "drop", "self_action")
	evaluate("issue_comment", comment("o/r", "billybotapp[bot]", "@BillyAcachofa", 1), "drop", "self_action")
	evaluate("issue_comment", comment("o/r", "alice", "@BillyAcachofa-extra", 1), "drop", "not_tagged_or_watched")
	tag := comment("o/r", "alice", "please @billyacachofa help", 1)
	d := evaluate("issue_comment", tag, "keep", "tagged_in")
	if !d.NewWatch || !d.Watched {
		t.Fatal(d)
	}
	evaluate("issue_comment", tag, "drop", "duplicate")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	f.Store = store
	evaluate("issue_comment", tag, "drop", "duplicate")
	evaluate("issue_comment", comment("o/r", "alice", "follow up", 1), "keep", "watched_subject")
	evaluate("issue_comment", comment("o/other", "alice", "follow up", 1), "drop", "not_tagged_or_watched")
	evaluate("issue_comment", comment("o/r", "alice", "follow up", 2), "drop", "not_tagged_or_watched")
	evaluate("issue_comment", comment("o/r", "billyacachofa", "own response", 1), "drop", "self_action")
	f.Login = "someone-else"
	evaluate("issue_comment", comment("o/r", "alice", "another response", 1), "drop", "not_tagged_or_watched")
}

func TestActionRulesAndWorkflowRecovery(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	f := &Filter{Store: store, Login: "BillyAcachofa", ReleaseRepositories: []string{"o/r"}}
	check := func(event, action string, extra map[string]any, want, reason string) {
		t.Helper()
		p := map[string]any{"repository": map[string]any{"full_name": "o/r"}, "sender": user{"alice"}, "action": action}
		for k, v := range extra {
			p[k] = v
		}
		body, _ := json.Marshal(p)
		d, err := f.Evaluate(ctx, body, event, "")
		if err != nil || d.Disposition != want || d.Reason != reason {
			t.Fatalf("%s %s: %+v %v", event, action, d, err)
		}
	}
	pr := map[string]any{"number": 1, "body": "@BillyAcachofa", "head": map[string]any{"sha": "abc"}}
	check("pull_request", "opened", map[string]any{"pull_request": pr}, "keep", "tagged_in")
	for _, tc := range []struct{ event, action, want, reason string }{
		{"issues", "labeled", "drop", "action_noise"},
		{"issues", "closed", "keep", "watched_subject"},
		{"pull_request", "review_request_removed", "keep", "watched_subject"},
		{"pull_request_review", "submitted", "keep", "watched_subject"},
		{"pull_request_review", "dismissed", "keep", "watched_subject"},
		{"pull_request_review_comment", "deleted", "drop", "action_noise"},
		{"pull_request_review_thread", "resolved", "keep", "watched_subject"},
		{"pull_request_review_thread", "unresolved", "keep", "watched_subject"},
	} {
		check(tc.event, tc.action, map[string]any{"pull_request": pr, "review": textObject{}}, tc.want, tc.reason)
	}
	check("issue_comment", "edited", map[string]any{"issue": pr, "comment": textObject{" hello  world "}, "changes": map[string]any{"body": map[string]any{"from": "hello world"}}}, "drop", "cosmetic_edit")
	check("issue_comment", "edited", map[string]any{"issue": pr, "comment": textObject{"new request"}, "changes": map[string]any{"body": map[string]any{"from": "old request"}}}, "keep", "meaningful_edit")
	check("issues", "edited", map[string]any{"issue": pr}, "needs_context", "missing_edit_history")
	check("release", "published", map[string]any{"release": map[string]any{"draft": false, "prerelease": false}}, "keep", "published_release")
	check("release", "published", map[string]any{"release": map[string]any{"prerelease": true}}, "drop", "release_noise")
	run := &workflow{ID: 1, WorkflowID: 10, RunNumber: 1, RunAttempt: 1, HeadSHA: "abc", Conclusion: "failure", PullRequests: []subject{{Number: 1}}}
	check("workflow_run", "completed", map[string]any{"workflow_run": run, "sender": user{"BillyAcachofa"}}, "keep", "workflow_failure")
	run.RunAttempt = 2
	run.Conclusion = "success"
	check("workflow_run", "completed", map[string]any{"workflow_run": run}, "keep", "workflow_recovered")
	run.RunAttempt = 3
	check("workflow_run", "completed", map[string]any{"workflow_run": run}, "drop", "routine_workflow_result")
	run.RunAttempt = 1
	run.Conclusion = "timed_out"
	check("workflow_run", "completed", map[string]any{"workflow_run": run}, "drop", "superseded_workflow")
	pr["head"] = map[string]any{"sha": "def"}
	check("pull_request", "synchronize", map[string]any{"pull_request": pr, "sender": user{"BillyAcachofa"}}, "drop", "self_action")
	run.RunAttempt = 4
	check("workflow_run", "completed", map[string]any{"workflow_run": run}, "drop", "workflow_unwatched_or_stale_commit")
	run.HeadSHA = "def"
	check("workflow_run", "completed", map[string]any{"workflow_run": run}, "keep", "workflow_failure")
	run.PullRequests = nil
	check("workflow_run", "completed", map[string]any{"workflow_run": run}, "needs_context", "workflow_missing_pr_context")
}

func TestMentionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"@BillyAcachofa", true}, {"hi (@billyacachofa)", true}, {"@BillyAcachofa-other", false}, {"@BillyAcachofax", false}, {"me@BillyAcachofa.com", false}, {"https://github.com/@BillyAcachofa", false},
	} {
		if mention(tc.text, "BillyAcachofa") != tc.want {
			t.Error(tc.text)
		}
	}
}
