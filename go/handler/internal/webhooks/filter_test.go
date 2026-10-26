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

func TestAssignmentStartsWatch(t *testing.T) {
	for _, tc := range []struct {
		name, event, action, sender, assignee, want, reason string
	}{
		{"issue", "issues", "assigned", "alice", "billyacachofa", "keep", "assigned_to_us"},
		{"pull request", "pull_request", "assigned", "alice", "BillyAcachofa", "keep", "assigned_to_us"},
		{"other assignee", "issues", "assigned", "alice", "bob", "drop", "not_tagged_or_watched"},
		{"missing assignee", "issues", "assigned", "alice", "", "drop", "not_tagged_or_watched"},
		{"unassignment", "issues", "unassigned", "alice", "BillyAcachofa", "drop", "not_tagged_or_watched"},
		{"opening with old assignment", "issues", "opened", "alice", "BillyAcachofa", "drop", "not_tagged_or_watched"},
		{"own assignment", "issues", "assigned", "BillyAcachofa", "BillyAcachofa", "drop", "self_action"},
		{"app assignment", "issues", "assigned", "billybotapp[bot]", "BillyAcachofa", "drop", "self_action"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.db")
			store, err := sqlite.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { store.Close() }()
			f := &Filter{Store: store, Login: "BillyAcachofa", SelfLogins: []string{"billybotapp[bot]"}}
			subjectKey := "issue"
			if tc.event == "pull_request" {
				subjectKey = "pull_request"
			}
			payload := map[string]any{
				"action": tc.action, "sender": user{tc.sender}, "assignee": user{tc.assignee},
				"repository": map[string]string{"full_name": "o/r"},
				subjectKey:   map[string]any{"number": 1, "assignees": []user{{"BillyAcachofa"}}},
			}
			body, _ := json.Marshal(payload)
			d, err := f.Evaluate(ctx, body, tc.event, "assignment")
			if err != nil || d.Disposition != tc.want || d.Reason != tc.reason || d.NewWatch != (tc.want == "keep") {
				t.Fatalf("assignment: %+v %v", d, err)
			}
			if tc.want != "keep" {
				return
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlite.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			f.Store = store
			d, err = f.Evaluate(ctx, body, tc.event, "assignment")
			if err != nil || d.Reason != "duplicate" {
				t.Fatalf("redelivery: %+v %v", d, err)
			}
			followup := []byte(`{"action":"created","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"issue":{"number":1},"comment":{"body":"please check this"}}`)
			d, err = f.Evaluate(ctx, followup, "issue_comment", "followup")
			if err != nil || d.Disposition != "keep" || !d.Watched || d.NewWatch {
				t.Fatalf("followup after restart: %+v %v", d, err)
			}
		})
	}
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

func TestOwnPRWatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.Close() }()
	f := &Filter{Store: store, Login: "BillyAcachofa", SelfLogins: []string{"billybotapp[bot]"}}
	check := func(event, body, want, reason string, newWatch bool) {
		t.Helper()
		d, err := f.Evaluate(ctx, []byte(body), event, "")
		if err != nil || d.Disposition != want || d.Reason != reason || d.NewWatch != newWatch {
			t.Fatalf("%s: %+v %v", body, d, err)
		}
	}
	check("pull_request", `{"action":"opened","sender":{"login":"BillyAcachofa"},"repository":{"full_name":"o/r"},"pull_request":{"number":1,"user":{"login":"billyacachofa"},"head":{"sha":"abc"}}}`, "drop", "self_action", true)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	f.Store = store
	check("issue_comment", `{"action":"created","sender":{"login":"alice"},"repository":{"full_name":"o/r"},"issue":{"number":1},"comment":{"body":"please fix this"}}`, "keep", "watched_subject", false)
	check("pull_request_review", `{"action":"submitted","sender":{"login":"alice"},"repository":{"full_name":"o/r"},"pull_request":{"number":1},"review":{"body":"changes requested"}}`, "keep", "watched_subject", false)
	check("workflow_run", `{"action":"completed","sender":{"login":"BillyAcachofa"},"repository":{"full_name":"o/r"},"workflow_run":{"workflow_id":10,"run_number":1,"run_attempt":1,"head_sha":"abc","conclusion":"failure","pull_requests":[{"number":1}]}}`, "keep", "workflow_failure", false)
	check("issue_comment", `{"action":"created","sender":{"login":"BillyAcachofa"},"repository":{"full_name":"o/r"},"issue":{"number":1},"comment":{"body":"fixed"}}`, "drop", "self_action", false)
	// PR author metadata can establish a watch even if the opening hook was missed.
	check("issue_comment", `{"action":"created","sender":{"login":"alice"},"repository":{"full_name":"o/r"},"issue":{"number":2,"user":{"login":"BillyAcachofa"},"pull_request":{"url":"https://api.github.com/repos/o/r/pulls/2"}},"comment":{"body":"feedback"}}`, "keep", "own_pr", true)
	check("pull_request", `{"action":"opened","sender":{"login":"billybotapp[bot]"},"repository":{"full_name":"o/r"},"pull_request":{"number":3,"user":{"login":"billybotapp[bot]"}}}`, "drop", "self_action", true)
	// Authorship alone must not subscribe ordinary issues or other people's PRs.
	check("issue_comment", `{"action":"created","sender":{"login":"alice"},"repository":{"full_name":"o/r"},"issue":{"number":4,"user":{"login":"BillyAcachofa"}},"comment":{"body":"feedback"}}`, "drop", "not_tagged_or_watched", false)
	check("pull_request", `{"action":"opened","sender":{"login":"alice"},"repository":{"full_name":"o/r"},"pull_request":{"number":5,"user":{"login":"alice"}}}`, "drop", "not_tagged_or_watched", false)
}
