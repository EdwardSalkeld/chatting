package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/webhooks"
)

func TestGitHubSubjectOwnership(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	origin := testTaskMessage(t)
	origin.Envelope.Source = "im"
	origin.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": 7}}
	origin, err := store.AssignTask(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterPR(ctx, origin.TaskID, "https://github.com/o/r/pull/9"); err != nil {
		t.Fatal(err)
	}
	notify := func(event, body string) contracts.TaskQueueMessage {
		t.Helper()
		task, err := webhooks.GitHubNotification([]byte(body), event, "delivery", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		task, err = store.AssignTask(ctx, task)
		if err != nil {
			t.Fatal(err)
		}
		return task
	}
	feedback := notify("pull_request_review", `{"repository":{"full_name":"O/R"},"pull_request":{"number":9},"review":{"id":5}}`)
	if feedback.WorkItemID != origin.WorkItemID || feedback.Envelope.ReplyChannel.Target != "https://github.com/O/R/pull/9" || len(feedback.Envelope.ConversationChannels) != 1 || feedback.Envelope.ConversationChannels[0].Metadata["message_thread_id"] != float64(7) {
		t.Fatalf("lost origin channel/item: %+v", feedback)
	}
	issue := notify("issue_comment", `{"repository":{"full_name":"o/r"},"issue":{"number":2},"comment":{"id":6}}`)
	if issue.WorkItemID == origin.WorkItemID || issue.Envelope.ReplyChannel.Target != "https://github.com/o/r/issues/2" {
		t.Fatal("unowned tag-in did not receive a dedicated item")
	}
	followup := notify("issues", `{"repository":{"full_name":"o/r"},"issue":{"number":2},"action":"closed"}`)
	if followup.WorkItemID != issue.WorkItemID {
		t.Fatal("subject changed owner")
	}
	if err := store.RegisterSubject(ctx, origin.TaskID, "https://github.com/o/r/issues/2"); err == nil {
		t.Fatal("conflicting ownership accepted")
	}
	if err := store.RegisterSubject(ctx, origin.TaskID, "https://github.com/o/r/issues/3"); err != nil {
		t.Fatal(err)
	}
	linked := notify("issues", `{"repository":{"full_name":"o/r"},"issue":{"number":3},"action":"closed"}`)
	if linked.WorkItemID != origin.WorkItemID {
		t.Fatal("related issue did not join PR item")
	}
	if err := store.RegisterRepository(ctx, origin.TaskID, "o/r"); err != nil {
		t.Fatal(err)
	}
	release := notify("release", `{"repository":{"full_name":"o/r"},"release":{"id":10},"action":"published"}`)
	if release.WorkItemID != origin.WorkItemID {
		t.Fatal("release did not use explicit repository owner")
	}
}

func TestRelatedPRCommentPreservesEventAndOriginContext(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	issue, err := webhooks.GitHubNotification([]byte(`{"repository":{"full_name":"o/r"},"issue":{"number":70},"action":"assigned"}`), "issues", "issue", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	issue, err = store.AssignTask(ctx, issue)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterPR(ctx, issue.TaskID, "https://github.com/o/r/pull/71"); err != nil {
		t.Fatal(err)
	}
	comment, err := webhooks.GitHubNotification([]byte(`{"repository":{"full_name":"o/r"},"issue":{"number":71,"pull_request":{}},"comment":{"id":123},"action":"created"}`), "issue_comment", "comment", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	original := comment.Envelope.ReplyChannel
	for i := 0; i < 2; i++ {
		comment, err = store.AssignTask(ctx, comment)
		if err != nil {
			t.Fatal(err)
		}
		if comment.WorkItemID != issue.WorkItemID || comment.Envelope.ReplyChannel.Target != "https://github.com/o/r/pull/71" || comment.Envelope.ReplyChannel.Metadata["comment_id"] != original.Metadata["comment_id"] {
			t.Fatalf("PR comment origin changed: %+v", comment)
		}
		if len(comment.Envelope.ConversationChannels) != 1 || comment.Envelope.ConversationChannels[0].Target != "https://github.com/o/r/issues/70" {
			t.Fatalf("lost associated issue: %+v", comment)
		}
	}
}

func TestReceiptRollsBackOnStagingFailure(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	f := &webhooks.Filter{Store: store, Login: "BillyAcachofa", DeliveryEnabled: true}
	body := []byte(`{"action":"created","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"issue":{"number":1},"comment":{"id":5,"body":"@BillyAcachofa help"}}`)
	if _, err := store.db.Exec(`CREATE TRIGGER fail_stage BEFORE INSERT ON github_webhook_pending BEGIN SELECT RAISE(FAIL, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := webhooks.ReceiveGitHub(ctx, store, f, body, "issue_comment", "one", time.Now()); err == nil {
		t.Fatal("expected staging failure")
	}
	watch, err := store.WebhookState(ctx, "watch", "billyacachofa/o/r#1")
	if err != nil || watch != "" {
		t.Fatal("failed receipt committed watch", err)
	}
	if _, err := store.db.Exec(`DROP TRIGGER fail_stage`); err != nil {
		t.Fatal(err)
	}
	d, _, err := webhooks.ReceiveGitHub(ctx, store, f, body, "issue_comment", "one", time.Now())
	if err != nil || d.Disposition != "keep" {
		t.Fatalf("retry swallowed after staging failure: %+v %v", d, err)
	}
	// A release with no registered destination is held, never sent to General.
	f.ReleaseRepositories = []string{"o/other"}
	release := []byte(`{"action":"published","repository":{"full_name":"o/other"},"sender":{"login":"alice"},"release":{"id":9}}`)
	d, _, err = webhooks.ReceiveGitHub(ctx, store, f, release, "release", "release-one", time.Now())
	if err != nil || d.Disposition != "needs_context" {
		t.Fatalf("unowned release not held: %+v %v", d, err)
	}
}
