package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/routing"
)

func TestPersistentLaneAssignments(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	base := testTaskMessage(t)
	base.TaskID = "task:telegram-first"
	base.Envelope.ID = "im:first"
	base.Envelope.Source = "im"
	base.Envelope.ReplyChannel.Type = "telegram"
	base.Envelope.ReplyChannel.Target = "-100"
	base.Envelope.ReplyChannel.Metadata = map[string]any{"message_thread_id": int64(7)}
	first, err := store.AssignTask(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if first.WorkItemID == "" {
		t.Fatal("missing lane identity")
	}

	second := base
	second.TaskID = "task:second"
	second.Envelope.ID = "im:second"
	second.Envelope.Content = "A completely different objective"
	second, err = store.AssignTask(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if second.WorkItemID != first.WorkItemID {
		t.Fatal("same topic changed lanes")
	}

	otherTopic := base
	otherTopic.TaskID = "task:other-topic"
	otherTopic.Envelope.ID = "im:other-topic"
	otherTopic.Envelope.ReplyChannel.Metadata = map[string]any{"message_thread_id": int64(8)}
	otherTopic, err = store.AssignTask(ctx, otherTopic)
	if err != nil {
		t.Fatal(err)
	}
	if otherTopic.WorkItemID == first.WorkItemID {
		t.Fatal("different topics shared lane")
	}

	email := testTaskMessage(t)
	email, err = store.AssignTask(ctx, email)
	if err != nil {
		t.Fatal(err)
	}
	otherEmail := testTaskMessage(t)
	otherEmail.TaskID = "task:other-email"
	otherEmail.Envelope.ID = "email:other"
	otherEmail, err = store.AssignTask(ctx, otherEmail)
	if err != nil {
		t.Fatal(err)
	}
	if email.WorkItemID != otherEmail.WorkItemID {
		t.Fatal("unmatched email should use general lane")
	}

	if err = store.RegisterPR(ctx, base.TaskID, "https://github.com/owner/repo/pull/50"); err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterPR(ctx, base.TaskID, "https://github.com/owner/repo/pull/50"); err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterPR(ctx, email.TaskID, "https://github.com/owner/repo/pull/50"); err == nil {
		t.Fatal("conflicting PR owner accepted")
	}
	notification := testTaskMessage(t)
	notification.TaskID = "task:notification"
	notification.Envelope.ID = "email:notification"
	actor := "notifications@github.com"
	notification.Envelope.Actor = &actor
	notification.Envelope.Content = "CI failed: https://github.com/owner/repo/pull/50/checks"
	originalNotification := notification
	notification, err = store.AssignTask(ctx, notification)
	if err != nil {
		t.Fatal(err)
	}
	if notification.WorkItemID != first.WorkItemID {
		t.Fatal("PR notification did not return to origin lane")
	}
	if !reflect.DeepEqual(notification.Envelope.ReplyChannel, originalNotification.Envelope.ReplyChannel) {
		t.Fatal("PR notification origin was replaced")
	}
	if len(notification.Envelope.ConversationChannels) != 1 || notification.Envelope.ConversationChannels[0].Target != "-100" {
		t.Fatal("PR notification lost associated Telegram context")
	}
	replayed, err := store.AssignTask(ctx, originalNotification)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.WorkItemID != first.WorkItemID || replayed.Envelope.ReplyChannel.Type != "email" || len(replayed.Envelope.ConversationChannels) != 1 {
		t.Fatal("replayed notification lost its route")
	}
	if err = store.RecordTask(ctx, notification); err != nil {
		t.Fatal(err)
	}
	var recordedID string
	if err = store.db.QueryRowContext(ctx, `SELECT work_item_id FROM task_ledger WHERE task_id = ?`, notification.TaskID).Scan(&recordedID); err != nil {
		t.Fatal(err)
	}
	if recordedID != first.WorkItemID {
		t.Fatal("task ledger lost lane ID")
	}
}

func TestCINotificationRoutesThroughItsWorkflowRun(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	origin := testTaskMessage(t)
	origin.TaskID = "task:telegram-origin"
	origin.Envelope.ID = "im:origin"
	origin.Envelope.Source = "im"
	origin.Envelope.ReplyChannel.Type = "telegram"
	origin.Envelope.ReplyChannel.Target = "-100"
	origin, err := store.AssignTask(ctx, origin)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RegisterPR(ctx, origin.TaskID, "https://github.com/owner/repo/pull/50"); err != nil {
		t.Fatal(err)
	}
	store.ResolveRunsWith(func(_ context.Context, run routing.RunRef) ([]string, error) {
		if run.Repo == "owner/repo" && run.ID == 99 {
			return []string{"owner/repo#50"}, nil
		}
		return nil, errors.New("unknown run")
	})
	actor := "notifications@github.com"
	ciEmail := func(id string, run string) contracts.TaskQueueMessage {
		task := testTaskMessage(t)
		task.TaskID = "task:" + id
		task.Envelope.ID = id
		task.Envelope.Actor = &actor
		task.Envelope.Content = "Subject: [Owner/Repo] PR run failed: CI - Fix it (abc1234)\n\n" +
			"View results: https://github.com/Owner/Repo/actions/runs/" + run + "\n"
		return task
	}

	routed, err := store.AssignTask(ctx, ciEmail("email:ci", "99"))
	if err != nil {
		t.Fatal(err)
	}
	if routed.WorkItemID != origin.WorkItemID || routed.Envelope.ReplyChannel.Type != "email" || len(routed.Envelope.ConversationChannels) != 1 {
		t.Fatalf("CI email did not return to the PR's work item: %#v", routed)
	}

	unresolved, err := store.AssignTask(ctx, ciEmail("email:ci-unknown", "100"))
	if err != nil {
		t.Fatal(err)
	}
	if unresolved.WorkItemID == origin.WorkItemID {
		t.Fatal("an unresolvable run should fall back to the general lane")
	}
}

func TestRepliesInAChatShareItsWorkItem(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	chat := testTaskMessage(t)
	chat.TaskID = "task:telegram:1"
	chat.Envelope.Source = "im"
	chat.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": int64(7)}}
	chat, err := store.AssignTask(ctx, chat)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"reminder", "cron"} {
		task := testTaskMessage(t)
		task.TaskID = "task:" + source
		task.Envelope.Source = source
		task.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": int64(7)}}
		task, err = store.AssignTask(ctx, task)
		if err != nil {
			t.Fatal(err)
		}
		if task.WorkItemID != chat.WorkItemID {
			t.Errorf("%s replying in the chat did not join its work item", source)
		}
	}
	notice := testTaskMessage(t)
	notice.TaskID = "task:notice"
	notice.Envelope.Source = "internal"
	notice.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": int64(7)}}
	notice, err = store.AssignTask(ctx, notice)
	if err != nil {
		t.Fatal(err)
	}
	if notice.WorkItemID == chat.WorkItemID {
		t.Fatal("an internal notice joined a chat's work item")
	}
}

func TestUnrelatedWorkGetsSeparateItemsAndKeepsItsReplyRoute(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	alice := testTaskMessage(t)
	alice, err := store.AssignTask(ctx, alice)
	if err != nil {
		t.Fatal(err)
	}
	bob := testTaskMessage(t)
	bob.TaskID = "task:email:bob"
	bob.Envelope.ID = "email:bob"
	bob.Envelope.ReplyChannel.Target = "bob@example.com"
	bob, err = store.AssignTask(ctx, bob)
	if err != nil {
		t.Fatal(err)
	}
	if alice.WorkItemID == bob.WorkItemID {
		t.Fatal("unrelated senders shared a work item")
	}

	// A PR opened from an email request does not redirect later GitHub mail
	// to that email address.
	if err = store.RegisterPR(ctx, alice.TaskID, "https://github.com/owner/repo/pull/5"); err != nil {
		t.Fatal(err)
	}
	actor := "notifications@github.com"
	notification := testTaskMessage(t)
	notification.TaskID = "task:email:notification"
	notification.Envelope.ID = "email:notification"
	notification.Envelope.Actor = &actor
	notification.Envelope.ReplyChannel.Target = "notifications@github.com"
	notification.Envelope.Content = "Review: https://github.com/owner/repo/pull/5#pullrequestreview-1"
	notification, err = store.AssignTask(ctx, notification)
	if err != nil {
		t.Fatal(err)
	}
	if notification.WorkItemID != alice.WorkItemID {
		t.Fatal("PR notification did not return to the work item that opened the PR")
	}
	if notification.Envelope.ReplyChannel.Target != "notifications@github.com" {
		t.Fatalf("reply route was replaced: %#v", notification.Envelope.ReplyChannel)
	}
}
