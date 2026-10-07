package sqlite

import (
	"context"
	"testing"
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
	if notification.Envelope.ReplyChannel.Type != "telegram" || notification.Envelope.ReplyChannel.Target != "-100" {
		t.Fatal("PR notification did not inherit origin reply route")
	}
	replayed, err := store.AssignTask(ctx, originalNotification)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.WorkItemID != first.WorkItemID || replayed.Envelope.ReplyChannel.Type != "telegram" {
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
