package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func reminderInput(key string, runAt time.Time) ReminderInput {
	return ReminderInput{
		RunAt: runAt, Prompt: "Take the bins out", ContextRefs: []string{"repo:/srv/chatting/workspace"},
		ReplyChannelType: "telegram", ReplyChannelTarget: "-123",
		ReplyChannelMetadata: map[string]any{"message_id": float64(42)},
		CreatedFromTaskID:    "task:telegram:1", CreatedBy: "worker", IdempotencyKey: key,
	}
}

func TestReminderLifecycleAndIdempotency(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	runAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	created, replay, err := store.CreateReminder(ctx, reminderInput("create-1", runAt))
	if err != nil {
		t.Fatalf("CreateReminder: %v", err)
	}
	if replay || created.Revision != 1 || created.Status != ReminderStatusScheduled {
		t.Fatalf("created = %+v replay=%v", created, replay)
	}

	retried, replay, err := store.CreateReminder(ctx, reminderInput("create-1", runAt))
	if err != nil || !replay || retried.ReminderID != created.ReminderID {
		t.Fatalf("retry = %+v replay=%v err=%v", retried, replay, err)
	}
	conflicting := reminderInput("create-1", runAt)
	conflicting.Prompt = "Different"
	if _, _, err := store.CreateReminder(ctx, conflicting); !errors.Is(err, ErrReminderIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}

	updatedInput := reminderInput("update-1", runAt.Add(time.Hour))
	updatedInput.Prompt = "Take recycling out"
	updated, replay, err := store.ReplaceReminder(ctx, created.ReminderID, updatedInput)
	if err != nil || replay || updated.Revision != 2 {
		t.Fatalf("updated = %+v replay=%v err=%v", updated, replay, err)
	}
	updatedAgain, replay, err := store.ReplaceReminder(ctx, created.ReminderID, updatedInput)
	if err != nil || !replay || updatedAgain.Revision != 2 {
		t.Fatalf("update retry = %+v replay=%v err=%v", updatedAgain, replay, err)
	}

	history, err := store.GetReminderHistory(ctx, created.ReminderID)
	if err != nil || len(history) != 2 {
		t.Fatalf("history = %+v err=%v", history, err)
	}
	if history[0].Status != ReminderStatusScheduled || history[1].Status != ReminderStatusCancelled || history[1].CancelledAt == nil {
		t.Fatalf("unexpected history: %+v", history)
	}

	due, err := store.ListDueReminders(ctx, runAt.Add(2*time.Hour))
	if err != nil || len(due) != 1 || due[0].Revision != 2 {
		t.Fatalf("due = %+v err=%v", due, err)
	}
	firedAt := runAt.Add(2 * time.Hour)
	changed, err := store.MarkReminderFired(ctx, created.ReminderID, 2, firedAt)
	if err != nil || !changed {
		t.Fatalf("MarkReminderFired changed=%v err=%v", changed, err)
	}
	changed, err = store.MarkReminderFired(ctx, created.ReminderID, 2, firedAt)
	if err != nil || changed {
		t.Fatalf("second MarkReminderFired changed=%v err=%v", changed, err)
	}
	latest, err := store.GetLatestReminder(ctx, created.ReminderID)
	if err != nil || latest == nil || latest.Status != ReminderStatusFired || latest.FiredAt == nil {
		t.Fatalf("latest = %+v err=%v", latest, err)
	}
}

func TestReminderCancelKeepsHistory(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	created, _, err := store.CreateReminder(ctx, reminderInput("cancel-1", time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.CancelReminder(ctx, created.ReminderID)
	if err != nil || !cancelled {
		t.Fatalf("cancelled=%v err=%v", cancelled, err)
	}
	cancelled, err = store.CancelReminder(ctx, created.ReminderID)
	if err != nil || cancelled {
		t.Fatalf("second cancel cancelled=%v err=%v", cancelled, err)
	}
	latest, err := store.GetLatestReminder(ctx, created.ReminderID)
	if err != nil || latest == nil || latest.Status != ReminderStatusCancelled {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
}
