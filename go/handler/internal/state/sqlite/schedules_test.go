package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestScheduleLifecycle(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	created, err := store.CreateSchedule(ctx, ScheduleInput{
		JobName:            "daily-report",
		Content:            "summarise yesterday",
		Cron:               "0 9 * * *",
		ContextRefs:        []string{"ref-a"},
		ReplyChannelType:   "telegram",
		ReplyChannelTarget: "12345",
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}
	if created.Version != 1 {
		t.Fatalf("expected version 1, got %d", created.Version)
	}
	if created.Status != "active" {
		t.Fatalf("expected status active, got %q", created.Status)
	}
	if created.Timezone != "UTC" {
		t.Fatalf("expected default timezone UTC, got %q", created.Timezone)
	}
	if created.CreatedBy != "api" {
		t.Fatalf("expected default created_by api, got %q", created.CreatedBy)
	}
	if created.ScheduleID == "" {
		t.Fatal("expected a generated schedule_id")
	}

	active, err := store.ListActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("ListActiveSchedules: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active schedule, got %d", len(active))
	}
	if !reflect.DeepEqual(active[0].ContextRefs, []string{"ref-a"}) {
		t.Fatalf("unexpected context_refs: %v", active[0].ContextRefs)
	}
	if active[0].PromptContext == nil || len(active[0].PromptContext) != 0 {
		t.Fatalf("expected empty prompt_context slice, got %v", active[0].PromptContext)
	}

	got, err := store.GetActiveSchedule(ctx, created.ScheduleID)
	if err != nil {
		t.Fatalf("GetActiveSchedule: %v", err)
	}
	if got == nil || got.ScheduleID != created.ScheduleID {
		t.Fatalf("GetActiveSchedule returned %+v", got)
	}

	replaced, err := store.ReplaceSchedule(ctx, created.ScheduleID, ScheduleInput{
		JobName:   "daily-report",
		Content:   "summarise the past week",
		Cron:      "0 10 * * 1",
		Timezone:  "Europe/London",
		CreatedBy: "ed",
	})
	if err != nil {
		t.Fatalf("ReplaceSchedule: %v", err)
	}
	if replaced.Version != 2 {
		t.Fatalf("expected version 2, got %d", replaced.Version)
	}
	if replaced.Timezone != "Europe/London" {
		t.Fatalf("expected timezone Europe/London, got %q", replaced.Timezone)
	}
	if replaced.CreatedBy != "ed" {
		t.Fatalf("expected created_by ed, got %q", replaced.CreatedBy)
	}

	active, err = store.ListActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("ListActiveSchedules after replace: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected exactly 1 active schedule after replace, got %d", len(active))
	}
	if active[0].Version != 2 {
		t.Fatalf("expected active version 2, got %d", active[0].Version)
	}

	history, err := store.GetScheduleHistory(ctx, created.ScheduleID)
	if err != nil {
		t.Fatalf("GetScheduleHistory: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 history rows, got %d", len(history))
	}
	if history[0].Version != 2 || history[1].Version != 1 {
		t.Fatalf("expected history ordered version desc, got %d then %d", history[0].Version, history[1].Version)
	}
	if history[1].Status != "dead" {
		t.Fatalf("expected superseded version to be dead, got %q", history[1].Status)
	}
	if history[1].SupersededAt == nil {
		t.Fatal("expected superseded_at to be set on the dead version")
	}

	deleted, err := store.MarkScheduleDead(ctx, created.ScheduleID)
	if err != nil {
		t.Fatalf("MarkScheduleDead: %v", err)
	}
	if !deleted {
		t.Fatal("expected MarkScheduleDead to report a change")
	}

	active, err = store.ListActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("ListActiveSchedules after delete: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected no active schedules after delete, got %d", len(active))
	}

	gone, err := store.GetActiveSchedule(ctx, created.ScheduleID)
	if err != nil {
		t.Fatalf("GetActiveSchedule after delete: %v", err)
	}
	if gone != nil {
		t.Fatalf("expected nil active schedule after delete, got %+v", gone)
	}

	history, err = store.GetScheduleHistory(ctx, created.ScheduleID)
	if err != nil {
		t.Fatalf("GetScheduleHistory after delete: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected history retained after delete, got %d rows", len(history))
	}
}

func TestReplaceScheduleMissingReturnsSentinel(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	_, err := store.ReplaceSchedule(ctx, "sched_missing", ScheduleInput{
		JobName: "x",
		Content: "y",
		Cron:    "0 9 * * *",
	})
	if !errors.Is(err, ErrScheduleNotFound) {
		t.Fatalf("expected ErrScheduleNotFound, got %v", err)
	}
}

func TestMarkScheduleDeadMissingReturnsFalse(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)

	deleted, err := store.MarkScheduleDead(ctx, "sched_missing")
	if err != nil {
		t.Fatalf("MarkScheduleDead: %v", err)
	}
	if deleted {
		t.Fatal("expected MarkScheduleDead to report no change for a missing schedule")
	}
}
