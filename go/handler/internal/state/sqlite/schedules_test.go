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

	if _, err := store.ReplaceSchedule(ctx, "sched_missing", ScheduleInput{JobName: "x", Content: "y", Cron: "0 9 * * *"}); !errors.Is(err, ErrScheduleNotFound) {
		t.Fatalf("expected ErrScheduleNotFound, got %v", err)
	}
	created, err := store.CreateSchedule(ctx, ScheduleInput{
		JobName: "daily-report", Content: "summarise yesterday", Cron: "0 9 * * *",
		ContextRefs: []string{"ref-a"}, ReplyChannelType: "telegram", ReplyChannelTarget: "12345",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ScheduleID == "" || created.Version != 1 || created.Status != "active" || created.Timezone != "UTC" || created.CreatedBy != "api" {
		t.Fatalf("created = %+v", created)
	}
	active, err := store.ListActiveSchedules(ctx)
	if err != nil || len(active) != 1 || !reflect.DeepEqual(active[0].ContextRefs, []string{"ref-a"}) || active[0].PromptContext == nil {
		t.Fatalf("active = %+v err=%v", active, err)
	}

	replaced, err := store.ReplaceSchedule(ctx, created.ScheduleID, ScheduleInput{
		JobName: "daily-report", Content: "summarise the past week", Cron: "0 10 * * 1", Timezone: "Europe/London", CreatedBy: "ed",
	})
	if err != nil || replaced.Version != 2 || replaced.Timezone != "Europe/London" || replaced.CreatedBy != "ed" {
		t.Fatalf("replaced = %+v err=%v", replaced, err)
	}
	active, err = store.ListActiveSchedules(ctx)
	if err != nil || len(active) != 1 || active[0].Version != 2 {
		t.Fatalf("active after replace = %+v err=%v", active, err)
	}
	history, err := store.GetScheduleHistory(ctx, created.ScheduleID)
	if err != nil || len(history) != 2 || history[0].Version != 2 || history[1].Status != "dead" || history[1].SupersededAt == nil {
		t.Fatalf("history = %+v err=%v", history, err)
	}

	for _, want := range []bool{true, false} {
		if deleted, err := store.MarkScheduleDead(ctx, created.ScheduleID); err != nil || deleted != want {
			t.Fatalf("MarkScheduleDead = %v err=%v, want %v", deleted, err, want)
		}
	}
	if gone, err := store.GetActiveSchedule(ctx, created.ScheduleID); err != nil || gone != nil {
		t.Fatalf("active schedule after delete = %+v err=%v", gone, err)
	}
	if history, err = store.GetScheduleHistory(ctx, created.ScheduleID); err != nil || len(history) != 2 {
		t.Fatalf("history not retained after delete: %d rows err=%v", len(history), err)
	}
}
