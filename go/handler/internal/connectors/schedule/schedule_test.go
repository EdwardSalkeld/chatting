package schedule

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPollEmitsDueJobWithTaskInstructionsAndRefs(t *testing.T) {
	now := mustTime(t, "2026-03-09T12:34:56Z")
	connector, err := NewFromSource(&fakeSource{scheduled: []Scheduled{{
		ScheduleID: "sched_daily",
		Job: Job{
			JobName:       "daily-note",
			Content:       "Write the daily note",
			Cron:          "34 12 * * *",
			ContextRefs:   []string{"repo:/workspace/chatting"},
			PromptContext: []string{"Keep it terse."},
		},
	}}}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(envelopes) != 1 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	envelope := envelopes[0]
	if envelope.ID != "cron:daily-note:2026-03-09T12:34:00+00:00" {
		t.Fatalf("ID = %q", envelope.ID)
	}
	if envelope.Source != Source {
		t.Fatalf("Source = %q", envelope.Source)
	}
	if envelope.ReceivedAt.Time != now.UTC() {
		t.Fatalf("ReceivedAt = %s", envelope.ReceivedAt.Time)
	}
	if envelope.Content != "Write the daily note" {
		t.Fatalf("Content = %q", envelope.Content)
	}
	if got := strings.Join(envelope.ContextRefs, ","); got != "repo:/workspace/chatting" {
		t.Fatalf("ContextRefs = %#v", envelope.ContextRefs)
	}
	if envelope.ReplyChannel.Type != "log" || envelope.ReplyChannel.Target != "daily-note" {
		t.Fatalf("ReplyChannel = %#v", envelope.ReplyChannel)
	}
	if envelope.DedupeKey != envelope.ID {
		t.Fatalf("DedupeKey = %q", envelope.DedupeKey)
	}
	if envelope.PromptContext == nil {
		t.Fatal("PromptContext = nil")
	}
	if got := strings.Join(envelope.PromptContext.TaskInstructions, ","); got != "Keep it terse." {
		t.Fatalf("TaskInstructions = %#v", envelope.PromptContext.TaskInstructions)
	}

	envelopes, err = connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 0 {
		t.Fatalf("second poll envelopes = %#v", envelopes)
	}
}

func TestPollUsesConfiguredTimezoneAndReplyChannel(t *testing.T) {
	connector, err := NewFromSource(&fakeSource{scheduled: []Scheduled{{
		ScheduleID: "sched_morning",
		Job: Job{
			JobName:            "morning",
			Content:            "Morning note",
			Cron:               "5 7 * * *",
			TimezoneName:       "Europe/London",
			ReplyChannelType:   "telegram",
			ReplyChannelTarget: "8605042448",
		},
	}}}, func() time.Time { return mustTime(t, "2026-07-01T06:05:30Z") })
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(envelopes) != 1 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	if envelopes[0].ID != "cron:morning:2026-07-01T06:05:00+00:00" {
		t.Fatalf("ID = %q", envelopes[0].ID)
	}
	if envelopes[0].ReplyChannel.Type != "telegram" || envelopes[0].ReplyChannel.Target != "8605042448" {
		t.Fatalf("ReplyChannel = %#v", envelopes[0].ReplyChannel)
	}
}

type fakeSource struct {
	scheduled []Scheduled
}

func (source *fakeSource) ActiveSchedules(context.Context) ([]Scheduled, error) {
	return source.scheduled, nil
}

func TestPollFromSourceFiresDueScheduleOnce(t *testing.T) {
	now := mustTime(t, "2026-03-09T12:34:56Z")
	source := &fakeSource{scheduled: []Scheduled{
		{
			ScheduleID: "sched_a",
			Job: Job{
				JobName: "daily-note",
				Content: "Write the daily note",
				Cron:    "34 12 * * *",
			},
		},
	}}
	connector, err := NewFromSource(source, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("first poll envelopes = %#v", envelopes)
	}
	if envelopes[0].ID != "cron:daily-note:2026-03-09T12:34:00+00:00" {
		t.Fatalf("ID = %q", envelopes[0].ID)
	}

	envelopes, err = connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 0 {
		t.Fatalf("second poll should not refire: %#v", envelopes)
	}
}

func TestPollFromSourceStopsRemovedSchedule(t *testing.T) {
	now := mustTime(t, "2026-03-09T12:34:56Z")
	source := &fakeSource{scheduled: []Scheduled{
		{
			ScheduleID: "sched_a",
			Job: Job{
				JobName: "daily-note",
				Content: "Write the daily note",
				Cron:    "34 12 * * *",
			},
		},
	}}
	connector, err := NewFromSource(source, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	if envelopes, err := connector.Poll(context.Background()); err != nil || len(envelopes) != 1 {
		t.Fatalf("first poll envelopes = %#v err = %v", envelopes, err)
	}

	// Remove the schedule and advance to the next due window; it must not fire.
	source.scheduled = nil
	now = mustTime(t, "2026-03-10T12:34:56Z")
	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 0 {
		t.Fatalf("removed schedule should not fire: %#v", envelopes)
	}
	if _, ok := connector.states["sched_a"]; ok {
		t.Fatal("state for removed schedule was not pruned")
	}
}

func TestPollFromSourceRecomputesOnCronChange(t *testing.T) {
	now := mustTime(t, "2026-03-09T11:00:00Z")
	job := Scheduled{
		ScheduleID: "sched_a",
		Job: Job{
			JobName: "daily-note",
			Content: "Write the daily note",
			Cron:    "0 12 * * *",
		},
	}
	source := &fakeSource{scheduled: []Scheduled{job}}
	connector, err := NewFromSource(source, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}

	if envelopes, err := connector.Poll(context.Background()); err != nil || len(envelopes) != 0 {
		t.Fatalf("initial poll should not fire: %#v err = %v", envelopes, err)
	}

	// Change the cron so the next run moves from 12:00 to 13:00.
	job.Job.Cron = "0 13 * * *"
	source.scheduled = []Scheduled{job}

	now = mustTime(t, "2026-03-09T12:00:00Z")
	if envelopes, err := connector.Poll(context.Background()); err != nil || len(envelopes) != 0 {
		t.Fatalf("must not fire at the old cron time after a cron change: %#v err = %v", envelopes, err)
	}

	now = mustTime(t, "2026-03-09T13:00:00Z")
	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("should fire at the new cron time: %#v", envelopes)
	}
	if envelopes[0].ID != "cron:daily-note:2026-03-09T13:00:00+00:00" {
		t.Fatalf("ID = %q", envelopes[0].ID)
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
