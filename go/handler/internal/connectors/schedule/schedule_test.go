package schedule

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestLoadJobsJSONAcceptsPythonSupportedKeys(t *testing.T) {
	jobs, err := LoadJobsJSON([]byte(`[
		{
			"job_name": "daily-note",
			"content": "Write the daily note",
			"cron": "5 7 * * *",
			"timezone": "Europe/London",
			"context_refs": ["repo:/workspace/chatting"],
			"prompt_context": ["Keep it terse."],
			"reply_channel_type": "telegram",
			"reply_channel_target": "8605042448"
		}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs = %#v", jobs)
	}
	job := jobs[0]
	if job.JobName != "daily-note" {
		t.Fatalf("JobName = %q", job.JobName)
	}
	if job.Content != "Write the daily note" {
		t.Fatalf("Content = %q", job.Content)
	}
	if job.TimezoneName != "Europe/London" {
		t.Fatalf("TimezoneName = %q", job.TimezoneName)
	}
	if got := strings.Join(job.ContextRefs, ","); got != "repo:/workspace/chatting" {
		t.Fatalf("ContextRefs = %#v", job.ContextRefs)
	}
	if got := strings.Join(job.PromptContext, ","); got != "Keep it terse." {
		t.Fatalf("PromptContext = %#v", job.PromptContext)
	}
	if job.ReplyChannelType != "telegram" || job.ReplyChannelTarget != "8605042448" {
		t.Fatalf("reply channel = %#v", job)
	}
}

func TestLoadJobsJSONRejectsUnknownKeys(t *testing.T) {
	_, err := LoadJobsJSON([]byte(`[
		{
			"job_name": "daily-note",
			"content": "Write the daily note",
			"cron": "5 7 * * *",
			"surprise": true
		}
	]`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "schedule job at index 0 contains unknown keys: surprise") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadJobsJSONRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		message string
	}{
		{
			name:    "missing content",
			raw:     `[{"job_name": "daily-note", "cron": "5 7 * * *"}]`,
			message: "schedule job at index 0 is missing required keys: content",
		},
		{
			name:    "blank cron",
			raw:     `[{"job_name": "daily-note", "content": "Write", "cron": "  "}]`,
			message: "schedule job at index 0 cron must be a non-empty string",
		},
		{
			name:    "six field cron",
			raw:     `[{"job_name": "daily-note", "content": "Write", "cron": "0 5 7 * * *"}]`,
			message: "schedule job at index 0 cron must contain exactly 5 fields",
		},
		{
			name:    "invalid timezone",
			raw:     `[{"job_name": "daily-note", "content": "Write", "cron": "5 7 * * *", "timezone": "Mars/Base"}]`,
			message: "schedule job at index 0 invalid timezone: Mars/Base",
		},
		{
			name:    "bad context refs",
			raw:     `[{"job_name": "daily-note", "content": "Write", "cron": "5 7 * * *", "context_refs": [""]}]`,
			message: "schedule job at index 0 context_refs must be a list of non-empty strings",
		},
		{
			name:    "partial reply channel",
			raw:     `[{"job_name": "daily-note", "content": "Write", "cron": "5 7 * * *", "reply_channel_type": "log"}]`,
			message: "schedule job at index 0 reply_channel_type and reply_channel_target must be provided together",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadJobsJSON([]byte(tt.raw))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("error = %v, want %q", err, tt.message)
			}
		})
	}
}

func TestPollEmitsDueJobWithPromptContextAndRefs(t *testing.T) {
	now := mustTime(t, "2026-03-09T12:34:56Z")
	connector, err := New(
		[]Job{
			{
				JobName:       "daily-note",
				Content:       "Write the daily note",
				Cron:          "34 12 * * *",
				ContextRefs:   []string{"repo:/workspace/chatting"},
				PromptContext: []string{"Keep it terse."},
			},
		},
		[]string{"Global guidance."},
		[]string{"Scheduled task guidance."},
		func() time.Time { return now },
	)
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
	if got := strings.Join(envelope.PromptContext.GlobalInstructions, ","); got != "Global guidance." {
		t.Fatalf("GlobalInstructions = %#v", envelope.PromptContext.GlobalInstructions)
	}
	if got := strings.Join(envelope.PromptContext.SourceInstructions, ","); got != "Scheduled task guidance." {
		t.Fatalf("SourceInstructions = %#v", envelope.PromptContext.SourceInstructions)
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
	connector, err := New(
		[]Job{
			{
				JobName:            "morning",
				Content:            "Morning note",
				Cron:               "5 7 * * *",
				TimezoneName:       "Europe/London",
				ReplyChannelType:   "telegram",
				ReplyChannelTarget: "8605042448",
			},
		},
		nil,
		nil,
		func() time.Time { return mustTime(t, "2026-07-01T06:05:30Z") },
	)
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

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
