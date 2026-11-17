package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/personalinbox"
	state "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

type richSchedule struct {
	name, content, cron, timezone string
	edits                         []string
}

// Job names match the worker fixture's --rich cron runs.
var richSchedules = []richSchedule{
	{"Morning briefing", "", "", "", []string{"0 8 * * 1-5"}},
	{"Weekly repository review", "", "", "", []string{"0 17 * * 5", "0 16 * * 5"}},
	{"Evening inbox digest", "", "", "", []string{"30 18 * * *"}},
	{"Sentry triage", "Triage new Sentry issues since the last run and flag regressions.", "30 */6 * * *", "UTC", []string{"30 */4 * * *", "30 */6 * * *"}},
	{"Backup verification", "Verify last night’s SQLite backups restore cleanly and report sizes.", "15 3 * * *", "UTC", []string{"15 3 * * *"}},
	{"CI flake report", "List CI jobs that failed then passed on retry in the last six hours.", "0 */6 * * *", "UTC", []string{"0 */6 * * *"}},
	{"Dependency audit", "Check Go, Python and npm dependencies for security advisories.", "0 6 * * 5", "Europe/London", []string{"0 6 * * 1", "0 6 * * 5"}},
	{"Release notes draft", "Draft release notes from merged PRs since the last tag.", "0 15 * * 4", "America/New_York", []string{"0 14 * * 4"}},
	{"On-call handover", "Summarise open incidents and alerts for the incoming on-call engineer.", "0 9 * * 1", "Europe/London", []string{"0 9 * * 1", "0 10 * * 1"}},
	{"PR review nudge", "List PRs waiting on my review for more than a day.", "0 10,14 * * 1-5", "Europe/London", []string{"0 10 * * 1-5"}},
	{"Monthly cost report", "Summarise last month’s Hetzner, AWS and API spend against budget.", "0 7 1 * *", "UTC", []string{"0 7 1 * *"}},
	{"Calendar prep", "Look at tomorrow’s calendar and prepare notes for each meeting.", "45 19 * * 0-4", "Europe/London", []string{"0 20 * * 0-4", "45 19 * * 0-4"}},
	{"Standup notes", "Collect yesterday’s commits and today’s plan into standup notes.", "55 9 * * 1-5", "Europe/Berlin", []string{"50 9 * * 1-5"}},
	{"Newsletter roundup", "Pick the five best newsletter articles of the week with one-line summaries.", "0 12 * * 6", "Europe/London", []string{"0 11 * * 6"}},
	{"Quarterly OKR check", "Compare progress against this quarter’s OKRs and flag anything off track.", "0 9 1 1,4,7,10 *", "Asia/Tokyo", []string{"0 9 1 1,4,7,10 *", "30 9 1 1,4,7,10 *"}},
}

// Fired reminders (12–23) match the worker fixture's reminder runs by id and
// revision, and fire at the same times.
var richReminderPrompts = []string{
	"Check the staging deploy finished", "Reply to the accountant", "Review the platform redesign screenshots",
	"Water the plants", "Renew the domain for example.dev", "Pay the electricity bill",
	"Check whether CI is green on main", "Call the bank about the card", "Prepare the Friday review agenda",
	"Read the JMAP rate limit reply", "Back up the laptop", "Order printer ink",
	"Chase the Hetzner invoice query", "Renew the staging TLS certificate", "Check whether the reminder backfill finished",
	"Send Sam the Friday review notes", "Rotate the Telegram bot token", "Look at the egress drop metrics again",
	"Book the dentist", "Review the open dependabot PRs", "Confirm the worker disk cleanup ran",
	"Prepare the quarterly cost summary", "Follow up on the JMAP rate limit ticket", "Archive last month’s inbox digests",
	"Cancel the unused SaaS trial", "Ask about the conference ticket", "Move the 1:1 to Thursday",
	"Check the flaky race test again", "Pick up the parcel", "Update the on-call rota",
}

func reminderFireTime(now time.Time, index int) time.Time {
	day := now.UTC().AddDate(0, 0, -(12 - index))
	return time.Date(day.Year(), day.Month(), day.Day(), 9+index%8, 0, 0, 0, time.UTC)
}

func ts(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func seedRich(ctx context.Context, store *state.Store, handlerPath, inboxPath string) error {
	db, err := sql.Open("sqlite", handlerPath)
	if err != nil {
		return err
	}
	defer db.Close()
	now := time.Now().UTC()
	rng := rand.New(rand.NewSource(398))
	if err := seedRichSchedules(ctx, store, db, now); err != nil {
		return fmt.Errorf("schedules: %w", err)
	}
	if err := seedRichReminders(ctx, store, db, now, rng); err != nil {
		return fmt.Errorf("reminders: %w", err)
	}
	if err := seedRichInbox(ctx, inboxPath, now, rng); err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	return nil
}

func seedRichSchedules(ctx context.Context, store *state.Store, db *sql.DB, now time.Time) error {
	active, err := store.ListActiveSchedules(ctx)
	if err != nil {
		return err
	}
	byName := map[string]state.ScheduleRecord{}
	for _, record := range active {
		byName[record.JobName] = record
	}
	for index, job := range richSchedules {
		input := state.ScheduleInput{JobName: job.name, Content: job.content, Cron: job.cron, Timezone: job.timezone, ReplyChannelType: "telegram", ReplyChannelTarget: "-1002233445566", CreatedBy: "preview", ContextRefs: []string{"repo:/workspace/chatting"}, PromptContext: []string{}}
		record, existing := byName[job.name]
		if !existing {
			if record, err = store.CreateSchedule(ctx, input); err != nil {
				return err
			}
		}
		final := state.ScheduleInput{JobName: record.JobName, Content: record.Content, Cron: record.Cron, Timezone: record.Timezone, ReplyChannelType: record.ReplyChannelType, ReplyChannelTarget: record.ReplyChannelTarget, CreatedBy: record.CreatedBy, ContextRefs: record.ContextRefs, PromptContext: record.PromptContext}
		// Rewrite history so the seeded edits lead up to the current definition.
		versions := append(append([]string{}, job.edits...), final.Cron)
		first := final
		first.Cron = versions[0]
		first.Content = "Draft: " + final.Content
		if _, err := db.ExecContext(ctx, "UPDATE schedules SET cron=?, content=? WHERE schedule_id=? AND version=1", first.Cron, first.Content, record.ScheduleID); err != nil {
			return err
		}
		for version, cron := range versions[1:] {
			next := final
			next.Cron = cron
			if version < len(versions)-2 {
				next.Content = "Draft: " + final.Content
			}
			if _, err := store.ReplaceSchedule(ctx, record.ScheduleID, next); err != nil {
				return err
			}
		}
		created := now.Add(-time.Duration(40+index*3) * 24 * time.Hour)
		for version := 1; version <= len(versions); version++ {
			at := created.Add(time.Duration(version-1) * time.Duration(9+index) * 24 * time.Hour)
			var superseded any
			if version < len(versions) {
				superseded = ts(created.Add(time.Duration(version) * time.Duration(9+index) * 24 * time.Hour))
			}
			if _, err := db.ExecContext(ctx, "UPDATE schedules SET created_at=?, superseded_at=? WHERE schedule_id=? AND version=?", ts(at), superseded, record.ScheduleID, version); err != nil {
				return err
			}
		}
	}
	dead, err := store.CreateSchedule(ctx, state.ScheduleInput{JobName: "Legacy uptime check", Content: "Ping the old status page and report if it is down.", Cron: "*/5 * * * *", Timezone: "UTC", ReplyChannelType: "telegram", ReplyChannelTarget: "-1002233445566", CreatedBy: "preview", ContextRefs: []string{}, PromptContext: []string{}})
	if err != nil {
		return err
	}
	_, err = store.MarkScheduleDead(ctx, dead.ScheduleID)
	return err
}

func seedRichReminders(ctx context.Context, store *state.Store, db *sql.DB, now time.Time, rng *rand.Rand) error {
	revisions := map[int]int{2: 2, 5: 2, 7: 3, 13: 2, 16: 3, 19: 2, 25: 2}
	for index, prompt := range richReminderPrompts {
		var runAt time.Time
		switch {
		case index == 0:
			runAt = now.Add(-25 * time.Minute).Truncate(time.Minute)
		case index == 1:
			runAt = now.Add(-3*time.Hour - 10*time.Minute).Truncate(time.Minute)
		case index < 12:
			runAt = now.Add(time.Duration(index*index) * 2 * time.Hour).Truncate(time.Hour)
		case index < 24:
			runAt = reminderFireTime(now, index-12)
		default:
			runAt = now.Add(time.Duration(index-26) * 36 * time.Hour).Truncate(time.Hour)
		}
		id := fmt.Sprintf("rem_rich%02d", index)
		count := revisions[index]
		if count == 0 {
			count = 1
		}
		input := state.ReminderInput{RunAt: runAt.Add(-time.Duration(count-1) * time.Hour), Prompt: prompt, ReplyChannelType: "telegram", ReplyChannelTarget: "-1002233445566", ReplyChannelMetadata: map[string]any{"message_thread_id": 90 + index%4}, ContextRefs: []string{}, PromptContext: []string{}, CreatedBy: "telegram", CreatedFromTaskID: fmt.Sprintf("task:telegram:item_rich_im_%02d:0", index%22), IdempotencyKey: id + ":1"}
		record, _, err := store.CreateReminder(ctx, input)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, "UPDATE reminders SET reminder_id=? WHERE reminder_id=?", id, record.ReminderID); err != nil {
			return err
		}
		for revision := 2; revision <= count; revision++ {
			input.RunAt = runAt.Add(-time.Duration(count-revision) * time.Hour)
			input.IdempotencyKey = fmt.Sprintf("%s:%d", id, revision)
			if revision == count && index%2 == 0 {
				input.Prompt = prompt + " (moved)"
			}
			if _, _, err := store.ReplaceReminder(ctx, id, input); err != nil {
				return err
			}
		}
		step := 5 * time.Hour
		base := runAt.Add(-time.Duration(48+rng.Intn(72)) * time.Hour)
		if latest := now.Add(-time.Duration(count)*step - time.Duration(1+index)*time.Hour); base.After(latest) {
			base = latest
		}
		for revision := 1; revision <= count; revision++ {
			created := base.Add(time.Duration(revision-1) * step)
			if revision < count {
				replaced := ts(created.Add(step))
				_, err = db.ExecContext(ctx, "UPDATE reminders SET created_at=?, updated_at=?, cancelled_at=? WHERE reminder_id=? AND revision=?", ts(created), replaced, replaced, id, revision)
			} else {
				_, err = db.ExecContext(ctx, "UPDATE reminders SET created_at=?, updated_at=? WHERE reminder_id=? AND revision=?", ts(created), ts(created), id, revision)
			}
			if err != nil {
				return err
			}
		}
		switch {
		case index >= 12 && index < 24:
			if _, err := store.MarkReminderFired(ctx, id, count, runAt.Add(2*time.Second)); err != nil {
				return err
			}
		case index >= 24:
			if _, err := store.CancelReminder(ctx, id); err != nil {
				return err
			}
			cancelled := base.Add(time.Duration(count-1)*step + time.Hour)
			if _, err := db.ExecContext(ctx, "UPDATE reminders SET updated_at=?, cancelled_at=? WHERE reminder_id=? AND revision=?", ts(cancelled), ts(cancelled), id, count); err != nil {
				return err
			}
		}
	}
	return nil
}

type richMail struct{ sender, subject, category, reason string }

var richMails = []richMail{
	{"Alex · Example Studio", "Feedback needed on the v0.14 release notes", "action", "Direct request for feedback with a deadline."},
	{"GitHub", "[chatting] PR #405 review requested", "action", "Review requested on a repository you own."},
	{"HMRC", "Your Self Assessment tax return is due", "action", "Payment deadline in the body."},
	{"Hetzner Online", "Invoice R0021938217", "action", "Invoice that needs checking against usage."},
	{"Sam · Example Team", "Can we move Friday’s review?", "action", "A question that needs a reply."},
	{"Unknown sender", "Urgent: verify your account details", "review", "Possible phishing; held for review."},
	{"Recruiter · TalentCo", "Exciting opportunity", "review", "Low confidence between news and action."},
	{"Project updates", "Unusual access pattern detected", "review", "Security notice from an unfamiliar service."},
	{"Example Store", "Your order is on its way", "order", "Order confirmation with delivery details."},
	{"Royal Mail", "Your parcel will arrive tomorrow", "order", "Delivery notification."},
	{"Amazon.co.uk", "Your Amazon.co.uk order #204-1182733", "order", "Order receipt."},
	{"Uber Receipts", "Your Thursday evening trip", "order", "Trip receipt."},
	{"The Pragmatic Engineer", "This week in engineering", "news", "Newsletter with no action required."},
	{"Hacker Newsletter", "#712: the best of HN this week", "news", "Newsletter digest."},
	{"Go Weekly", "Go 1.25.4 is released", "news", "Release newsletter."},
	{"Stratechery", "The AI capex question", "news", "Long-form newsletter."},
	{"Sentry", "Weekly report for chatting", "news", "Automated weekly report."},
	{"Monzo", "Your monthly spending summary", "news", "Account summary with no action."},
	{"LWN.net", "LWN.net Weekly Edition", "news", "Subscription newsletter."},
}

func seedRichInbox(ctx context.Context, inboxPath string, now time.Time, rng *rand.Rand) error {
	db, err := sql.Open("sqlite", inboxPath)
	if err != nil {
		return err
	}
	defer db.Close()
	weights := []string{"news", "news", "news", "news", "news", "order", "order", "action", "action", "review"}
	byCategory := map[string][]richMail{}
	for _, mail := range richMails {
		byCategory[mail.category] = append(byCategory[mail.category], mail)
	}
	categories := []string{"action", "review", "order", "news"}
	for index := range 120 {
		created := now.Add(-time.Duration(rng.Int63n(int64(5 * 24 * time.Hour))))
		message := personalinbox.Message{ID: fmt.Sprintf("rich-mail-%03d", index), ThreadID: fmt.Sprintf("rich-thread-%03d", index/2), Created: created.Unix(), BodyChars: 800 + rng.Intn(24000)}
		category := weights[rng.Intn(len(weights))]
		pool := byCategory[category]
		mail := pool[rng.Intn(len(pool))]
		message.Mail = personalinbox.Mail{From: mail.sender, Subject: mail.subject, Date: created.Format(time.RFC1123Z)}
		if index%12 == 5 {
			message.Created = now.Add(-time.Duration(5+index) * time.Minute).Unix()
			message.Attempts = index % 5
		} else {
			confidence := 0.8 + rng.Float64()*0.19
			if category == "review" {
				confidence = 0.55 + rng.Float64()*0.2
			}
			probabilities := map[string]float64{}
			rest := 1 - confidence
			for _, other := range categories {
				if other != category {
					probabilities[other] = rest / 3
				}
			}
			probabilities[category] = confidence
			trace := &personalinbox.Trace{Reason: mail.reason, JevCategory: category, JevConfidence: confidence, JevModel: "jev-1.14.0", JevPolicyVersion: "inbox-2026-10", RulesRevision: "r42", CategoryProbabilities: probabilities, Summarised: category == "news", RequiresAction: category == "action", InputBodyChars: message.BodyChars, ExcerptChars: min(message.BodyChars, 6000), ExcerptTruncated: message.BodyChars > 6000}
			final := category
			switch {
			case index%17 == 3:
				final, trace.Reason = "review", "Read as action but the sender is new; held for review."
			case index%23 == 7:
				final = "review"
				trace.TriageFailed, trace.TriageError, trace.TriageHTTPStatus = true, "jev: 503 Service Unavailable", 503
				trace.Reason, trace.JevCategory, trace.JevConfidence, trace.CategoryProbabilities = "Triage failed; held for review.", "", 0, nil
				confidence = 0.55
			}
			message.Classification = &personalinbox.Classification{Category: final, Confidence: confidence, Summary: mail.reason, Trace: trace}
			message.Published = created.Add(time.Duration(5+rng.Intn(90)) * time.Second).Unix()
		}
		raw, err := json.Marshal(message)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, "INSERT OR REPLACE INTO inbox_messages(id,data) VALUES (?,?)", message.ID, string(raw)); err != nil {
			return err
		}
	}
	stamp := now.Add(-40 * time.Second).Format(time.RFC3339)
	meta := map[string]string{
		"dead_letters":         "2",
		"last_error":           "jmap: Email/get: 429 Too Many Requests (retry after 30s)",
		"step_success:results": stamp,
		"step_success:ingest":  stamp,
		"step_success:digest":  stamp,
		"step_success:perform": stamp,
		"step_success:publish": now.Add(-47 * time.Minute).Format(time.RFC3339),
		"step_error:results":   "",
		"step_error:ingest":    "",
		"step_error:digest":    "",
		"step_error:perform":   "",
		"step_error:publish":   "personal inbox publish failed: broker unavailable",
	}
	for key, value := range meta {
		if _, err := db.ExecContext(ctx, "INSERT INTO inbox_meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value); err != nil {
			return err
		}
	}
	for index, effect := range []personalinbox.Effect{
		{Kind: "file", Category: "news", Target: "Newsletters", Status: "uncertain", Note: "move timed out; mailbox state unknown"},
		{Kind: "telegram", Target: "-1002233445566", Body: "Inbox digest: 4 newsletters, 1 order.", Status: "pending"},
		{Kind: "file", Category: "order", Target: "Orders", Status: "inflight"},
		{Kind: "file", Category: "news", Target: "Newsletters", Status: "done", Receipt: "moved"},
	} {
		effect.ID = fmt.Sprintf("rich-mail-%03d:%s", index, effect.Kind)
		effect.MessageID = fmt.Sprintf("rich-mail-%03d", index)
		raw, err := json.Marshal(effect)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, "INSERT OR REPLACE INTO inbox_effects(id,data) VALUES (?,?)", effect.ID, string(raw)); err != nil {
			return err
		}
	}
	return nil
}

// seedRichMetrics feeds the real recorder counts resembling three days of uptime.
func seedRichMetrics(recorder *metrics.Recorder, now time.Time) {
	for range 52_000 {
		recorder.RecordLoop(0, now)
	}
	recorder.RecordLoop(412, now)
	for status, count := range map[string]int{"dispatched": 380, "completed": 190, "deduped": 31} {
		for range count {
			recorder.RecordEgressResult(status, "")
		}
	}
	for reason, count := range map[string]int{"unknown_task": 3, "completed_task": 2, "disallowed_channel": 1} {
		for range count {
			recorder.RecordEgressResult("dropped", reason)
		}
	}
	for index := range 30 {
		recorder.RecordReminderCreated(false)
		if index < 6 {
			recorder.RecordReminderCancelled()
		}
		if index < 14 {
			recorder.RecordReminderDue(index < 3, false)
		}
		if index < 2 {
			recorder.RecordReminderDue(false, true)
		}
		if index < 12 {
			recorder.RecordReminderFired()
		}
	}
}
