// Command testserver runs real handler APIs against disposable local fixtures.
// No broker, connector, executor, or outbound delivery is started.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/metrics"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/personalinbox"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/reminders"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/schedules"
	state "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func main() {
	dir := flag.String("state-dir", "", "Disposable fixture directory (required)")
	port := flag.Int("port", 19464, "Loopback fixture port")
	empty := flag.Bool("empty", false, "Start with empty fixtures")
	noInbox := flag.Bool("no-inbox", false, "Omit the optional personal inbox endpoint")
	telegramOutage := flag.Bool("telegram-outage", false, "Simulate Telegram API 502 responses and a dropped reply")
	rich := flag.Bool("rich", false, "Also seed many schedules, reminders, inbox decisions and handler metrics")
	flag.Parse()
	if *dir == "" {
		log.Fatal("--state-dir is required")
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	store, err := state.Open(ctx, filepath.Join(*dir, "handler.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	inboxPath := filepath.Join(*dir, "inbox.db")
	inbox, err := personalinbox.Open(inboxPath)
	if err != nil {
		log.Fatal(err)
	}
	defer inbox.Close()
	if !*empty {
		existing, err := store.ListActiveSchedules(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if len(existing) == 0 {
			for _, job := range []struct{ name, content, cron string }{
				{"Morning briefing", "Bring together today’s priorities, open pull requests, and anything that needs attention.", "0 8 * * 1-5"},
				{"Weekly repository review", "Review the week’s changes and prepare a short summary of progress and outstanding work.", "0 16 * * 5"},
				{"Evening inbox digest", "Summarise today’s newsletters and share the highlights worth reading.", "30 18 * * *"},
			} {
				if _, err := store.CreateSchedule(ctx, state.ScheduleInput{JobName: job.name, Content: job.content, Cron: job.cron, Timezone: "Europe/London", ReplyChannelType: "telegram", ReplyChannelTarget: "fixture-chat", CreatedBy: "preview", ContextRefs: []string{"repo:/workspace/chatting"}, PromptContext: []string{}}); err != nil {
					log.Fatal(err)
				}
			}
		}
		existingReminders, err := store.ListLatestReminders(ctx, "all")
		if err != nil {
			log.Fatal(err)
		}
		if len(existingReminders) == 0 {
			for i, prompt := range []string{"Review the platform interface", "Follow up on the repository review"} {
				if _, _, err := store.CreateReminder(ctx, state.ReminderInput{RunAt: time.Now().UTC().Add(time.Duration(i+1) * 24 * time.Hour).Truncate(time.Minute), Prompt: prompt, ReplyChannelType: "telegram", ReplyChannelTarget: "fixture-chat", ReplyChannelMetadata: map[string]any{"thread_id": "42"}, ContextRefs: []string{}, PromptContext: []string{}, CreatedBy: "preview", CreatedFromTaskID: "fixture", IdempotencyKey: fmt.Sprintf("fixture:%d", i)}); err != nil {
					log.Fatal(err)
				}
			}
		}
		db, err := sql.Open("sqlite", inboxPath)
		if err != nil {
			log.Fatal(err)
		}
		for i, row := range []struct{ sender, subject, category, reason string }{
			{"Alex · Example Studio", "A few thoughts on the next release", "action", "A direct request for feedback before the next release."},
			{"The Pragmatic Engineer", "This week in engineering", "news", "A newsletter with no action required."},
			{"Example Store", "Your order is on its way", "order", "An order confirmation with delivery information."},
			{"Project updates", "An unusual request", "review", "Low confidence; held for the owner to review."},
			{"Sam · Example Team", "A quick question about Friday", "", ""},
		} {
			message := personalinbox.Message{ID: fmt.Sprintf("fixture-mail-%d", i), Created: time.Now().Add(-time.Duration(i+1) * 15 * time.Minute).Unix(), Mail: personalinbox.Mail{From: row.sender, Subject: row.subject}, ThreadID: fmt.Sprintf("fixture-thread-%d", i)}
			if row.category != "" {
				message.Classification = &personalinbox.Classification{Category: row.category, Confidence: .97, Trace: &personalinbox.Trace{Reason: row.reason}}
			}
			raw, _ := json.Marshal(message)
			if _, err := db.Exec("INSERT OR REPLACE INTO inbox_messages(id,data) VALUES (?,?)", message.ID, string(raw)); err != nil {
				log.Fatal(err)
			}
		}
		_ = db.Close()
		if *rich {
			if err := seedRich(ctx, store, filepath.Join(*dir, "handler.db"), inboxPath); err != nil {
				log.Fatal(err)
			}
		}
	}
	// Mirrors the real handler, whose metrics listener also serves the
	// schedule, reminder and inbox routes.
	startedAt := time.Now()
	if *rich && !*empty {
		startedAt = startedAt.Add(-76 * time.Hour)
	}
	recorder := metrics.New(startedAt, nil)
	if *telegramOutage {
		recorder.EnableTelegram()
		recorder.RecordTelegramPoll(errors.New("telegram_http_status=502"), 502)
		recorder.RecordEgressResult("dropped", "telegram_send_failed")
	}
	recorder.SetSQLitePath(filepath.Join(*dir, "handler.db"))
	if *rich && !*empty {
		seedRichMetrics(recorder, time.Now())
	}
	go func() {
		for now := range time.Tick(5 * time.Second) {
			recorder.RecordLoop(0, now)
		}
	}()
	register := func(mux *http.ServeMux) {
		scheduleService := schedules.NewService(store)
		schedules.RegisterRoutes(mux, scheduleService)
		schedules.RegisterUIRoutes(mux, scheduleService)
		reminderService := reminders.NewService(store)
		reminders.RegisterRoutes(mux, reminderService)
		reminders.RegisterUIRoutes(mux, reminderService)
		if !*noInbox {
			personalinbox.RegisterUIRoutes(mux, &personalinbox.Service{Store: inbox, Now: time.Now, Config: personalinbox.Config{DryRun: true, ConfidenceThreshold: .9, LinkTemplate: "https://example.com/mail/{email}", Folders: map[string]string{}}})
		}
	}
	if _, err := metrics.StartServer(recorder, "127.0.0.1", *port, register); err != nil {
		log.Fatal(err)
	}
	log.Printf("Fixture handler: http://127.0.0.1:%d", *port)
	select {}
}
