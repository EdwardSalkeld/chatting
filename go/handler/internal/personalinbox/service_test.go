package personalinbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/jmap"
)

const inboxID = "P-F"

type fakeBroker struct {
	queues       map[string][]*bbmb.PickedMessage
	publications map[string]int
	failPublish  bool
	failAck      bool
}

func (broker *fakeBroker) EnsureQueue(context.Context, string) error { return nil }
func (broker *fakeBroker) PublishJSON(_ context.Context, queue string, payload map[string]any) (string, error) {
	if broker.failPublish {
		return "", errors.New("offline")
	}
	broker.publications[queue]++
	broker.queues[queue] = append(broker.queues[queue], &bbmb.PickedMessage{GUID: fmt.Sprint(broker.publications[queue]), Payload: payload})
	return "guid", nil
}
func (broker *fakeBroker) PickupJSON(_ context.Context, queue string, _, _ int) (*bbmb.PickedMessage, error) {
	if len(broker.queues[queue]) == 0 {
		return nil, nil
	}
	return broker.queues[queue][0], nil
}
func (broker *fakeBroker) Ack(_ context.Context, queue, guid string) error {
	if broker.failAck {
		return errors.New("ack failed")
	}
	broker.queues[queue] = broker.queues[queue][1:]
	return nil
}
func (broker *fakeBroker) result(id string, classification Classification) {
	raw, _ := json.Marshal(map[string]any{"kind": "personal_inbox.result.v1", "message_id": id, "classification": classification})
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	_, _ = broker.PublishJSON(context.Background(), ResultsQueue, payload)
}

type filing struct {
	Email    string
	Mailbox  string
	MarkRead bool
}

type fakeMail struct {
	boxes    []jmap.Mailbox
	emails   map[string]jmap.Email
	state    string
	created  []string
	stale    bool
	failGet  bool
	failFile bool
	filed    []filing
}

func (mail *fakeMail) AccountID(context.Context) (string, error) { return "ub8884168", nil }
func (mail *fakeMail) Mailboxes(context.Context) ([]jmap.Mailbox, error) {
	return mail.boxes, nil
}
func (mail *fakeMail) EmailState(context.Context) (string, error) { return mail.state, nil }
func (mail *fakeMail) Changes(_ context.Context, since string, _ int) (jmap.Changes, error) {
	if mail.stale {
		return jmap.Changes{}, jmap.ErrCannotCalculateChanges
	}
	if since == mail.state {
		return jmap.Changes{OldState: since, NewState: mail.state, Created: nil}, nil
	}
	return jmap.Changes{OldState: since, NewState: mail.state, Created: mail.created}, nil
}
func (mail *fakeMail) InMailbox(_ context.Context, box string, _ int) ([]string, error) {
	ids := []string{}
	for id, email := range mail.emails {
		if email.MailboxIDs[box] {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
func (mail *fakeMail) Emails(_ context.Context, ids []string, _ int) ([]jmap.Email, error) {
	if mail.failGet {
		return nil, errors.New("offline")
	}
	out := []jmap.Email{}
	for _, id := range ids {
		if email, ok := mail.emails[id]; ok {
			out = append(out, email)
		}
	}
	return out, nil
}
func (mail *fakeMail) File(_ context.Context, id, box string, markRead bool) error {
	if mail.failFile {
		return errors.New("refused")
	}
	mail.filed = append(mail.filed, filing{Email: id, Mailbox: box, MarkRead: markRead})
	email := mail.emails[id]
	email.MailboxIDs = map[string]bool{box: true}
	mail.emails[id] = email
	return nil
}

type fakePerformer struct {
	calls    []Effect
	failKind string
}

func (performer *fakePerformer) Perform(_ context.Context, effect Effect) (string, error) {
	performer.calls = append(performer.calls, effect)
	if effect.Kind == performer.failKind {
		return "", errors.New("timeout after acceptance")
	}
	return "receipt", nil
}

func email(id, thread, from, subject, body string) jmap.Email {
	return jmap.Email{
		ID: id, ThreadID: thread, MailboxIDs: map[string]bool{inboxID: true},
		Subject: subject, From: []jmap.EmailAddress{{Name: from}}, ReceivedAt: "2026-10-15T16:10:24Z",
		TextBody: []jmap.BodyPart{{PartID: "1"}}, BodyValues: map[string]jmap.BodyValue{"1": {Value: body}},
	}
}

func fixture(t *testing.T) (*Service, *fakeMail, *fakeBroker, *fakePerformer) {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	mail := &fakeMail{
		boxes: []jmap.Mailbox{
			{ID: inboxID, Name: "Inbox", Role: "inbox"},
			{ID: "T1", Name: "Triage"},
			{ID: "T2", Name: "News", ParentID: "T1"},
			{ID: "T3", Name: "Orders", ParentID: "T1"},
			{ID: "T4", Name: "Actions", ParentID: "T1"},
		},
		emails:  map[string]jmap.Email{"Stm4": email("Stm4", "AUjt", "Echelon Coffee", "Your order", "Thanks for your order.")},
		state:   "J1",
		created: []string{"Stm4"},
	}
	broker := &fakeBroker{queues: map[string][]*bbmb.PickedMessage{}, publications: map[string]int{}}
	performer := &fakePerformer{}
	config := Config{Account: "main", Backfill: true, OrdersTarget: "orders", ActionTarget: "actions",
		DigestTarget: "digest", DigestAt: "08:00", Timezone: "UTC", LinearTeam: "team", LinkTemplate: DefaultLinkTemplate,
		Folders:  map[string]string{"news": "Triage/News", "order": "Triage/Orders", "action": "Triage/Actions"},
		MarkRead: []string{"news"}}
	service := &Service{Config: config, Store: store, Broker: broker, Mail: mail,
		Folders: &Resolver{Mail: mail}, Performer: performer,
		Now: func() time.Time { return time.Unix(2000000000, 0) }, account: "ub8884168"}
	return service, mail, broker, performer
}

func oneMessage(t *testing.T, service *Service) Message {
	t.Helper()
	messages, err := service.Store.messages(context.Background(), "1=1")
	if err != nil || len(messages) != 1 {
		t.Fatalf("messages %v err %v", messages, err)
	}
	return messages[0]
}

func TestBackfillIngestsInboxAndRecordsState(t *testing.T) {
	service, mail, _, _ := fixture(t)
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	if message.ID != "Stm4" || message.ThreadID != "AUjt" || message.Mail.Subject != "Your order" {
		t.Fatalf("%+v", message)
	}
	state, _ := service.Store.getMeta(context.Background(), "email_state")
	if state != mail.state {
		t.Fatalf("state %q", state)
	}
}

func TestBaselineWithoutBackfillIngestsNothing(t *testing.T) {
	service, _, _, _ := fixture(t)
	service.Config.Backfill = false
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	messages, _ := service.Store.messages(context.Background(), "1=1")
	if len(messages) != 0 {
		t.Fatalf("baseline ingested %d messages", len(messages))
	}
	state, _ := service.Store.getMeta(context.Background(), "email_state")
	if state == "" {
		t.Fatal("baseline recorded no state")
	}
}

func TestReplayedChangesNeverOverwriteAClassification(t *testing.T) {
	service, mail, broker, _ := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "order", Summary: "Order confirmed", Confidence: .99})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	// The same id arrives again on the change feed after a state reset.
	mail.state = "J2"
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	if message.Classification == nil || message.Classification.Category != "order" {
		t.Fatalf("replay wiped the classification: %+v", message.Classification)
	}
}

func TestExpiredStateRebaselinesInsteadOfReplayingTheMailbox(t *testing.T) {
	service, mail, _, _ := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := service.Store.messages(ctx, "1=1")
	mail.stale = true
	mail.state = "J9"
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := service.Store.messages(ctx, "1=1")
	if len(after) != len(before) {
		t.Fatalf("rebaseline ingested %d extra messages", len(after)-len(before))
	}
	state, _ := service.Store.getMeta(ctx, "email_state")
	if state != "J9" {
		t.Fatalf("state not re-baselined: %q", state)
	}
	if stamp, _ := service.Store.getMeta(ctx, "state_reset_at"); stamp == "" {
		t.Fatal("state reset not recorded for the operator")
	}
}

func TestFetchFailureLeavesStateUnadvanced(t *testing.T) {
	service, mail, _, _ := fixture(t)
	ctx := context.Background()
	service.Config.Backfill = false
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	mail.state = "J2"
	mail.failGet = true
	if err := service.ingest(ctx); err == nil {
		t.Fatal("expected the fetch failure to surface")
	}
	state, _ := service.Store.getMeta(ctx, "email_state")
	if state != "J1" {
		t.Fatalf("state advanced past an unfetched message: %q", state)
	}
}

func TestOrderIsNotifiedFiledAndLinked(t *testing.T) {
	service, mail, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "order", Summary: "Order confirmed", Confidence: .99})
	if err := service.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]Effect{}
	for _, call := range performer.calls {
		kinds[call.Kind] = call
	}
	if len(kinds) != 2 || kinds["telegram"].Target != "orders" || kinds["file"].Target != "Triage/Orders" {
		t.Fatalf("effects %+v", performer.calls)
	}
	want := "https://app.fastmail.com/mail/Triage%2FOrders/AUjt.Stm4?u=b8884168"
	if !strings.Contains(kinds["telegram"].Body, want) {
		t.Fatalf("alert missing deep link %q:\n%s", want, kinds["telegram"].Body)
	}
	if kinds["file"].Category != "order" {
		t.Fatalf("file effect lost its category: %+v", kinds["file"])
	}
	_ = mail
}

func TestReviewIsLeftInTheInboxAndNotAlerted(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "review", Summary: "Not sure", Confidence: .99})
	if err := service.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	// Review means the workflow could not classify it, and such mail stays in
	// the inbox, so alerting as well would repeat what the inbox already shows.
	if len(performer.calls) != 0 {
		t.Fatalf("review produced effects: %+v", performer.calls)
	}
}

func TestIncompleteContentDoesNotOverrideJev(t *testing.T) {
	service, mail, broker, _ := fixture(t)
	ctx := context.Background()
	attached := mail.emails["Stm4"]
	attached.HasAttachment = true
	mail.emails["Stm4"] = attached
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	if !oneMessage(t, service).Mail.Incomplete {
		t.Fatal("attachment not surfaced as incomplete")
	}
	broker.result("Stm4", Classification{Category: "news", Summary: "A newsletter", Confidence: .99})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	if got := oneMessage(t, service).Classification.Category; got != "news" {
		t.Fatalf("category %q, want news", got)
	}
}

func TestNewsIsFiledAndNeverAlertedIndividually(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "news", Confidence: .99})
	if err := service.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	for _, call := range performer.calls {
		if call.Kind == "telegram" && call.Target != "digest" {
			t.Fatalf("news alerted outside the digest: %+v", call)
		}
	}
	filed := 0
	for _, call := range performer.calls {
		if call.Kind == "file" && call.Target == "Triage/News" {
			filed++
		}
	}
	if filed != 1 {
		t.Fatalf("news not filed: %+v", performer.calls)
	}
}

func TestRoutineNewsNeedsNoSummary(t *testing.T) {
	// The digest writes the line, so a news classification with no summary is
	// valid; other categories need a summary or a fallback notice.
	if err := (Classification{Category: "news", Confidence: .99}).Validate(); err != nil {
		t.Fatalf("news without a summary rejected: %v", err)
	}
	if err := (Classification{Category: "order", Confidence: .99}).Validate(); err == nil {
		t.Fatal("order without a summary accepted")
	}
}

func TestClearingAFolderStopsPendingFiling(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "order", Summary: "Order confirmed", Confidence: .99})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	service.Config.Folders = map[string]string{}
	if err := service.perform(ctx); err != nil {
		t.Fatal(err)
	}
	for _, call := range performer.calls {
		if call.Kind == "file" {
			t.Fatal("filed into a folder the operator had cleared")
		}
	}
}

func TestDryRunPreviewsAndRejectsInvalidResults(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	service.Config.DryRun = true
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "order", Summary: "Order confirmed", Confidence: .99})
	if err := service.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	effects, _ := service.Store.effects(ctx, "1=1")
	if len(effects) != 2 || len(performer.calls) != 0 {
		t.Fatalf("dry run acted: %d effects, %d calls", len(effects), len(performer.calls))
	}
	for _, effect := range effects {
		if effect.Status != "preview" {
			t.Fatalf("%+v", effect)
		}
	}
	broker.queues[ResultsQueue] = append(broker.queues[ResultsQueue], &bbmb.PickedMessage{GUID: "evil",
		Payload: map[string]any{"kind": "personal_inbox.result.v1", "message_id": "Stm4", "target": "attacker"}})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	if broker.publications[DeadQueue] != 1 {
		t.Fatal("invalid result accepted")
	}
}

func TestUncertainEffectIsHeldAndReportedUnhealthy(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	performer.failKind = "telegram"
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "order", Summary: "Order", Confidence: .99})
	if err := service.Cycle(ctx); err == nil {
		t.Fatal("expected uncertain delivery to surface")
	}
	effects, _ := service.Store.effects(ctx, "json_extract(data,'$.kind')='telegram'")
	if len(effects) != 1 || effects[0].Status != "uncertain" {
		t.Fatalf("%+v", effects)
	}
	health, err := service.Store.stepHealth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if health["perform"]["error"] == "" {
		t.Fatalf("failed delivery reported as healthy: %+v", health["perform"])
	}
}

func TestStepHealthSeparatesIngestFromTheRest(t *testing.T) {
	service, mail, _, _ := fixture(t)
	mail.failGet = true
	mail.state = "J2"
	ctx := context.Background()
	if err := service.Store.putMeta(ctx, "email_state", "J1"); err != nil {
		t.Fatal(err)
	}
	if err := service.Cycle(ctx); err == nil {
		t.Fatal("expected the ingest failure to surface")
	}
	health, err := service.Store.stepHealth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if health["ingest"]["error"] == "" || health["ingest"]["success"] != "" {
		t.Fatalf("ingest health %+v", health["ingest"])
	}
	for _, step := range []string{"results", "publish", "digest", "perform"} {
		if health[step]["success"] == "" || health[step]["error"] != "" {
			t.Fatalf("%s reported unhealthy because ingest failed: %+v", step, health[step])
		}
	}
}

func TestPublishFailureKeepsTheMessageForRetry(t *testing.T) {
	service, _, broker, _ := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.failPublish = true
	if err := service.publish(ctx); err == nil {
		t.Fatal("expected publish failure")
	}
	if oneMessage(t, service).Published != 0 {
		t.Fatal("message marked published despite a failed publish")
	}
	broker.failPublish = false
	if err := service.publish(ctx); err != nil {
		t.Fatal(err)
	}
	if oneMessage(t, service).Published == 0 {
		t.Fatal("retry did not publish")
	}
}

// classifyAsNews ingests the fixture message and marks it routine.
func classifyAsNews(t *testing.T, service *Service, broker *fakeBroker) {
	t.Helper()
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "news", Confidence: .99})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
}

func digestTask(t *testing.T, broker *fakeBroker) map[string]any {
	t.Helper()
	for _, picked := range broker.queues[TasksQueue] {
		if picked.Payload["kind"] == "personal_inbox.digest.v1" {
			return picked.Payload
		}
	}
	t.Fatal("no digest task published")
	return nil
}

func TestDigestFreezesTheDayAndAsksForOneBatch(t *testing.T) {
	service, _, broker, _ := fixture(t)
	ctx := context.Background()
	classifyAsNews(t, service, broker)
	if err := service.digest(ctx); err != nil {
		t.Fatal(err)
	}
	payload := digestTask(t, broker)
	items := payload["items"].([]map[string]any)
	if len(items) != 1 || items[0]["ref"] != "Stm4" {
		t.Fatalf("items %+v", items)
	}
	// The line has to come from the body: marketing subjects say nothing.
	if items[0]["body"] != "Thanks for your order." {
		t.Fatalf("body not sent for summarising: %+v", items[0])
	}
	// Membership is frozen, so a second pass neither re-opens nor re-publishes.
	before := broker.publications[TasksQueue]
	if err := service.digest(ctx); err != nil {
		t.Fatal(err)
	}
	if broker.publications[TasksQueue] != before {
		t.Fatal("digest re-published within its own window")
	}
}

func TestSuspiciousNewsCannotReachTheDigestSummariser(t *testing.T) {
	service, _, broker, _ := fixture(t)
	ctx := context.Background()
	classifyOnce(t, service, broker, Classification{Category: "news", Confidence: .99,
		Summary: "Withheld", Trace: &Trace{Suspicious: true}})
	if err := service.digest(ctx); err != nil {
		t.Fatal(err)
	}
	items := digestTask(t, broker)["items"].([]map[string]any)
	if len(items) != 1 || items[0]["ref"] != "Stm4" || items[0]["from"] != "Withheld email" ||
		items[0]["subject"] != "Summary withheld" || items[0]["body"] != "Automatic summary withheld; read the original email." {
		t.Fatalf("untrusted mail reached digest: %+v", items)
	}
}

func TestLNEROrderWithExtractedTaskUsesOnlyOrderEffects(t *testing.T) {
	service, _, broker, performer := fixture(t)
	classifyOnce(t, service, broker, Classification{Category: "order", Summary: "Paid LNER booking",
		Action: "Find an available seat", Deadline: "Departure 10:00", Confidence: .99})
	if err := service.perform(context.Background()); err != nil {
		t.Fatal(err)
	}
	notifications, filings := 0, 0
	for _, call := range performer.calls {
		switch call.Kind {
		case "linear":
			t.Fatal("order created a Linear ticket")
		case "telegram":
			if call.Target != "orders" {
				t.Fatalf("wrong notification destination: %+v", call)
			}
			notifications++
		case "file":
			if call.Target != "Triage/Orders" {
				t.Fatalf("wrong folder: %+v", call)
			}
			filings++
		}
	}
	if notifications != 1 || filings != 1 {
		t.Fatalf("missing order effects: %+v", performer.calls)
	}
}

func TestDigestSendsOneMessageWithPerSenderLines(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	classifyAsNews(t, service, broker)
	if err := service.digest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.queues[ResultsQueue] = append(broker.queues[ResultsQueue], &bbmb.PickedMessage{GUID: "d1",
		Payload: map[string]any{"kind": "personal_inbox.digest_result.v1",
			"digest_id": digestTask(t, broker)["digest_id"],
			"items":     []any{map[string]any{"ref": "Stm4", "sender": "Echelon Coffee", "line": "order shipped"}}}})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.digest(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.perform(ctx); err != nil {
		t.Fatal(err)
	}
	sent := []Effect{}
	for _, call := range performer.calls {
		if call.Target == "digest" {
			sent = append(sent, call)
		}
	}
	if len(sent) != 1 {
		t.Fatalf("want one digest message, got %d: %+v", len(sent), performer.calls)
	}
	if !strings.Contains(sent[0].Body, "• Echelon Coffee — order shipped") {
		t.Fatalf("digest body:\n%s", sent[0].Body)
	}
}

func TestDigestRejectsLinesThatDoNotMatchItsMembership(t *testing.T) {
	cases := map[string][]any{
		"dropped a message": {},
		"invented a message": {
			map[string]any{"ref": "Stm4", "sender": "Echelon", "line": "ok"},
			map[string]any{"ref": "nope", "sender": "Ghost", "line": "ok"},
		},
		"unknown reference": {map[string]any{"ref": "nope", "sender": "Ghost", "line": "ok"}},
		"empty line":        {map[string]any{"ref": "Stm4", "sender": "Echelon", "line": "  "}},
	}
	for name, items := range cases {
		t.Run(name, func(t *testing.T) {
			service, _, broker, _ := fixture(t)
			ctx := context.Background()
			classifyAsNews(t, service, broker)
			if err := service.digest(ctx); err != nil {
				t.Fatal(err)
			}
			broker.queues[ResultsQueue] = append(broker.queues[ResultsQueue], &bbmb.PickedMessage{GUID: "d1",
				Payload: map[string]any{"kind": "personal_inbox.digest_result.v1",
					"digest_id": digestTask(t, broker)["digest_id"], "items": items}})
			if err := service.results(ctx); err != nil {
				t.Fatal(err)
			}
			if broker.publications[DeadQueue] != 1 {
				t.Fatal("mismatched digest accepted")
			}
			digests, _ := service.Store.digests(ctx, "1=1")
			if len(digests) != 1 || digests[0].Lines != nil {
				t.Fatalf("digest recorded lines anyway: %+v", digests)
			}
		})
	}
}

func TestQuietDayRecordsTheOccurrenceWithoutSending(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.digest(ctx); err != nil {
		t.Fatal(err)
	}
	if len(performer.calls) != 0 || broker.publications[TasksQueue] != 0 {
		t.Fatal("quiet day sent something")
	}
	seen, _ := service.Store.getMeta(ctx, "digest_occurrence")
	if seen == "" {
		t.Fatal("quiet day did not record its occurrence")
	}
	// A newsletter arriving after the send time waits for tomorrow rather than
	// firing a digest the moment it lands.
	classifyAsNews(t, service, broker)
	if err := service.digest(ctx); err != nil {
		t.Fatal(err)
	}
	if broker.publications[TasksQueue] != 0 {
		t.Fatal("late newsletter fired its own digest")
	}
}

func TestLatestOccurrenceRollsBackBeforeTheSendTime(t *testing.T) {
	london, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Skip("no tzdata")
	}
	morning := time.Date(2026, 10, 16, 7, 30, 0, 0, london)
	if got := latestOccurrence(morning, 8, 0, london); got.Day() != 15 {
		t.Fatalf("before the send time should use yesterday, got %s", got)
	}
	evening := time.Date(2026, 10, 16, 20, 0, 0, 0, london)
	if got := latestOccurrence(evening, 8, 0, london); got.Day() != 16 {
		t.Fatalf("after the send time should use today, got %s", got)
	}
}

func TestActionMailWithoutLinearIsStillNotifiedAndFiled(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	service.Config.LinearTeam = ""
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "action", Summary: "Pay the invoice",
		Action: "Pay it", Confidence: .99})
	if err := service.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, call := range performer.calls {
		kinds[call.Kind]++
	}
	if kinds["linear"] != 0 {
		t.Fatalf("ticket raised with no team configured: %+v", performer.calls)
	}
	if kinds["telegram"] != 1 || kinds["file"] != 1 {
		t.Fatalf("action mail lost its alert or filing: %+v", performer.calls)
	}
}

func classifyOnce(t *testing.T, service *Service, broker *fakeBroker, c Classification) Message {
	t.Helper()
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", c)
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	return oneMessage(t, service)
}

func TestEveryOutcomeRecordsWhichRuleSettledIt(t *testing.T) {
	cases := []struct {
		name     string
		incoming Classification
		attach   bool
		category string
		reason   string
	}{
		{"accepted", Classification{Category: "order", Summary: "Order", Confidence: .95},
			false, "order", "accepted"},
		{"below the threshold", Classification{Category: "order", Summary: "Order", Confidence: .7},
			false, "review", "confidence_below_threshold"},
		{"weak reading with extracted action is still unsettled", Classification{Category: "order", Summary: "Order", Action: "Pay it", Deadline: "Tomorrow", Confidence: .7},
			false, "review", "confidence_below_threshold"},
		{"an extracted action preserves news", Classification{Category: "news", Summary: "x", Action: "Pay it", Confidence: .95},
			false, "news", "accepted"},
		{"an extracted action preserves order", Classification{Category: "order", Summary: "LNER booking", Action: "Find an available seat", Confidence: .95},
			false, "order", "accepted"},
		{"an extracted deadline preserves order", Classification{Category: "order", Summary: "LNER booking", Deadline: "Departure 10:00", Confidence: .95},
			false, "order", "accepted"},
		{"an action with no extracted task stays action", Classification{Category: "action", Summary: "Inspect the original", Confidence: .95},
			false, "action", "accepted"},
		{"review with an extracted task stays review", Classification{Category: "review", Summary: "Uncertain", Action: "Pay it", Confidence: .5},
			false, "review", "confidence_below_threshold"},
		{"an attachment preserves the category", Classification{Category: "order", Summary: "Order", Confidence: .95},
			true, "order", "accepted"},
		{"suspicion preserves the category", Classification{Category: "news", Summary: "Withheld", Confidence: .95,
			Trace: &Trace{Suspicious: true}}, false, "news", "injection_suspected"},
		{"mail aimed at the machine", Classification{Category: "review", Summary: "x", Confidence: .99,
			Trace: &Trace{Suspicious: true}}, false, "review", "injection_suspected"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			service, mail, broker, _ := fixture(t)
			if test.attach {
				email := mail.emails["Stm4"]
				email.HasAttachment = true
				mail.emails["Stm4"] = email
			}
			got := classifyOnce(t, service, broker, test.incoming).Classification
			if got.Category != test.category || got.Trace == nil || got.Trace.Reason != test.reason {
				t.Fatalf("category=%q trace=%+v, want %q/%q", got.Category, got.Trace, test.category, test.reason)
			}
		})
	}
}

func TestThresholdHoldsUnsettledReadingsForReview(t *testing.T) {
	for _, test := range []struct {
		threshold float64
		category  string
	}{{0.9, "review"}, {0.8, "order"}} {
		service, _, broker, _ := fixture(t)
		service.Config.ConfidenceThreshold = test.threshold
		got := classifyOnce(t, service, broker, Classification{Category: "order", Summary: "Order", Confidence: .85})
		if got.Classification.Category != test.category {
			t.Fatalf("threshold %v gave %q, want %q", test.threshold, got.Classification.Category, test.category)
		}
	}
}

func TestWeakNewsWithoutSummaryIsHeldForReview(t *testing.T) {
	service, _, broker, _ := fixture(t)
	got := classifyOnce(t, service, broker, Classification{Category: "news", Confidence: .5}).Classification
	if got.Category != "review" || got.Summary == "" || got.Trace.Reason != "confidence_below_threshold" {
		t.Fatalf("unsettled news not held: %+v", got)
	}
}

func TestHTMLBodiesAreConvertedAndRecorded(t *testing.T) {
	service, mail, _, _ := fixture(t)
	email := mail.emails["Stm4"]
	email.TextBody = nil
	email.HTMLBody = []jmap.BodyPart{{PartID: "1"}}
	email.BodyValues = map[string]jmap.BodyValue{"1": {
		Value:       `<html><head><style>.a{color:red}</style></head><body><p>50% off until 31 October</p></body></html>`,
		IsTruncated: true,
	}}
	mail.emails["Stm4"] = email
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	if message.Mail.Body != "50% off until 31 October" {
		t.Fatalf("body not converted: %q", message.Mail.Body)
	}
	if !message.BodyHTML || message.BodyChars == 0 || !message.BodyTruncated {
		t.Fatalf("body facts missing: %+v", message)
	}
	// Truncation is our own fetch limit, so it must not hold the mail back.
	if message.Mail.Incomplete {
		t.Fatal("truncation treated as a defect in the mail")
	}
}

func TestMailThatNeverComesBackIsReportedNotRetriedInSilence(t *testing.T) {
	service, _, _, _ := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(2000000000, 0)
	service.Now = func() time.Time { return clock }
	// Nothing ever answers, and the republish window keeps passing.
	for attempt := 1; attempt <= StuckAttempts; attempt++ {
		if err := service.publish(ctx); err != nil {
			t.Fatalf("attempt %d reported a problem too early: %v", attempt, err)
		}
		clock = clock.Add(601 * time.Second)
	}
	err := service.publish(ctx)
	if err == nil {
		t.Fatal("a message with no classification after repeated attempts was not reported")
	}
	if !strings.Contains(err.Error(), "no classification") {
		t.Fatalf("unhelpful error: %v", err)
	}
	count, oldest, stuck, storeErr := service.Store.awaiting(ctx, clock.Unix())
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	if count != 1 || stuck != 1 || oldest <= 0 {
		t.Fatalf("awaiting count=%d stuck=%d oldest=%d", count, stuck, oldest)
	}
	// The failure has to reach the status endpoint, not just the return value.
	if err := service.Cycle(ctx); err == nil {
		t.Fatal("the cycle reported healthy while mail was stuck")
	}
	health, _ := service.Store.stepHealth(ctx)
	if health["publish"]["error"] == "" {
		t.Fatalf("publish reported healthy: %+v", health["publish"])
	}
}

func TestDeadLetteredResultsAreCounted(t *testing.T) {
	service, _, broker, _ := fixture(t)
	ctx := context.Background()
	broker.queues[ResultsQueue] = append(broker.queues[ResultsQueue],
		&bbmb.PickedMessage{GUID: "junk", Payload: map[string]any{"kind": "nonsense"}})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	if count, _ := service.Store.getMeta(ctx, "dead_letters"); count != "1" {
		t.Fatalf("dead letters counted as %q", count)
	}
}

func TestAnOutageIsNotReportedAsAnAttack(t *testing.T) {
	for _, test := range []struct {
		name   string
		trace  *Trace
		reason string
	}{
		{"classifier unavailable", &Trace{TriageFailed: true}, "triage_unavailable"},
		{"mail aimed at the machine", &Trace{Suspicious: true}, "injection_suspected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, _, broker, _ := fixture(t)
			got := classifyOnce(t, service, broker,
				Classification{Category: "review", Summary: "x", Confidence: .99, Trace: test.trace}).Classification
			if got.Trace.Reason != test.reason {
				t.Fatalf("reason %q, want %q", got.Trace.Reason, test.reason)
			}
		})
	}
}

func TestDecisionsPageShowsWhatWasDecidedAndWhy(t *testing.T) {
	service, _, broker, _ := fixture(t)
	ctx := context.Background()
	service.Config.DryRun = true
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	// A low confidence reading is unsettled and goes to review.
	broker.result("Stm4", Classification{Category: "order", Summary: "Order", Confidence: .5,
		Trace: &Trace{JevCategory: "order", JevConfidence: .5}})
	if err := service.results(ctx); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterUIRoutes(mux, service)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/personal-inbox", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d", recorder.Code)
	}
	page := recorder.Body.String()
	for _, want := range []string{
		"Echelon Coffee",             // sender
		"Your order",                 // subject
		"review",                     // the decision
		"0.50",                       // the score
		"confidence_below_threshold", // why
		"Jev decides the category",   // routing policy
		"preview only",               // the mode it is running in
		"app.fastmail.com",           // a way back to the message
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("page missing %q:\n%s", want, page)
		}
	}
}

func TestDecisionsPageWarnsWhenMailIsWaiting(t *testing.T) {
	service, _, _, _ := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterUIRoutes(mux, service)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/personal-inbox", nil))
	page := recorder.Body.String()
	if !strings.Contains(page, "waiting 1") || !strings.Contains(page, `class="warn"`) {
		t.Fatalf("unclassified mail not surfaced:\n%s", page)
	}
}
