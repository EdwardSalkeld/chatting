package personalinbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		DigestTarget: "digest", DigestSeconds: 1, LinearTeam: "team", LinkTemplate: DefaultLinkTemplate,
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

func TestReviewStaysInTheInboxAndIsStillRelayed(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "review", Summary: "Not sure", Confidence: .99})
	if err := service.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	if len(performer.calls) != 1 || performer.calls[0].Kind != "telegram" {
		t.Fatalf("review should alert and nothing else: %+v", performer.calls)
	}
	// The link still points somewhere useful even with no destination folder.
	if !strings.Contains(performer.calls[0].Body, "/mail/Inbox/AUjt.Stm4?u=b8884168") {
		t.Fatalf("review link wrong:\n%s", performer.calls[0].Body)
	}
}

func TestAttachmentsAndDegradedBodiesGoToReview(t *testing.T) {
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
	if got := oneMessage(t, service).Classification.Category; got != "review" {
		t.Fatalf("category %q, want review", got)
	}
}

func TestNewsIsFiledAndDigestedButNotAlerted(t *testing.T) {
	service, _, broker, performer := fixture(t)
	ctx := context.Background()
	if err := service.ingest(ctx); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	message.Created -= 10
	if err := service.Store.saveMessage(ctx, message, nil); err != nil {
		t.Fatal(err)
	}
	broker.result("Stm4", Classification{Category: "news", Summary: "A newsletter", Confidence: .99})
	if err := service.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, call := range performer.calls {
		kinds[call.Kind]++
	}
	// A filing and a digest, but no per-newsletter alert.
	if kinds["file"] != 1 || kinds["telegram"] != 1 {
		t.Fatalf("effects %+v", performer.calls)
	}
	for _, call := range performer.calls {
		if call.Kind == "telegram" && call.Target != "digest" {
			t.Fatalf("news alerted outside the digest: %+v", call)
		}
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
