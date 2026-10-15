package personalinbox

import (
	"context"
	"database/sql"
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
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/imap"
)

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

type fakeIMAP struct {
	validity uint32
	next     uint32
	mail     map[uint32][]byte
	failUID  uint32
	fetched  []uint32
	read     []uint32
	moved    []uint32
	dials    int
	logouts  int
}

func (client *fakeIMAP) Login(string, string) error { return nil }
func (client *fakeIMAP) Logout() error              { client.logouts++; return nil }
func (client *fakeIMAP) ExistsUID(uid uint32) (bool, error) {
	_, ok := client.mail[uid]
	return ok, nil
}
func (client *fakeIMAP) SelectUID(string) (uint32, uint32, error) {
	return client.validity, client.next, nil
}
func (client *fakeIMAP) SearchUID(start uint32) ([]uint32, error) {
	values := []uint32{}
	for uid := range client.mail {
		if uid >= start {
			values = append(values, uid)
		}
	}
	return values, nil
}
func (client *fakeIMAP) PeekUID(uid uint32) ([]byte, error) {
	client.fetched = append(client.fetched, uid)
	if uid == client.failUID {
		return nil, errors.New("fetch failure")
	}
	return client.mail[uid], nil
}
func (client *fakeIMAP) MarkReadUID(uid uint32) error {
	client.read = append(client.read, uid)
	return nil
}
func (client *fakeIMAP) MoveUID(uid uint32, folder string) error {
	client.moved = append(client.moved, uid)
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

func fixture(t *testing.T) (*Service, *fakeIMAP, *fakeBroker, *fakePerformer) {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	client := &fakeIMAP{validity: 42, next: 11, mail: map[uint32][]byte{10: []byte("From: shop@example.com\r\nSubject: Your order\r\n\r\nThanks for your order.")}}
	broker := &fakeBroker{queues: map[string][]*bbmb.PickedMessage{}, publications: map[string]int{}}
	performer := &fakePerformer{}
	service := &Service{Config: Config{Account: "main", Host: "imap.example.com", Port: 993, Username: "ed", Mailbox: "INBOX", Backfill: true, OrdersTarget: "orders", ActionTarget: "actions", DigestTarget: "digest", DigestSeconds: 1, LinearTeam: "team"}, Store: store, Broker: broker, Dial: func(context.Context) (imap.UIDClient, error) { client.dials++; return client, nil }, Performer: performer, Now: func() time.Time { return time.Unix(2000000000, 0) }}
	return service, client, broker, performer
}
func oneMessage(t *testing.T, service *Service) Message {
	t.Helper()
	messages, err := service.Store.messages(context.Background(), "1=1")
	if err != nil || len(messages) != 1 {
		t.Fatalf("messages %v err %v", messages, err)
	}
	return messages[0]
}

func TestBaselineAndAllNewMail(t *testing.T) {
	service, client, broker, _ := fixture(t)
	service.Config.Backfill = false
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(client.fetched) != 0 || broker.publications[TasksQueue] != 0 {
		t.Fatal("historical mail ingested")
	}
	client.mail[12] = []byte("From: test@example.com\r\nSubject: Already read in another client\r\n\r\nNew mail")
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	if message.UID != 12 || broker.publications[TasksQueue] != 1 {
		t.Fatal("new mail missing")
	}
	if len(client.read) != 0 || len(client.moved) != 0 {
		t.Fatal("ingestion mutated mailbox")
	}
	if len(broker.publications) != 1 {
		t.Fatal("mail published outside dedicated queue")
	}
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if broker.publications[TasksQueue] != 1 {
		t.Fatal("duplicate publish")
	}
}

func TestFailedFetchCannotSkipAnEmail(t *testing.T) {
	service, client, _, _ := fixture(t)
	client.mail[11] = []byte("Subject: second\r\n\r\nbody")
	client.failUID = 10
	if err := service.ingest(context.Background()); err == nil {
		t.Fatal("expected fetch failure")
	}
	cursor, _ := service.Store.getMeta(context.Background(), "cursor")
	if cursor != "0" || len(client.fetched) != 1 {
		t.Fatal("cursor skipped failed message")
	}
	client.failUID = 0
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	messages, _ := service.Store.messages(context.Background(), "1=1")
	if len(messages) != 2 {
		t.Fatal("recovery lost mail")
	}
}

func TestDurableIngestionSurvivesPublishFailure(t *testing.T) {
	service, client, broker, _ := fixture(t)
	broker.failPublish = true
	if err := service.Cycle(context.Background()); err == nil {
		t.Fatal("expected broker failure")
	}
	_ = oneMessage(t, service)
	cursor, _ := service.Store.getMeta(context.Background(), "cursor")
	if cursor != "10" {
		t.Fatal(cursor)
	}
	broker.failPublish = false
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if broker.publications[TasksQueue] != 1 || len(client.fetched) != 1 {
		t.Fatal("lost or refetched persisted mail")
	}
}

func TestPartialSuccessAndDuplicateResults(t *testing.T) {
	service, _, broker, performer := fixture(t)
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	classification := Classification{Category: "action", Summary: "Pay invoice", Action: "Pay", Confidence: .98}
	broker.result(message.ID, classification)
	broker.result(message.ID, classification)
	performer.failKind = "telegram"
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	effects, _ := service.Store.effects(context.Background(), "1=1")
	if len(effects) != 2 || effects[0].Status != "done" || effects[1].Status != "uncertain" {
		t.Fatalf("%+v", effects)
	}
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(performer.calls) != 2 {
		t.Fatal("blindly retried partial success")
	}
	broker.failAck = true
	broker.result(message.ID, classification)
	if err := service.results(context.Background()); err == nil {
		t.Fatal("expected ack error")
	}
	broker.failAck = false
	if err := service.results(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.perform(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(performer.calls) != 2 {
		t.Fatal("ACK replay repeated an effect")
	}
}

func TestActionPrecedenceAndUncertainty(t *testing.T) {
	for _, test := range []struct {
		name           string
		classification Classification
		incomplete     bool
		expected       string
	}{
		{"news with deadline", Classification{Category: "news", Summary: "Terms require response", Deadline: "Friday", Confidence: .99}, false, "action"},
		{"uncertain", Classification{Category: "news", Summary: "Uncertain newsletter", Confidence: .6}, false, "review"},
		{"attachment", Classification{Category: "news", Summary: "Attachment present", Confidence: .99}, true, "review"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, _, broker, _ := fixture(t)
			_ = service.ingest(context.Background())
			message := oneMessage(t, service)
			message.Mail.Incomplete = test.incomplete
			_ = service.Store.saveMessage(context.Background(), message, nil, "")
			broker.result(message.ID, test.classification)
			if err := service.results(context.Background()); err != nil {
				t.Fatal(err)
			}
			message = oneMessage(t, service)
			if message.Classification.Category != test.expected {
				t.Fatalf("%+v", message.Classification)
			}
		})
	}
}

func TestNewsDigestAndReadBeforeMove(t *testing.T) {
	service, _, broker, performer := fixture(t)
	service.Config.MoveNews = true
	service.Config.MarkNewsRead = true
	service.Config.NewsFolder = "News"
	_ = service.ingest(context.Background())
	message := oneMessage(t, service)
	message.Created -= 10
	_ = service.Store.saveMessage(context.Background(), message, nil, "")
	broker.result(message.ID, Classification{Category: "news", Summary: "A newsletter", Confidence: .99})
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(performer.calls) != 3 {
		t.Fatalf("effects %+v", performer.calls)
	}
	readIndex, moveIndex := -1, -1
	for i, call := range performer.calls {
		if call.Kind == "read" {
			readIndex = i
		}
		if call.Kind == "move" {
			moveIndex = i
		}
	}
	if readIndex < 0 || moveIndex <= readIndex {
		t.Fatal("read/move order incorrect")
	}
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(performer.calls) != 3 {
		t.Fatal("digest or mail effect repeated")
	}
}

func TestDryRunAndInvalidDestination(t *testing.T) {
	service, _, broker, performer := fixture(t)
	service.Config.DryRun = true
	_ = service.ingest(context.Background())
	message := oneMessage(t, service)
	broker.result(message.ID, Classification{Category: "order", Summary: "Order confirmed", Confidence: .99})
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	effects, _ := service.Store.effects(context.Background(), "1=1")
	if len(effects) != 1 || effects[0].Status != "preview" || len(performer.calls) != 0 {
		t.Fatal("dry-run sent an effect")
	}
	service.Config.DryRun = false
	_ = service.perform(context.Background())
	if len(performer.calls) != 0 {
		t.Fatal("preview replayed when live enabled")
	}
	broker.queues[ResultsQueue] = append(broker.queues[ResultsQueue], &bbmb.PickedMessage{GUID: "evil", Payload: map[string]any{"kind": "personal_inbox.result.v1", "message_id": message.ID, "target": "attacker"}})
	if err := service.results(context.Background()); err != nil {
		t.Fatal(err)
	}
	if broker.publications[DeadQueue] != 1 {
		t.Fatal("invalid result accepted")
	}
}

func TestRestartHoldsInflightAndResolvesExplicitly(t *testing.T) {
	service, _, _, performer := fixture(t)
	path := filepath.Join(t.TempDir(), "restart.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	message := Message{ID: "mail", UID: 10, Created: 1}
	effect := Effect{ID: "mail:telegram", Kind: "telegram", Status: "inflight"}
	if err := store.saveMessage(context.Background(), message, []Effect{effect}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("second writer accepted")
	}
	_ = store.Close()
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service.Store = store
	effects, _ := store.effects(context.Background(), "1=1")
	if effects[0].Status != "uncertain" {
		t.Fatal("lost inflight state")
	}
	_ = service.perform(context.Background())
	if len(performer.calls) != 0 {
		t.Fatal("uncertain effect retried")
	}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	request := httptest.NewRequest("POST", "/api/personal-inbox/resolve", strings.NewReader(`{"id":"mail:telegram","outcome":"done","note":"Confirmed Telegram delivery"}`))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	_ = service.perform(context.Background())
	if len(performer.calls) != 0 {
		t.Fatal("resolved delivery repeated")
	}
}

func TestUIDValidityChangeStopsIngestion(t *testing.T) {
	service, client, _, _ := fixture(t)
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.validity++
	client.mail[11] = []byte("Subject: changed\r\n\r\nnew message")
	if err := service.ingest(context.Background()); err == nil || !strings.Contains(err.Error(), "UIDVALIDITY") {
		t.Fatal(err)
	}
	if len(client.fetched) != 1 {
		t.Fatal("fetched reused UIDs")
	}
}

func TestTransientSQLiteWriterContentionRecovers(t *testing.T) {
	service, _, _, _ := fixture(t)
	path := filepath.Join(t.TempDir(), "contended.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service.Store = store
	other, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO inbox_meta VALUES ('contention-test','held')"); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() { time.Sleep(50 * time.Millisecond); _ = tx.Commit(); close(released) }()
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-released
	if oneMessage(t, service).UID != 10 {
		t.Fatal("contention lost message")
	}
	if err := service.Cycle(context.Background()); err != nil {
		t.Fatal("loop failed after contention", err)
	}
}

func TestOversizedMessageDoesNotBlockFollowingMail(t *testing.T) {
	service, client, _, _ := fixture(t)
	service.Dial = func(context.Context) (imap.UIDClient, error) { return &oversizedClient{client}, nil }
	client.mail[11] = []byte("Subject: subsequent email\r\n\r\nbody")
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	message := oneMessage(t, service)
	if !message.Mail.Incomplete || message.UID != 10 {
		t.Fatal("large message not held")
	}
	if err := service.ingest(context.Background()); err != nil {
		t.Fatal(err)
	}
	messages, _ := service.Store.messages(context.Background(), "1=1")
	if len(messages) != 2 {
		t.Fatal("subsequent mail blocked")
	}
}

type oversizedClient struct{ *fakeIMAP }

func (client *oversizedClient) PeekUID(uid uint32) ([]byte, error) {
	if uid == 10 {
		return nil, imap.ErrMessageTooLarge
	}
	return client.fakeIMAP.PeekUID(uid)
}

func TestMessageStatusTracksAllRequiredEffects(t *testing.T) {
	service, _, broker, _ := fixture(t)
	_ = service.ingest(context.Background())
	message := oneMessage(t, service)
	views, err := service.Store.messageViews(context.Background(), "")
	if err != nil || views[0].Status != "awaiting_classification" {
		t.Fatalf("%+v %v", views, err)
	}
	broker.result(message.ID, Classification{Category: "action", Summary: "Reply needed", Action: "Reply", Confidence: .99})
	_ = service.results(context.Background())
	views, _ = service.Store.messageViews(context.Background(), "")
	if views[0].Status != "processing" {
		t.Fatal(views[0].Status)
	}
	_ = service.perform(context.Background())
	views, _ = service.Store.messageViews(context.Background(), "")
	if views[0].Status != "processed" {
		t.Fatal(views[0].Status)
	}
	mux := http.NewServeMux()
	service.RegisterRoutes(mux)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest("GET", "/api/personal-inbox", nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"status":"processed"`) {
		t.Fatal(response.Body.String())
	}
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest("GET", "/api/personal-inbox?after="+message.ID, nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"messages":[]`) {
		t.Fatal("pagination failed", response.Body.String())
	}
}

func TestEnablingMarkReadDoesNotStrandPlannedMoves(t *testing.T) {
	service, _, broker, performer := fixture(t)
	service.Config.MoveNews = true
	service.Config.NewsFolder = "News"
	_ = service.ingest(context.Background())
	message := oneMessage(t, service)
	broker.result(message.ID, Classification{Category: "news", Summary: "A newsletter", Confidence: .99})
	if err := service.results(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The operator pauses before the move runs, then turns on mark_news_read.
	service.Config.DryRun = true
	if err := service.perform(context.Background()); err != nil {
		t.Fatal(err)
	}
	service.Config.DryRun = false
	service.Config.MarkNewsRead = true
	if err := service.perform(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(performer.calls) != 1 || performer.calls[0].Kind != "move" {
		t.Fatalf("move stranded by a read that was never planned: %+v", performer.calls)
	}
}

func TestMailboxEffectsShareOneLoginPerPass(t *testing.T) {
	service, client, broker, _ := fixture(t)
	service.Config.MoveNews = true
	service.Config.MarkNewsRead = true
	service.Config.NewsFolder = "News"
	service.Performer = &HandlerPerformer{Config: service.Config, Store: service.Store, Dial: service.Dial}
	_ = service.ingest(context.Background())
	message := oneMessage(t, service)
	broker.result(message.ID, Classification{Category: "news", Summary: "A newsletter", Confidence: .99})
	if err := service.results(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.dials, client.logouts = 0, 0
	for range 2 {
		if err := service.perform(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(client.read) != 1 || len(client.moved) != 1 {
		t.Fatalf("read %v moved %v", client.read, client.moved)
	}
	// One pass performs the read, the next the move: two passes, two logins,
	// not one login for every effect.
	if client.dials != 2 || client.logouts != 2 {
		t.Fatalf("dials %d logouts %d", client.dials, client.logouts)
	}
}
