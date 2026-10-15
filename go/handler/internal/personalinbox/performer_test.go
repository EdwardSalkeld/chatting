package personalinbox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/jmap"
)

func TestLinearRetryReconcilesStableIssueID(t *testing.T) {
	created := false
	mutations := 0
	effect := Effect{ID: "mail:linear", Kind: "linear", Target: "team", Body: "action: Invoice\nPay Friday"}
	expected := issueID(effect.ID)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "test-key" {
			t.Error("missing auth")
		}
		var payload struct {
			Query     string                     `json:"query"`
			Variables map[string]json.RawMessage `json:"variables"`
		}
		_ = json.NewDecoder(request.Body).Decode(&payload)
		if strings.HasPrefix(payload.Query, "query") {
			if created {
				fmt.Fprintf(writer, `{"data":{"issue":{"id":%q,"url":"https://linear.app/team/issue/ABC-1"}}}`, expected)
			} else {
				fmt.Fprint(writer, `{"data":{"issue":null}}`)
			}
			return
		}
		var input map[string]any
		_ = json.Unmarshal(payload.Variables["input"], &input)
		if input["id"] != expected || input["teamId"] != "team" || input["projectId"] != "project" {
			t.Errorf("wrong destination/id: %+v", input)
		}
		mutations++
		created = true
		// Simulate accepted creation with a lost/invalid response.
		fmt.Fprint(writer, `{"data":`)
	}))
	defer server.Close()
	performer := &HandlerPerformer{Config: Config{LinearTeam: "team", LinearProject: "project"}, HTTP: server.Client(), LinearURL: server.URL, LinearKey: "test-key"}
	if _, err := performer.Perform(context.Background(), effect); err == nil {
		t.Fatal("expected lost receipt")
	}
	receipt, err := performer.Perform(context.Background(), effect)
	if err != nil || receipt != "https://linear.app/team/issue/ABC-1" || mutations != 1 {
		t.Fatalf("receipt %q err %v mutations %d", receipt, err, mutations)
	}
}

func TestTelegramIsPlainTextAndFixedDestination(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		var payload map[string]any
		_ = json.NewDecoder(request.Body).Decode(&payload)
		if payload["chat_id"] != "orders" || payload["parse_mode"] != nil || payload["text"] != "untrusted [link] *text*" {
			t.Errorf("unsafe payload %+v", payload)
		}
		fmt.Fprint(writer, `{"ok":true,"result":{"message_id":123}}`)
	}))
	defer server.Close()
	performer := &HandlerPerformer{Config: Config{OrdersTarget: "orders", ActionTarget: "actions", DigestTarget: "digest"}, HTTP: server.Client(), TelegramBase: server.URL, TelegramToken: "test-token"}
	effect := Effect{Kind: "telegram", Target: "orders", Body: "untrusted [link] *text*"}
	if receipt, err := performer.Perform(context.Background(), effect); err != nil || receipt != "123" {
		t.Fatalf("%q %v", receipt, err)
	}
	effect.Target = "attacker"
	if _, err := performer.Perform(context.Background(), effect); err == nil {
		t.Fatal("arbitrary destination accepted")
	}
	if calls != 1 {
		t.Fatal("called unapproved destination")
	}
	for _, kind := range []string{"send", "delete", "smtp", "shell"} {
		effect.Kind = kind
		if _, err := performer.Perform(context.Background(), effect); err == nil {
			t.Fatal("unsupported operation accepted")
		}
	}
}

func TestFilingResolvesTheFolderAndMarksReadTogether(t *testing.T) {
	service, mail, _, _ := fixture(t)
	performer := &HandlerPerformer{Config: service.Config, Mail: mail, Folders: service.Folders}
	receipt, err := performer.Perform(context.Background(),
		Effect{Kind: "file", MessageID: "Stm4", Category: "news", Target: "Triage/News"})
	if err != nil || receipt != "filed:T2" {
		t.Fatalf("receipt %q err %v", receipt, err)
	}
	if len(mail.filed) != 1 || mail.filed[0] != (filing{Email: "Stm4", Mailbox: "T2", MarkRead: true}) {
		t.Fatalf("%+v", mail.filed)
	}
}

func TestFilingRefusesADestinationConfigurationNoLongerNames(t *testing.T) {
	service, mail, _, _ := fixture(t)
	performer := &HandlerPerformer{Config: service.Config, Mail: mail, Folders: service.Folders}
	// Planned for one folder, configuration now names another.
	_, err := performer.Perform(context.Background(),
		Effect{Kind: "file", MessageID: "Stm4", Category: "news", Target: "Triage/Orders"})
	if err == nil {
		t.Fatal("filed into an unapproved destination")
	}
	if len(mail.filed) != 0 {
		t.Fatalf("mailbox changed anyway: %+v", mail.filed)
	}
}

func TestAmbiguousFolderNameIsRefused(t *testing.T) {
	service, mail, _, _ := fixture(t)
	mail.boxes = append(mail.boxes, jmap.Mailbox{ID: "T9", Name: "News", ParentID: "T1"})
	service.Folders.Forget()
	service.Config.Folders = map[string]string{"news": "News"}
	performer := &HandlerPerformer{Config: service.Config, Mail: mail, Folders: service.Folders}
	if _, err := performer.Perform(context.Background(),
		Effect{Kind: "file", MessageID: "Stm4", Category: "news", Target: "News"}); err == nil {
		t.Fatal("ambiguous folder name accepted")
	}
	if len(mail.filed) != 0 {
		t.Fatalf("filed despite ambiguity: %+v", mail.filed)
	}
}

func TestResolverReportsAmbiguityAndAbsence(t *testing.T) {
	service, mail, _, _ := fixture(t)
	mail.boxes = append(mail.boxes, jmap.Mailbox{ID: "T9", Name: "News", ParentID: "T1"})
	service.Folders.Forget()
	ctx := context.Background()
	if _, err := service.Folders.ID(ctx, "News"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("want ambiguity reported, got %v", err)
	}
	// The full path still resolves even when the leaf name does not.
	if id, err := service.Folders.ID(ctx, "Triage/Orders"); err != nil || id != "T3" {
		t.Fatalf("id %q err %v", id, err)
	}
	if _, err := service.Folders.ID(ctx, "Nope"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("want absence reported, got %v", err)
	}
	if err := service.Folders.CheckConfigured(ctx, Config{Folders: map[string]string{"news": "Nope"}}); err == nil {
		t.Fatal("startup check passed a missing folder")
	}
}
