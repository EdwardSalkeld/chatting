package personalinbox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestMailboxUIDResetPreventsMutation(t *testing.T) {
	service, client, _, _ := fixture(t)
	_ = service.ingest(context.Background())
	service.Config.MoveNews = true
	service.Config.MarkNewsRead = true
	service.Config.NewsFolder = "News"
	performer := &HandlerPerformer{Config: service.Config, Store: service.Store, Dial: service.Dial}
	client.validity++
	for _, effect := range []Effect{{Kind: "move", UID: 10, Target: "News"}, {Kind: "read", UID: 10, Target: "INBOX"}} {
		if _, err := performer.Perform(context.Background(), effect); err == nil {
			t.Fatal("reused UID changed")
		}
	}
	if len(client.read)+len(client.moved) != 0 {
		t.Fatal("mailbox mutated after reset")
	}
}

func TestLinearCreatesWhenLookupIsUnavailable(t *testing.T) {
	effect := Effect{ID: "mail:linear", Kind: "linear", Target: "team", Body: "action: Invoice\nPay Friday"}
	expected := issueID(effect.ID)
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(request.Body).Decode(&payload)
		if strings.HasPrefix(payload.Query, "query") {
			// Linear can answer an unknown UUID with a status rather than a
			// null issue; that says nothing about whether creation is needed.
			http.Error(writer, `{"errors":[{"message":"Entity not found"}]}`, http.StatusBadRequest)
			return
		}
		mutations++
		fmt.Fprintf(writer, `{"data":{"issueCreate":{"success":true,"issue":{"id":%q,"url":"https://linear.app/team/issue/ABC-2"}}}}`, expected)
	}))
	defer server.Close()
	performer := &HandlerPerformer{Config: Config{LinearTeam: "team"}, HTTP: server.Client(), LinearURL: server.URL, LinearKey: "test-key"}
	receipt, err := performer.Perform(context.Background(), effect)
	if err != nil || receipt != "https://linear.app/team/issue/ABC-2" || mutations != 1 {
		t.Fatalf("receipt %q err %v mutations %d", receipt, err, mutations)
	}
}
