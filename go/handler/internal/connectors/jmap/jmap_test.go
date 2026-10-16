package jmap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fixture(t *testing.T, handler func(request *http.Request, body map[string]any, writer http.ResponseWriter)) (*Client, *int) {
	t.Helper()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		if request.Header.Get("Authorization") != "Bearer secret-token" {
			t.Errorf("missing or wrong bearer token")
		}
		body := map[string]any{}
		if request.Method == http.MethodPost {
			_ = json.NewDecoder(request.Body).Decode(&body)
		}
		handler(request, body, writer)
	}))
	t.Cleanup(server.Close)
	client := &Client{HTTP: server.Client(), Token: "secret-token", SessionURL: server.URL + "/jmap/session"}
	return client, &calls
}

func sessionJSON(apiURL string) string {
	return fmt.Sprintf(`{"apiUrl":%q,"primaryAccounts":{"urn:ietf:params:jmap:mail":"acct1"},
		"capabilities":{"urn:ietf:params:jmap:core":{"maxCallsInRequest":42},"urn:ietf:params:jmap:mail":{}}}`, apiURL)
}

func TestSessionIsDiscoveredAndCached(t *testing.T) {
	var client *Client
	var calls *int
	client, calls = fixture(t, func(request *http.Request, _ map[string]any, writer http.ResponseWriter) {
		if !strings.HasSuffix(request.URL.Path, "/jmap/session") {
			t.Fatalf("unexpected path %s", request.URL.Path)
		}
		fmt.Fprint(writer, sessionJSON("https://region.example/jmap/api/"))
	})
	for range 3 {
		session, err := client.Session(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// The API URL is regional and must come from the session, never a constant.
		if session.APIURL != "https://region.example/jmap/api/" || session.AccountID != "acct1" || session.MaxCallsInRequest != 42 {
			t.Fatalf("%+v", session)
		}
	}
	if *calls != 1 {
		t.Fatalf("session fetched %d times, want 1", *calls)
	}
}

func TestStaleStateIsReportedDistinctly(t *testing.T) {
	client, _ := fixture(t, func(request *http.Request, _ map[string]any, writer http.ResponseWriter) {
		if strings.HasSuffix(request.URL.Path, "/jmap/session") {
			fmt.Fprint(writer, sessionJSON("http://"+request.Host+"/jmap/api/"))
			return
		}
		fmt.Fprint(writer, `{"methodResponses":[["error",{"type":"cannotCalculateChanges"},"c0"]]}`)
	})
	_, err := client.Changes(context.Background(), "old-state", 10)
	if !errors.Is(err, ErrCannotCalculateChanges) {
		t.Fatalf("want ErrCannotCalculateChanges, got %v", err)
	}
}

func TestBodyTextPrefersTextConvertsHTMLAndSeparatesFailures(t *testing.T) {
	cases := []struct {
		name  string
		email Email
		want  Body
	}{
		{"plain text wins", Email{
			TextBody:   []BodyPart{{PartID: "1"}},
			HTMLBody:   []BodyPart{{PartID: "2"}},
			BodyValues: map[string]BodyValue{"1": {Value: " plain "}, "2": {Value: "<p>html</p>"}},
		}, Body{Text: "plain"}},
		{"html is converted, not passed through", Email{
			HTMLBody:   []BodyPart{{PartID: "2"}},
			BodyValues: map[string]BodyValue{"2": {Value: "<style>.a{color:red}</style><p>50% off</p>"}},
		}, Body{Text: "50% off", HTML: true}},
		{"truncation is our limit, not a defect", Email{
			TextBody:   []BodyPart{{PartID: "1"}},
			BodyValues: map[string]BodyValue{"1": {Value: "cut", IsTruncated: true}},
		}, Body{Text: "cut", Truncated: true}},
		{"an encoding problem is a defect", Email{
			TextBody:   []BodyPart{{PartID: "1"}},
			BodyValues: map[string]BodyValue{"1": {Value: "mojibake", IsEncodingProblem: true}},
		}, Body{Text: "mojibake", Degraded: true}},
		{"a part that converts to nothing falls through", Email{
			HTMLBody:   []BodyPart{{PartID: "1"}, {PartID: "2"}},
			BodyValues: map[string]BodyValue{"1": {Value: `<img src="x.png">`}, "2": {Value: "<p>real words</p>"}},
		}, Body{Text: "real words", HTML: true}},
		{"no usable body at all is degraded", Email{}, Body{Degraded: true}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := test.email.BodyText(); got != test.want {
				t.Fatalf("got %+v want %+v", got, test.want)
			}
		})
	}
}

func TestSenderPrefersDisplayName(t *testing.T) {
	named := Email{From: []EmailAddress{{Name: " Echelon Coffee ", Email: "no-reply@squarespace.info"}}}
	if named.Sender() != "Echelon Coffee" {
		t.Fatalf("%q", named.Sender())
	}
	bare := Email{From: []EmailAddress{{Email: "no-reply@squarespace.info"}}}
	if bare.Sender() != "no-reply@squarespace.info" {
		t.Fatalf("%q", bare.Sender())
	}
	if (Email{}).Sender() != "" {
		t.Fatal("empty From should give an empty sender")
	}
}

func TestFileMovesAndMarksReadInOneSet(t *testing.T) {
	var update map[string]any
	client, _ := fixture(t, func(request *http.Request, body map[string]any, writer http.ResponseWriter) {
		if strings.HasSuffix(request.URL.Path, "/jmap/session") {
			fmt.Fprint(writer, sessionJSON("http://"+request.Host+"/jmap/api/"))
			return
		}
		calls := body["methodCalls"].([]any)
		if len(calls) != 1 {
			t.Fatalf("want one atomic set, got %d calls", len(calls))
		}
		args := calls[0].([]any)[1].(map[string]any)
		update = args["update"].(map[string]any)
		fmt.Fprint(writer, `{"methodResponses":[["Email/set",{"updated":{"m1":null}},"c0"]]}`)
	})
	if err := client.File(context.Background(), "m1", "box9", true); err != nil {
		t.Fatal(err)
	}
	patch := update["m1"].(map[string]any)
	if boxes := patch["mailboxIds"].(map[string]any); len(boxes) != 1 || boxes["box9"] != true {
		t.Fatalf("mailboxIds %+v", boxes)
	}
	if patch["keywords/$seen"] != true {
		t.Fatalf("read flag not set in the same patch: %+v", patch)
	}
}

func TestFileReportsRefusal(t *testing.T) {
	client, _ := fixture(t, func(request *http.Request, _ map[string]any, writer http.ResponseWriter) {
		if strings.HasSuffix(request.URL.Path, "/jmap/session") {
			fmt.Fprint(writer, sessionJSON("http://"+request.Host+"/jmap/api/"))
			return
		}
		fmt.Fprint(writer, `{"methodResponses":[["Email/set",{"notUpdated":{"m1":{"type":"notFound"}}},"c0"]]}`)
	})
	err := client.File(context.Background(), "m1", "box9", false)
	if err == nil || !strings.Contains(err.Error(), "notFound") {
		t.Fatalf("want the refusal surfaced, got %v", err)
	}
}

func TestRequestErrorsNeverCarryTheToken(t *testing.T) {
	client, _ := fixture(t, func(request *http.Request, _ map[string]any, writer http.ResponseWriter) {
		if strings.HasSuffix(request.URL.Path, "/jmap/session") {
			http.Error(writer, "nope", http.StatusUnauthorized)
			return
		}
	})
	_, err := client.Session(context.Background())
	if err == nil {
		t.Fatal("expected failure")
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("token leaked into error: %v", err)
	}
}
