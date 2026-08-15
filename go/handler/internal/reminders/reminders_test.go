package reminders

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	store, err := sqlitestate.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewService(store))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func validBody(key string, prompt string) string {
	return `{"run_at":"2026-08-15T14:30:00+01:00","prompt":"` + prompt + `","reply_channel":{"type":"telegram","target":"-123","metadata":{"message_id":42}},"context_refs":["repo:/workspace"],"prompt_context":["Be concise."],"created_from_task_id":"task:telegram:1","created_by":"worker","idempotency_key":"` + key + `"}`
}

func readBody(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decodeReminder(t *testing.T, raw []byte) reminderJSON {
	t.Helper()
	var body struct {
		Reminder reminderJSON `json:"reminder"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode: %v body=%s", err, raw)
	}
	return body.Reminder
}

func TestRemindersAPILifecycleAndIdempotency(t *testing.T) {
	server := newTestServer(t)
	client := server.Client()
	createdResponse, err := client.Post(server.URL+"/api/reminders", "application/json", strings.NewReader(validBody("create-1", "Do it")))
	if err != nil {
		t.Fatal(err)
	}
	if createdResponse.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", createdResponse.StatusCode, readBody(t, createdResponse))
	}
	created := decodeReminder(t, readBody(t, createdResponse))
	if created.Revision != 1 || created.Status != "scheduled" || created.RunAt != "2026-08-15T13:30:00Z" {
		t.Fatalf("created=%+v", created)
	}

	retryResponse, err := client.Post(server.URL+"/api/reminders", "application/json", strings.NewReader(validBody("create-1", "Do it")))
	if err != nil {
		t.Fatal(err)
	}
	if retryResponse.StatusCode != http.StatusOK {
		t.Fatalf("retry status=%d", retryResponse.StatusCode)
	}
	if retried := decodeReminder(t, readBody(t, retryResponse)); retried.ReminderID != created.ReminderID {
		t.Fatalf("retried=%+v", retried)
	}

	listResponse, err := client.Get(server.URL + "/api/reminders?status=scheduled")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Reminders []reminderJSON `json:"reminders"`
	}
	if err := json.Unmarshal(readBody(t, listResponse), &list); err != nil || len(list.Reminders) != 1 {
		t.Fatalf("list=%+v err=%v", list, err)
	}

	putRequest, _ := http.NewRequest(http.MethodPut, server.URL+"/api/reminders/"+created.ReminderID, strings.NewReader(validBody("update-1", "Do the new thing")))
	putResponse, err := client.Do(putRequest)
	if err != nil {
		t.Fatal(err)
	}
	if putResponse.StatusCode != http.StatusOK {
		t.Fatalf("put status=%d body=%s", putResponse.StatusCode, readBody(t, putResponse))
	}
	updated := decodeReminder(t, readBody(t, putResponse))
	if updated.Revision != 2 {
		t.Fatalf("updated=%+v", updated)
	}

	getResponse, err := client.Get(server.URL + "/api/reminders/" + created.ReminderID + "?history=1")
	if err != nil {
		t.Fatal(err)
	}
	var history struct {
		Reminder reminderJSON   `json:"reminder"`
		History  []reminderJSON `json:"history"`
	}
	if err := json.Unmarshal(readBody(t, getResponse), &history); err != nil || len(history.History) != 2 {
		t.Fatalf("history=%+v err=%v", history, err)
	}

	deleteRequest, _ := http.NewRequest(http.MethodDelete, server.URL+"/api/reminders/"+created.ReminderID, nil)
	deleteResponse, err := client.Do(deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	if deleteResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status=%d", deleteResponse.StatusCode)
	}
	after, err := client.Get(server.URL + "/api/reminders/" + created.ReminderID)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeReminder(t, readBody(t, after)); got.Status != "cancelled" {
		t.Fatalf("after=%+v", got)
	}
}

func TestReminderErrorsIncludeCorrectiveUsage(t *testing.T) {
	server := newTestServer(t)
	tests := []struct {
		name, body string
		wantStatus int
	}{
		{"timezone-less", `{"run_at":"2026-08-15T14:30:00","prompt":"x"}`, http.StatusBadRequest},
		{"unknown-field", `{"surprise":true}`, http.StatusBadRequest},
		{"missing-fields", `{}`, http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := server.Client().Post(server.URL+"/api/reminders", "application/json", strings.NewReader(test.body))
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status=%d", response.StatusCode)
			}
			var payload struct {
				Error string         `json:"error"`
				Usage map[string]any `json:"usage"`
			}
			if err := json.Unmarshal(readBody(t, response), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Error == "" || payload.Usage["request_body"] == nil || payload.Usage["endpoints"] == nil {
				t.Fatalf("payload=%+v", payload)
			}
		})
	}
}

func TestReminderIdempotencyConflictIncludesUsage(t *testing.T) {
	server := newTestServer(t)
	client := server.Client()
	first, err := client.Post(server.URL+"/api/reminders", "application/json", strings.NewReader(validBody("same-key", "First")))
	if err != nil {
		t.Fatal(err)
	}
	_ = readBody(t, first)
	conflict, err := client.Post(server.URL+"/api/reminders", "application/json", strings.NewReader(validBody("same-key", "Different")))
	if err != nil {
		t.Fatal(err)
	}
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d", conflict.StatusCode)
	}
	var payload map[string]any
	if err := json.Unmarshal(readBody(t, conflict), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["usage"] == nil {
		t.Fatalf("payload=%+v", payload)
	}
}
