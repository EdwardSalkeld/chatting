package reminders

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestReminderUIListsAndEditsScheduledReminder(t *testing.T) {
	store, err := sqlitestate.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record, _, err := store.CreateReminder(context.Background(), sqlitestate.ReminderInput{
		RunAt: time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC), Prompt: "Water plants",
		ReplyChannelType: "telegram", ReplyChannelTarget: "-123", CreatedFromTaskID: "ui", IdempotencyKey: "ui-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	service := NewService(store)
	RegisterRoutes(mux, service)
	RegisterUIRoutes(mux, service)
	server := httptest.NewServer(mux)
	defer server.Close()

	for path, markers := range map[string][]string{
		"/reminders":                      {"Water plants", "/reminders/new", "/reminders/" + record.ReminderID},
		"/reminders/" + record.ReminderID: {"Edit reminder", "Water plants", "Cancel reminder"},
		"/reminders/new":                  {"New reminder", "datetime-local"},
	} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		raw := string(readBody(t, response))
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.StatusCode)
		}
		for _, marker := range markers {
			if !strings.Contains(raw, marker) {
				t.Fatalf("%s missing %q", path, marker)
			}
		}
	}
}
