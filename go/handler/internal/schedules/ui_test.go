package schedules

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func newTestUIServer(t *testing.T) (*httptest.Server, *sqlitestate.Store) {
	t.Helper()
	store, err := sqlitestate.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	})
	mux := http.NewServeMux()
	service := NewService(store)
	RegisterRoutes(mux, service)
	RegisterUIRoutes(mux, service)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, store
}

func TestUIListAndDetailRender(t *testing.T) {
	server, store := newTestUIServer(t)
	record, err := store.CreateSchedule(context.Background(), sqlitestate.ScheduleInput{
		JobName:            "daily-note",
		Content:            "Write the daily note",
		Cron:               "5 7 * * *",
		Timezone:           "Europe/London",
		ContextRefs:        []string{"repo:/workspace/chatting"},
		ReplyChannelType:   "telegram",
		ReplyChannelTarget: "8605042448",
	})
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	listBody := getOK(t, server.Client(), server.URL+"/schedules")
	for _, marker := range []string{"daily-note", "5 7 * * *", "/schedules/new", "/schedules/" + record.ScheduleID} {
		if !strings.Contains(listBody, marker) {
			t.Fatalf("list page missing %q\n%s", marker, listBody)
		}
	}

	detailBody := getOK(t, server.Client(), server.URL+"/schedules/"+record.ScheduleID)
	for _, marker := range []string{
		`name="job_name"`,
		`name="content"`,
		`name="cron"`,
		`name="timezone"`,
		`name="context_refs"`,
		`name="prompt_context"`,
		`name="reply_channel_type"`,
		`name="reply_channel_target"`,
		"daily-note",
		"Europe/London",
		"8605042448",
		`id="delete"`,
	} {
		if !strings.Contains(detailBody, marker) {
			t.Fatalf("detail page missing %q\n%s", marker, detailBody)
		}
	}

	newBody := getOK(t, server.Client(), server.URL+"/schedules/new")
	if !strings.Contains(newBody, `name="job_name"`) {
		t.Fatalf("new schedule form missing job_name input\n%s", newBody)
	}
	if strings.Contains(newBody, `id="delete"`) {
		t.Fatal("new schedule form should not offer delete")
	}
}

func TestUIDetailMissingReturns404(t *testing.T) {
	server, _ := newTestUIServer(t)
	response, err := server.Client().Get(server.URL + "/schedules/sched_missing")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for missing schedule, got %d", response.StatusCode)
	}
}

func getOK(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s expected 200, got %d", url, response.StatusCode)
	}
	return string(readBody(t, response))
}
