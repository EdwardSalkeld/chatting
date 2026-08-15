package schedules

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
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	})
	mux := http.NewServeMux()
	RegisterRoutes(mux, NewService(store))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func decodeSchedule(t *testing.T, body []byte) scheduleJSON {
	t.Helper()
	var wrapper struct {
		Schedule scheduleJSON `json:"schedule"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		t.Fatalf("decode schedule: %v (body=%s)", err, body)
	}
	return wrapper.Schedule
}

func TestSchedulesAPILifecycle(t *testing.T) {
	server := newTestServer(t)
	client := server.Client()

	createBody := `{"job_name":"daily","content":"do the thing","cron":"0 9 * * *","context_refs":["a"],"reply_channel_type":"telegram","reply_channel_target":"123"}`
	response, err := client.Post(server.URL+"/api/schedules", "application/json", strings.NewReader(createBody))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("POST expected 201, got %d", response.StatusCode)
	}
	created := decodeSchedule(t, readBody(t, response))
	if created.Version != 1 || created.Status != "active" {
		t.Fatalf("unexpected created schedule: %+v", created)
	}
	if created.Timezone != "UTC" {
		t.Fatalf("expected default timezone UTC, got %q", created.Timezone)
	}

	listResponse, err := client.Get(server.URL + "/api/schedules")
	if err != nil {
		t.Fatalf("GET list: %v", err)
	}
	if listResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET list expected 200, got %d", listResponse.StatusCode)
	}
	var listWrapper struct {
		Schedules []scheduleJSON `json:"schedules"`
	}
	if err := json.Unmarshal(readBody(t, listResponse), &listWrapper); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listWrapper.Schedules) != 1 {
		t.Fatalf("expected 1 schedule listed, got %d", len(listWrapper.Schedules))
	}

	getResponse, err := client.Get(server.URL + "/api/schedules/" + created.ScheduleID)
	if err != nil {
		t.Fatalf("GET one: %v", err)
	}
	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET one expected 200, got %d", getResponse.StatusCode)
	}
	if got := decodeSchedule(t, readBody(t, getResponse)); got.ScheduleID != created.ScheduleID {
		t.Fatalf("GET one returned wrong schedule: %+v", got)
	}

	putBody := `{"job_name":"daily","content":"do the newer thing","cron":"0 10 * * *"}`
	putRequest, _ := http.NewRequest(http.MethodPut, server.URL+"/api/schedules/"+created.ScheduleID, strings.NewReader(putBody))
	putResponse, err := client.Do(putRequest)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if putResponse.StatusCode != http.StatusOK {
		t.Fatalf("PUT expected 200, got %d", putResponse.StatusCode)
	}
	replaced := decodeSchedule(t, readBody(t, putResponse))
	if replaced.Version != 2 {
		t.Fatalf("PUT expected version 2, got %d", replaced.Version)
	}

	historyResponse, err := client.Get(server.URL + "/api/schedules/" + created.ScheduleID + "?history=1")
	if err != nil {
		t.Fatalf("GET history: %v", err)
	}
	var historyWrapper struct {
		Schedule scheduleJSON   `json:"schedule"`
		History  []scheduleJSON `json:"history"`
	}
	if err := json.Unmarshal(readBody(t, historyResponse), &historyWrapper); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(historyWrapper.History) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(historyWrapper.History))
	}

	deleteRequest, _ := http.NewRequest(http.MethodDelete, server.URL+"/api/schedules/"+created.ScheduleID, nil)
	deleteResponse, err := client.Do(deleteRequest)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if deleteResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE expected 204, got %d", deleteResponse.StatusCode)
	}

	afterDelete, err := client.Get(server.URL + "/api/schedules/" + created.ScheduleID)
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	if afterDelete.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete expected 404, got %d", afterDelete.StatusCode)
	}
}

func TestSchedulesAPIValidationRejectsBadCron(t *testing.T) {
	server := newTestServer(t)
	body := `{"job_name":"daily","content":"x","cron":"not-a-cron"}`
	response, err := server.Client().Post(server.URL+"/api/schedules", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid cron, got %d", response.StatusCode)
	}
	var wrapper struct {
		Error string         `json:"error"`
		Usage map[string]any `json:"usage"`
	}
	if err := json.Unmarshal(readBody(t, response), &wrapper); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if wrapper.Error == "" {
		t.Fatal("expected an error message in the 400 response")
	}
	if wrapper.Usage["request_body"] == nil {
		t.Fatal("expected corrective API usage in the 400 response")
	}
}

func TestSchedulesAPIReplaceMissingReturns404(t *testing.T) {
	server := newTestServer(t)
	body := `{"job_name":"daily","content":"x","cron":"0 9 * * *"}`
	request, _ := http.NewRequest(http.MethodPut, server.URL+"/api/schedules/sched_missing", strings.NewReader(body))
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for missing schedule, got %d", response.StatusCode)
	}
}

func readBody(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return data
}
