package platform

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/reminders"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/schedules"
	state "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func assets() fstest.MapFS {
	return fstest.MapFS{"index.html": {Data: []byte("<title>Chatting platform</title>")}, "assets/app.js": {Data: []byte("console.log('app')")}}
}
func platformServer(t *testing.T, upstream string) *httptest.Server {
	t.Helper()
	config := DefaultConfig()
	config.HandlerURL = upstream
	config.WorkerURL = upstream
	handler, err := NewHandler(config, assets())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}
func TestConfigRejectsMistakesAndUpstreamCredentials(t *testing.T) {
	for _, raw := range []string{`{"handler_url":"file:///tmp/private"}`, `{"worker_url":"http://user:secret@localhost"}`, `{"worker_url":"http://localhost/admin"}`, `{"handler_url":"http://localhost?x=1"}`, `{"unknown":1}`, `{"port":65536}`, `{"port":0}`, `null`, `{"host":""}`, `{} {}`} {
		if _, err := DecodeConfig(strings.NewReader(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	if _, err := DecodeConfig(strings.NewReader(`{}`)); err != nil {
		t.Fatal(err)
	}
}
func TestGatewayOnlyForwardsSupportedRoutesAndMethods(t *testing.T) {
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) }))
	defer upstream.Close()
	server := platformServer(t, upstream.URL)
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"POST", "/api/worker/items", 405}, {"GET", "/api/handler/egress", 404}, {"POST", "/api/handler/inbox", 405}, {"GET", "/api/worker/metrics", 404}, {"GET", "/api/handler/schedules/extra/nested", 404}, {"DELETE", "/api/handler/reminders", 405},
	} {
		request, _ := http.NewRequest(tc.method, server.URL+tc.path, nil)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != tc.status {
			t.Errorf("%s %s: got %d want %d", tc.method, tc.path, response.StatusCode, tc.status)
		}
	}
	if calls != 0 {
		t.Fatalf("unsupported requests reached upstream: %d", calls)
	}
}
func TestGatewayRewritesEncodedIDsQueriesAndStripsBrowserCredentials(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tasks/task:one/events" || r.URL.Query().Get("after") != "42" {
			t.Errorf("unexpected URL %s", r.URL.String())
		}
		if r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
			t.Error("forwarded browser credentials")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(409)
		_, _ = w.Write([]byte(`{"error":"upstream conflict"}`))
	}))
	defer upstream.Close()
	server := platformServer(t, upstream.URL)
	request, _ := http.NewRequest("GET", server.URL+"/api/worker/tasks/task%3Aone/events?after=42", nil)
	request.Header.Set("Cookie", "private=1")
	request.Header.Set("Authorization", "Bearer test")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 409 || !strings.Contains(string(body), "upstream conflict") {
		t.Fatalf("lost upstream error: %d %s", response.StatusCode, body)
	}
}
func TestGatewayRejectsCrossOriginAndOversizeWrites(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("rejected write reached upstream") }))
	defer upstream.Close()
	server := platformServer(t, upstream.URL)
	for _, tc := range []struct {
		origin, body string
		status       int
	}{{"https://other.example", "{}", 403}, {"", strings.Repeat("x", (1<<20)+1), 413}} {
		request, _ := http.NewRequest("POST", server.URL+"/api/handler/schedules", strings.NewReader(tc.body))
		request.Header.Set("Origin", tc.origin)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != tc.status {
			t.Errorf("got %d want %d", response.StatusCode, tc.status)
		}
	}
}
func TestGatewayUnavailableAndFrontendDeepLinks(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	upstream.Close()
	server := platformServer(t, upstream.URL)
	for _, tc := range []struct {
		path   string
		status int
		body   string
	}{{"/api/worker/items", 502, "Service unavailable"}, {"/items/item_a", 200, "Chatting platform"}, {"/runs/run%3Aone", 200, "Chatting platform"}, {"/assets/app.js", 200, "console.log"}, {"/assets/missing.js", 404, "404"}, {"/secrets.json", 404, "404"}} {
		response, err := server.Client().Get(server.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != tc.status || !strings.Contains(string(body), tc.body) {
			t.Errorf("%s: %d %s", tc.path, response.StatusCode, body)
		}
		if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Security-Policy") == "" {
			t.Error("missing private page headers")
		}
	}
}
func TestPlatformUsesRealScheduleAndReminderAPIs(t *testing.T) {
	store, err := state.Open(context.Background(), filepath.Join(t.TempDir(), "handler.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	mux := http.NewServeMux()
	schedules.RegisterRoutes(mux, schedules.NewService(store))
	reminders.RegisterRoutes(mux, reminders.NewService(store))
	upstream := httptest.NewServer(mux)
	defer upstream.Close()
	server := platformServer(t, upstream.URL)
	call := func(method, path, body string, status int) map[string]json.RawMessage {
		t.Helper()
		request, _ := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		request.Header.Set("Origin", server.URL)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		if response.StatusCode != status {
			t.Fatalf("%s %s got %d want %d: %s", method, path, response.StatusCode, status, raw)
		}
		var value map[string]json.RawMessage
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
		}
		return value
	}
	scheduleBody := `{"job_name":"briefing","content":"Prepare a briefing","cron":"0 9 * * 1-5","timezone":"Europe/London","context_refs":["repo:/workspace/chatting"],"prompt_context":[],"reply_channel_type":"telegram","reply_channel_target":"fixture","created_by":"platform"}`
	created := call("POST", "/api/handler/schedules", scheduleBody, 201)
	var schedule struct {
		ID string `json:"schedule_id"`
	}
	_ = json.Unmarshal(created["schedule"], &schedule)
	schedulePath := "/api/handler/schedules/" + schedule.ID
	call("PUT", schedulePath, strings.Replace(scheduleBody, "briefing", "updated briefing", 1), 200)
	history := call("GET", schedulePath+"?history=1", "", 200)
	var revisions []json.RawMessage
	_ = json.Unmarshal(history["history"], &revisions)
	if len(revisions) != 2 {
		t.Fatalf("schedule history: %s", history["history"])
	}
	call("DELETE", schedulePath, "", 204)
	call("GET", schedulePath, "", 404)
	reminderBody := `{"run_at":"2099-11-20T14:30:00Z","prompt":"Remember this","reply_channel":{"type":"telegram","target":"fixture","metadata":{"thread_id":"42"}},"context_refs":[],"prompt_context":[],"created_from_task_id":"platform","created_by":"platform","idempotency_key":"platform:one"}`
	created = call("POST", "/api/handler/reminders", reminderBody, 201)
	call("POST", "/api/handler/reminders", reminderBody, 200)
	var reminder struct {
		ID string `json:"reminder_id"`
	}
	_ = json.Unmarshal(created["reminder"], &reminder)
	reminderPath := "/api/handler/reminders/" + reminder.ID
	call("PUT", reminderPath, strings.ReplaceAll(strings.ReplaceAll(reminderBody, "14:30", "15:45"), "platform:one", "platform:two"), 200)
	call("GET", reminderPath+"?history=1", "", 200)
	call("DELETE", reminderPath, "", 204)
	call("PUT", reminderPath, reminderBody, 409)
	listed := call("GET", "/api/handler/reminders?status=all", "", 200)
	if !strings.Contains(string(listed["reminders"]), `"thread_id":"42"`) {
		t.Fatal("reply metadata was lost")
	}
}

func TestGatewayPreservesEncodedSlashesInsideWorkerIDs(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tasks/task:github:owner/repo/events" || !strings.Contains(r.URL.EscapedPath(), "owner%2Frepo") {
			t.Errorf("worker ID encoding lost: %s", r.URL.String())
		}
		_, _ = w.Write([]byte(`{"events":[],"active":false}`))
	}))
	defer upstream.Close()
	server := platformServer(t, upstream.URL)
	response, err := server.Client().Get(server.URL + "/api/worker/tasks/task%3Agithub%3Aowner%2Frepo/events")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("encoded ID: %d", response.StatusCode)
	}
	response, err = server.Client().Get(server.URL + "/runs/run%3Agithub%3Aowner%2Frepo")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("encoded deep link: %d", response.StatusCode)
	}
}
