package secrets

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func setup(t *testing.T) (*Service, contracts.TaskQueueMessage, *http.ServeMux) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	env := contracts.TaskEnvelope{SchemaVersion: contracts.SchemaVersion, ID: "telegram:original", Source: "im", ReceivedAt: contracts.NewTimestamp(time.Now()), Content: "hello", ReplyChannel: contracts.ReplyChannel{Type: "telegram", Target: "-1", Metadata: map[string]any{"message_thread_id": 42}}, DedupeKey: "original"}
	owner, err := store.AssignTask(ctx, contracts.NewTaskQueueMessage(env, "trace", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(root, owner.WorkItemID), 0700); err != nil {
		t.Fatal(err)
	}
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			WorkItemID string  `json:"work_item_id"`
			ID         string  `json:"request_id"`
			Expires    string  `json:"expires_at"`
			Secret     *string `json:"secret"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		workspace := filepath.Join(root, input.WorkItemID)
		info, statErr := os.Lstat(workspace)
		if statErr != nil || !info.IsDir() {
			http.Error(w, "invalid", 500)
			return
		}
		dir := filepath.Join(workspace, ".chatting-secrets")
		if os.MkdirAll(dir, 0700) != nil {
			http.Error(w, "invalid", 500)
			return
		}
		path := filepath.Join(dir, input.ID)
		_, statErr = os.Stat(path)
		existing := statErr == nil
		if !existing {
			expires, _ := time.Parse(time.RFC3339, input.Expires)
			if input.Secret == nil || time.Now().After(expires) {
				http.Error(w, "unavailable", 410)
				return
			}
			if err := os.WriteFile(path, []byte(*input.Secret), 0600); err != nil {
				http.Error(w, "invalid", 500)
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"path": path, "existing": existing})
	}))
	t.Cleanup(worker.Close)
	service, err := New(store, root, "https://example.test", worker.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	service.RegisterWorker(mux)
	service.RegisterUI(mux)
	return service, owner, mux
}
func create(t *testing.T, mux *http.ServeMux, task string) string {
	t.Helper()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/work-items/secret-request", strings.NewReader(`{"task_id":"`+task+`","label":"API token"}`)))
	if w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(result["url"])
	if err != nil {
		t.Fatal(err)
	}
	return u.Path
}
func submit(mux *http.ServeMux, path, value string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", path, strings.NewReader(url.Values{"secret": {value}}.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	mux.ServeHTTP(w, r)
	return w
}

func TestSubmissionRoutesOnceToOriginalWorkItem(t *testing.T) {
	s, owner, mux := setup(t)
	path := create(t, mux, owner.TaskID)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "API token") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	if w.Header().Get("Referrer-Policy") != "same-origin" || !strings.Contains(w.Body.String(), `name="viewport"`) {
		t.Fatal("missing browser form protections or mobile viewport")
	}
	if w = submit(mux, path, "top secret"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Your secret is saved") {
		t.Fatal("missing confirmation")
	}
	if w = submit(mux, path, "replacement"); w.Code != 410 {
		t.Fatal(w.Code)
	}
	events, err := s.Poll(context.Background())
	if err != nil || len(events) != 1 {
		t.Fatalf("poll %v %v", events, err)
	}
	e := events[0]
	if strings.Contains(e.Content, "top secret") {
		t.Fatal("secret leaked into event")
	}
	assigned, err := s.Store.AssignTask(context.Background(), contracts.NewTaskQueueMessage(e, "trace", time.Now()))
	if err != nil || assigned.WorkItemID != owner.WorkItemID {
		t.Fatalf("wrong work item: %+v %v", assigned, err)
	}
	files, err := os.ReadDir(filepath.Join(s.Root, owner.WorkItemID, ".chatting-secrets"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files %v %v", files, err)
	}
	filename := filepath.Join(s.Root, owner.WorkItemID, ".chatting-secrets", files[0].Name())
	data, err := os.ReadFile(filename)
	if err != nil || string(data) != "top secret" || !strings.Contains(e.Content, filename) {
		t.Fatalf("file mismatch %v", err)
	}
	info, _ := os.Stat(filename)
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode %v", info.Mode())
	}
	if err = s.AckEnvelope(context.Background(), e.ID); err != nil {
		t.Fatal(err)
	}
	events, err = s.Poll(context.Background())
	if err != nil || len(events) != 0 {
		t.Fatal(events, err)
	}
}
func TestConcurrentAndCrashRecovery(t *testing.T) {
	s, owner, mux := setup(t)
	path := create(t, mux, owner.TaskID)
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, v := range []string{"first", "second"} {
		wg.Add(1)
		go func(v string) { defer wg.Done(); codes <- submit(mux, path, v).Code }(v)
	}
	wg.Wait()
	close(codes)
	count := 0
	for c := range codes {
		if c == 200 {
			count++
		} else if c != 410 {
			t.Fatal(c)
		}
	}
	if count != 1 {
		t.Fatal("expected exactly one submission")
	}
	path = create(t, mux, owner.TaskID)
	token := strings.TrimPrefix(path, prefix)
	req, err := s.Store.SecretByHash(context.Background(), hash(token))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.save(req, []byte("before crash")); err != nil {
		t.Fatal(err)
	}
	events, err := s.Poll(context.Background())
	if err != nil || len(events) != 2 {
		t.Fatal(events, err)
	}
	if w := submit(mux, path, "overwrite"); w.Code != 410 {
		t.Fatal(w.Code)
	}
}
func TestExpiredAndSymlinkWorkspace(t *testing.T) {
	s, owner, mux := setup(t)
	token, _ := randomHex(32)
	req := sqlite.SecretRequest{ID: "expired", TokenHash: hash(token), Label: "expired", ExpiresAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)}
	if err := s.Store.CreateSecretRequest(context.Background(), owner.TaskID, req); err != nil {
		t.Fatal(err)
	}
	if w := submit(mux, prefix+token, "value"); w.Code != 410 {
		t.Fatal(w.Code)
	}
	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	expired, err := s.Store.SecretByHash(context.Background(), hash(token))
	if err != nil || expired.Status != "expired" {
		t.Fatalf("expired request still pending: %+v %v", expired, err)
	}
	path := create(t, mux, owner.TaskID)
	directory := filepath.Join(s.Root, owner.WorkItemID)
	if err := os.RemoveAll(directory); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, directory); err != nil {
		t.Fatal(err)
	}
	if w := submit(mux, path, "value"); w.Code == 200 {
		t.Fatal("followed symlink")
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 0 {
		t.Fatal("wrote outside workspace")
	}
}
