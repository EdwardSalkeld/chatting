package secrets

import (
	"bufio"
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestWorkerFileHandoff(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	root := t.TempDir()
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	script := `import sys
from pathlib import Path
from http.server import ThreadingHTTPServer
from app.worker.activity_web import build_handler
s = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(None, Path(sys.argv[1])))
print(s.server_port, flush=True)
s.serve_forever()
`
	cmd := exec.Command(python, "-u", "-c", script, root)
	cmd.Dir = repo
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	workerURL := "http://127.0.0.1:" + strings.TrimSpace(line)
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	env := contracts.TaskEnvelope{SchemaVersion: contracts.SchemaVersion, ID: "test:origin", Source: "im", ReceivedAt: contracts.NewTimestamp(time.Now()), Content: "request", ReplyChannel: contracts.ReplyChannel{Type: "telegram", Target: "-1"}, DedupeKey: "test:origin"}
	origin, err := store.AssignTask(context.Background(), contracts.NewTaskQueueMessage(env, "trace", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(root, origin.WorkItemID), 0700); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, root, "http://example.test", workerURL)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	service.RegisterWorker(mux)
	service.RegisterUI(mux)
	path := create(t, mux, origin.TaskID)
	if response := submit(mux, path, "private value"); response.Code != http.StatusOK {
		t.Fatalf("submission: %d %s", response.Code, response.Body.String())
	}
	events, err := service.Poll(context.Background())
	if err != nil || len(events) != 1 {
		t.Fatalf("poll: %v %v", events, err)
	}
	task, err := store.AssignTask(context.Background(), contracts.NewTaskQueueMessage(events[0], "trace", time.Now()))
	if err != nil || task.WorkItemID != origin.WorkItemID {
		t.Fatalf("routing: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, origin.WorkItemID, ".chatting-secrets"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("saved files: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(root, origin.WorkItemID, ".chatting-secrets", entries[0].Name()))
	if err != nil || string(content) != "private value" {
		t.Fatalf("file: %v", err)
	}
}
