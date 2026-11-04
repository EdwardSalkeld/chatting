package metrics

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSQLiteMetricsFollowFilesAndMissingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	if err := os.WriteFile(path, []byte("database"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+"-wal", []byte("wal"), 0600); err != nil {
		t.Fatal(err)
	}
	recorder := New(time.Time{}, nil)
	recorder.SetSQLitePath(path)
	server, err := StartServer(recorder, "127.0.0.1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	response, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range []string{
		`chatting_sqlite_file_size_bytes{component="handler",file="database"} 8`,
		`chatting_sqlite_file_size_bytes{component="handler",file="wal"} 3`,
		`chatting_sqlite_file_size_bytes{component="handler",file="shm"} 0`,
	} {
		if !strings.Contains(string(body), sample) {
			t.Fatalf("missing %s in %s", sample, body)
		}
	}
	if err := os.Remove(path + "-wal"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(RenderSQLite(path), `file="wal"} 0`) {
		t.Fatal("missing WAL must report zero")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	missing := RenderSQLite(path)
	if strings.Contains(missing, `chatting_sqlite_file_size_bytes{component="handler",file="database"}`) {
		t.Fatal("missing database must not report a misleading zero size")
	}
	if !strings.Contains(missing, `chatting_sqlite_file_stat_success{component="handler",file="database"} 0`) {
		t.Fatal("missing failure metric")
	}
}
