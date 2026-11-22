package httpserver

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateSocketPreservesFilesAndActiveListenersAndRecoversStaleSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "chat-unix-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "private.sock")
	mux := http.NewServeMux()
	if err = os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = StartUnix(path, mux); err == nil {
		t.Fatal("replaced ordinary file")
	}
	if data, _ := os.ReadFile(path); string(data) != "keep" {
		t.Fatal("file changed")
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	server, err := StartUnix(path, mux)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = StartUnix(path, mux); err == nil {
		t.Fatal("replaced active listener")
	}
	if err = server.Close(); err != nil {
		t.Fatal(err)
	}
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	stale.Close()
	recovered, err := StartUnix(path, mux)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0660 {
		t.Fatalf("socket mode: %v %v", info, err)
	}
}
