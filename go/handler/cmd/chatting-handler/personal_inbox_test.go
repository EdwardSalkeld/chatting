package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/personalinbox"
)

func TestInboxConstructionDoesNotWaitForMailServer(t *testing.T) {
	var calls atomic.Int32
	mail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer mail.Close()
	t.Setenv("TEST_INBOX_TOKEN", "test-token")
	config := handlerconfig.Config{DBPath: filepath.Join(t.TempDir(), "handler.db"),
		PersonalInbox: &personalinbox.Config{Enabled: true, DryRun: true,
			DBPath: filepath.Join(t.TempDir(), "inbox.db"), TokenEnv: "TEST_INBOX_TOKEN",
			SessionURL: mail.URL, Folders: map[string]string{"news": "Triage/News"}}}
	service, err := newPersonalInbox(config, nil)
	if err != nil {
		t.Fatalf("mail outage blocked handler setup: %v", err)
	}
	defer service.Store.Close()
	if calls.Load() != 0 {
		t.Fatal("constructor contacted the remote mail server")
	}
}
