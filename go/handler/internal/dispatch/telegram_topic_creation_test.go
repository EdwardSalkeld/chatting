package dispatch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTelegramCreateForumTopicAndAmbiguousErrors(t *testing.T) {
	for _, code := range []int{200, 400, 500} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := recordTelegramJSONCall(t, r)
				if call.path != "/bottoken/createForumTopic" || call.payload["chat_id"] != "-100" || call.payload["name"] != "Workspace work" {
					t.Errorf("creation request: %+v", call)
				}
				if code != 200 {
					w.WriteHeader(code)
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": code, "description": "creation rejected or failed"})
				} else {
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_thread_id": 42, "name": "Workspace work"}})
				}
			}))
			defer server.Close()
			id, rejected, err := newTestTelegramSender(t, server.URL).CreateForumTopic(context.Background(), "-100", "Workspace work")
			if code == 200 && (err != nil || id != 42 || rejected) {
				t.Fatalf("success: id=%d rejected=%t err=%v", id, rejected, err)
			}
			if code != 200 && (err == nil || rejected != (code == 400)) {
				t.Fatalf("error: code=%d rejected=%t err=%v", code, rejected, err)
			}
		})
	}
}
