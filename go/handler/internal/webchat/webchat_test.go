package webchat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	state "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestWebIngressRejectsUntrustedRequestFieldsAndMethods(t *testing.T) {
	store, err := state.Open(context.Background(), filepath.Join(t.TempDir(), "handler.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	mux := http.NewServeMux()
	Service{Store: store}.Register(mux)
	for _, tc := range []struct {
		method, path, body string
		want               int
	}{
		{"POST", "/api/conversations", "{}", 405},
		{"DELETE", "/api/conversations/unknown/messages", "", 405},
		{"POST", "/api/conversations/unknown/messages", `{"message":"hello","client_message_id":"abcdefghijklmnop","context_refs":["repo:/secret"]}`, 400},
		{"POST", "/api/conversations/unknown/messages", `{"message":"hello","client_message_id":"abcdefghijklmnop","work_item_id":"fake"}`, 400},
		{"POST", "/api/conversations/unknown/messages", `{"message":"hello","client_message_id":"abcdefghijklmnop"} {}`, 400},
		{"POST", "/api/conversations/unknown/messages", `{"message":"hello","client_message_id":"abcdefghijklmnop"}`, 404},
	} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.want {
			t.Errorf("%s %s: %d want %d", tc.method, tc.body, w.Code, tc.want)
		}
	}
}
