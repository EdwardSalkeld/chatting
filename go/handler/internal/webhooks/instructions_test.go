package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestInstructionsNotificationIsSignedAndScoped(t *testing.T) {
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	secret := []byte("secret")
	handler := Handler(log.New(&bytes.Buffer{}, "", 0), secret, &Filter{InstructionsRepository: "Owner/Instructions", InstructionsStore: store})
	for _, tc := range []struct {
		body, event string
		signed      bool
		status      int
		want        int64
	}{
		{`{"ref":"refs/heads/main","repository":{"full_name":"owner/instructions"}}`, "push", false, 401, 0},
		{`{"ref":"refs/heads/other","repository":{"full_name":"owner/instructions"}}`, "push", true, 200, 0},
		{`{"ref":"refs/heads/main","repository":{"full_name":"other/instructions"}}`, "push", true, 200, 0},
		{`{"ref":"refs/heads/main","deleted":true,"repository":{"full_name":"owner/instructions"}}`, "push", true, 200, 0},
		{`{"ref":"refs/heads/main","repository":{"full_name":"owner/instructions"}}`, "release", true, 200, 0},
		{`{"ref":"refs/heads/main","repository":{"full_name":"owner/instructions"}}`, "push", true, 200, 1},
	} {
		r := httptest.NewRequest(http.MethodPost, GitHubPath, strings.NewReader(tc.body))
		r.Header.Set("X-GitHub-Event", tc.event)
		if tc.signed {
			r.Header.Set("X-Hub-Signature-256", signature(secret, tc.body))
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		got, err := store.InstructionsGeneration(context.Background())
		if err != nil || got != tc.want {
			t.Fatalf("generation=%d error=%v want=%d", got, err, tc.want)
		}
	}
	mux := http.NewServeMux()
	RegisterInstructionsRoutes(mux, store)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/instructions/generation", nil))
	var result map[string]int64
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result["generation"] != 1 {
		t.Fatalf("response=%s err=%v", w.Body.String(), err)
	}
}
