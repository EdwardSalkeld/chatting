package webhooks

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type fakeStatusStore struct {
	calls, limit, offset int
	err                  error
}

func (s *fakeStatusStore) GitHubWebhookStatus(_ context.Context, limit, offset int) (contracts.GitHubWebhookStatus, error) {
	s.calls++
	s.limit, s.offset = limit, offset
	return contracts.GitHubWebhookStatus{HeldCount: 3}, s.err
}

func TestStatusRouteIsReadOnlyAndValidatesPagination(t *testing.T) {
	for _, tc := range []struct {
		method, path        string
		code, limit, offset int
	}{
		{"GET", "/github/webhooks/status", 200, 50, 0},
		{"GET", "/github/webhooks/status?limit=1&offset=2", 200, 1, 2},
		{"GET", "/github/webhooks/status?limit=101", 400, 0, 0},
		{"GET", "/github/webhooks/status?offset=-1", 400, 0, 0},
		{"GET", "/github/webhooks/status?limit=x", 400, 0, 0},
		{"GET", "/github/webhooks/status?limit=1&limit=2", 400, 0, 0},
		{"GET", "/github/webhooks/status?replay=true", 400, 0, 0},
		{"POST", "/github/webhooks/status", 405, 0, 0},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			store := &fakeStatusStore{}
			mux := http.NewServeMux()
			RegisterStatusRoutes(mux, store)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.code || store.limit != tc.limit || store.offset != tc.offset {
				t.Fatalf("%d %s %+v", w.Code, w.Body.String(), store)
			}
			if tc.code != 200 && store.calls != 0 {
				t.Fatal("invalid request reached database")
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing cache control")
			}
		})
	}
	store := &fakeStatusStore{err: errors.New("private database detail")}
	mux := http.NewServeMux()
	RegisterStatusRoutes(mux, store)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/github/webhooks/status", nil))
	if w.Code != 500 || w.Body.String() != "{\"reason\":\"webhook status unavailable\"}\n" {
		t.Fatal(w.Code, w.Body.String())
	}
	// Public webhook ingress intentionally does not register the inspection API.
	w = httptest.NewRecorder()
	Handler(nil, []byte("secret")).ServeHTTP(w, httptest.NewRequest("GET", "/github/webhooks/status", nil))
	if w.Code != 404 {
		t.Fatal("inspection endpoint exposed on public webhook mux")
	}
}
