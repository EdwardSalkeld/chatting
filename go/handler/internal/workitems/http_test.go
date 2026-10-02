package workitems

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeRegistrar struct {
	taskID string
	prURL  string
}

func (fake *fakeRegistrar) RegisterPR(_ context.Context, taskID, prURL string) error {
	fake.taskID, fake.prURL = taskID, prURL
	return nil
}

func TestRegisterPRRoute(t *testing.T) {
	fake := &fakeRegistrar{}
	mux := http.NewServeMux()
	RegisterRoutes(mux, fake)
	request := httptest.NewRequest(http.MethodPost, "/work-items/register-pr", strings.NewReader(`{"task_id":"task:1","pr_url":"https://github.com/o/r/pull/2"}`))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK || fake.taskID != "task:1" || fake.prURL != "https://github.com/o/r/pull/2" {
		t.Fatalf("registration not passed to handler: status=%d task=%q pr=%q", response.Code, fake.taskID, fake.prURL)
	}
}
