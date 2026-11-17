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

func (fake *fakeRegistrar) RegisterRun(ctx context.Context, taskID, value string) error {
	return fake.RegisterPR(ctx, taskID, value)
}
func (fake *fakeRegistrar) RegisterWorkflow(ctx context.Context, taskID, value string) error {
	return fake.RegisterPR(ctx, taskID, value)
}

func TestRegisterRoutesPassOwnershipToStore(t *testing.T) {
	for kind, url := range map[string]string{
		"pr":       "https://github.com/o/r/pull/2",
		"run":      "https://github.com/o/r/actions/runs/7",
		"workflow": "https://github.com/o/r/actions/workflows/7",
	} {
		fake := &fakeRegistrar{}
		mux := http.NewServeMux()
		RegisterRoutes(mux, fake)
		body := `{"task_id":"task:1","` + kind + `_url":"` + url + `"}`
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/work-items/register-"+kind, strings.NewReader(body)))
		if response.Code != http.StatusOK || fake.taskID != "task:1" || fake.prURL != url {
			t.Fatalf("%s registration: %d %+v", kind, response.Code, fake)
		}
	}
}
