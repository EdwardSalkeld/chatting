package schedules

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestInvalidContextRefsCannotCreateOrReplaceSchedule(t *testing.T) {
	server := newTestServer(t)
	validBody := `{"job_name":"review-check","content":"Check reviews","cron":"0 12 * * *","context_refs":["repo:/workspace/memory"]}`
	response, err := server.Client().Post(server.URL+"/api/schedules", "application/json", strings.NewReader(validBody))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d %s", response.StatusCode, readBody(t, response))
	}
	created := decodeSchedule(t, readBody(t, response))
	for _, ref := range []string{"/workspace/memory", "repo:relative", "file:/workspace", ""} {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			t.Run(method+"/"+ref, func(t *testing.T) {
				body := strings.Replace(validBody, "repo:/workspace/memory", ref, 1)
				url := server.URL + "/api/schedules"
				if method == http.MethodPut {
					url += "/" + created.ScheduleID
				}
				request, err := http.NewRequest(method, url, strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				response, err := server.Client().Do(request)
				if err != nil {
					t.Fatal(err)
				}
				data := readBody(t, response)
				if response.StatusCode != http.StatusBadRequest {
					t.Fatalf("expected 400, got %d: %s", response.StatusCode, data)
				}
				var failure struct {
					Error string         `json:"error"`
					Usage map[string]any `json:"usage"`
				}
				if err := json.Unmarshal(data, &failure); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(failure.Error, "context_refs[0]") || !strings.Contains(failure.Error, "repo:/absolute/path") || failure.Usage["request_body"] == nil {
					t.Fatalf("missing corrective guidance: %s", data)
				}
			})
		}
	}
	response, err = server.Client().Get(server.URL + "/api/schedules/" + created.ScheduleID + "?history=1")
	if err != nil {
		t.Fatal(err)
	}
	var stored struct {
		Schedule scheduleJSON   `json:"schedule"`
		History  []scheduleJSON `json:"history"`
	}
	if err := json.Unmarshal(readBody(t, response), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Schedule.Version != 1 || len(stored.History) != 1 || stored.Schedule.ContextRefs[0] != "repo:/workspace/memory" {
		t.Fatalf("invalid replacement changed saved schedule: %+v", stored)
	}
	response, err = server.Client().Get(server.URL + "/api/schedules")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Schedules []scheduleJSON `json:"schedules"`
	}
	if err := json.Unmarshal(readBody(t, response), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Schedules) != 1 {
		t.Fatalf("invalid creates saved schedules: %+v", list.Schedules)
	}
}
