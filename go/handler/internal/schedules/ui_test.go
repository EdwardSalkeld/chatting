package schedules

import (
	"context"
	"net/http"
	"strings"
	"testing"

	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestUIListAndDetailRender(t *testing.T) {
	server, store := newTestServer(t)
	record, err := store.CreateSchedule(context.Background(), sqlitestate.ScheduleInput{
		JobName:            "daily-note",
		Content:            "Write the daily note",
		Cron:               "5 7 * * *",
		Timezone:           "Europe/London",
		ContextRefs:        []string{"repo:/workspace/chatting"},
		ReplyChannelType:   "telegram",
		ReplyChannelTarget: "8605042448",
	})
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	listBody := getOK(t, server.Client(), server.URL+"/schedules")
	for _, marker := range []string{"daily-note", "5 7 * * *", "/schedules/new", "/schedules/" + record.ScheduleID} {
		if !strings.Contains(listBody, marker) {
			t.Fatalf("list page missing %q\n%s", marker, listBody)
		}
	}

	detailBody := getOK(t, server.Client(), server.URL+"/schedules/"+record.ScheduleID)
	for _, marker := range []string{
		`name="job_name"`,
		"daily-note",
		"Europe/London",
		"8605042448",
		`id="delete"`,
	} {
		if !strings.Contains(detailBody, marker) {
			t.Fatalf("detail page missing %q\n%s", marker, detailBody)
		}
	}

	newBody := getOK(t, server.Client(), server.URL+"/schedules/new")
	if !strings.Contains(newBody, `name="job_name"`) {
		t.Fatalf("new schedule form missing job_name input\n%s", newBody)
	}
	if strings.Contains(newBody, `id="delete"`) {
		t.Fatal("new schedule form should not offer delete")
	}
}

func getOK(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s expected 200, got %d", url, response.StatusCode)
	}
	return string(readBody(t, response))
}
