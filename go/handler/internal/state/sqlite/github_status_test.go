package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestWebhookStatusReportsPagedHoldsWithoutChangingThem(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	body := []byte(`{"repository":{"full_name":"o/r"},"workflow_run":{"html_url":"https://github.com/o/r/actions/runs/12"},"comment":{"body":"PRIVATE REQUEST BODY"}}`)
	for _, key := range []string{"b", "a"} {
		if err := store.HoldGitHub(ctx, key, "workflow_run", "delivery-"+key, body, "workflow_missing_pr_context", time.Unix(100, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.HoldGitHub(ctx, "c", "issues", "delivery-c", []byte(`[]`), "invalid_event_shape", time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO github_webhook_pending VALUES ('pending','{}','now'); INSERT INTO work_item_artifacts VALUES ('github_workflow','https://github.com/o/r/actions/workflows/34','owner')`); err != nil {
		t.Fatal(err)
	}
	status, err := store.GitHubWebhookStatus(ctx, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if status.HeldCount != 3 || status.PendingCount != 1 || status.HeldByReason["workflow_missing_pr_context"] != 2 || len(status.Held) != 1 || status.Held[0].EventKey != "a" || status.Held[0].RunURL != "https://github.com/o/r/actions/runs/12" || status.Held[0].BodyBytes != len(body) || len(status.Workflows) != 1 || status.Workflows[0].WorkItemID != "owner" {
		t.Fatalf("%+v", status)
	}
	encoded, _ := json.Marshal(status)
	if strings.Contains(string(encoded), "PRIVATE REQUEST BODY") {
		t.Fatal("raw payload exposed")
	}
	page, err := store.GitHubWebhookStatus(ctx, 1, 1)
	if err != nil || len(page.Held) != 1 || page.Held[0].EventKey != "b" {
		t.Fatalf("%+v %v", page, err)
	}
	last, err := store.GitHubWebhookStatus(ctx, 1, 2)
	if err != nil || last.Held[0].EventKey != "c" || last.Held[0].Repository != "" {
		t.Fatalf("%+v %v", last, err)
	}
	var held, pending int
	if err := store.db.QueryRow(`SELECT count(*) FROM github_webhook_held`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM github_webhook_pending`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if held != 3 || pending != 1 {
		t.Fatal("status read changed delivery state")
	}
	for _, pagination := range [][2]int{{0, 0}, {101, 0}, {1, -1}, {1, 1000001}} {
		if _, err := store.GitHubWebhookStatus(ctx, pagination[0], pagination[1]); err == nil {
			t.Fatal("invalid pagination accepted")
		}
	}
}
