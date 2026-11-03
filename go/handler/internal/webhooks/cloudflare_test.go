package webhooks

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

// Shape retained from the provider comment inspected in the November 3 audit.
func previewBody(commit, status, url string) string {
	return fmt.Sprintf("## Deploying rnp-blog with &nbsp;Cloudflare Pages\n\n<table><tr><td><strong>Latest commit:</strong> </td><td><code>%s</code></td></tr><tr><td><strong>Status:</strong></td><td>&nbsp;%s</td></tr><tr><td>Preview URL:</td><td><a href='%s'>Preview</a></td></tr></table>", commit, status, url)
}

func TestCloudflarePreviewTransitionsPersist(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "handler.db")
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	f := &Filter{Store: store, Login: "BillyAcachofa"}
	if err := store.PutWebhookState(ctx, "watch", f.watchKey("o/r", 1), `{"head_sha":"full-head"}`); err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	check := func(commit, status, want, reason string) {
		t.Helper()
		stamp = stamp.Add(time.Second)
		payload := map[string]any{"action": "edited", "repository": map[string]string{"full_name": "o/r"}, "sender": user{Login: "cloudflare-workers-and-pages[bot]"}, "issue": map[string]any{"number": 1}, "comment": textObject{ID: 123, UpdatedAt: stamp.Format(time.RFC3339), Body: previewBody(commit, status, fmt.Sprint(stamp.Unix()))}}
		body, _ := json.Marshal(payload)
		d, err := f.Evaluate(ctx, body, "issue_comment", "")
		if err != nil || d.Disposition != want || d.Reason != reason {
			t.Fatalf("%s/%s: %+v %v", commit, status, d, err)
		}
	}
	check("68d8b48", "⏳ Build in progress", "drop", "preview_progress")
	check("68d8b48", "✅ Deploy successful!", "keep", "preview_ready")
	check("68d8b48", "✅ Deploy successful!", "drop", "preview_unchanged")
	check("68d8b48", "⏳ Building", "drop", "preview_progress")
	check("68d8b48", "✅ Deploy successful!", "drop", "preview_unchanged")
	check("68d8b48", "❌ Build failed", "keep", "preview_failed")
	check("68d8b48", "❌ Build failed", "drop", "preview_unchanged")
	check("68d8b48", "✅ Deploy successful!", "keep", "preview_ready")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	f.Store = store
	check("68d8b48", "✅ Deploy successful!", "drop", "preview_unchanged")
	check("dbb569c", "⏳ Building", "drop", "preview_progress")
	check("dbb569c", "✅ Deploy successful!", "keep", "preview_ready")
	stamp = stamp.Add(-time.Hour)
	check("68d8b48", "❌ Build failed", "drop", "preview_outdated")
	state, err := store.WebhookState(ctx, "watch", f.watchKey("o/r", 1))
	var watch watchState
	if err != nil || json.Unmarshal([]byte(state), &watch) != nil || watch.HeadSHA != "full-head" {
		t.Fatalf("watch overwritten: %s %v", state, err)
	}
}

func TestPreviewMessagesKeepGenericHandlingWhenUncertain(t *testing.T) {
	for _, tc := range []struct {
		name, actor, body, stamp string
		id                       int64
	}{
		{"human", "alice", previewBody("68d8b48", "✅ Deploy successful!", "url"), "2026-11-02T12:00:00Z", 1},
		{"unknown status", "cloudflare-workers-and-pages[bot]", previewBody("68d8b48", "Needs your attention", "url"), "2026-11-02T12:00:00Z", 1},
		{"unknown format", "cloudflare-workers-and-pages[bot]", "Please investigate this deployment", "2026-11-02T12:00:00Z", 1},
		{"missing timestamp", "cloudflare-workers-and-pages[bot]", previewBody("68d8b48", "✅ Deploy successful!", "url"), "", 1},
		{"missing id", "cloudflare-workers-and-pages[bot]", previewBody("68d8b48", "✅ Deploy successful!", "url"), "2026-11-02T12:00:00Z", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "handler.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			f := &Filter{Store: store, Login: "BillyAcachofa"}
			if err := store.PutWebhookState(ctx, "watch", f.watchKey("o/r", 1), `{}`); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(map[string]any{"action": "created", "repository": map[string]string{"full_name": "o/r"}, "sender": user{Login: tc.actor}, "issue": map[string]int{"number": 1}, "comment": textObject{Body: tc.body, ID: tc.id, UpdatedAt: tc.stamp}})
			d, err := f.Evaluate(ctx, body, "issue_comment", "")
			if err != nil || d.Disposition != "keep" || d.Reason != "watched_subject" {
				t.Fatalf("%+v %v", d, err)
			}
		})
	}
}
