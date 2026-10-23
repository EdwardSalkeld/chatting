package webhooks

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestGitHubTaskPreviewEvents(t *testing.T) {
	for _, event := range []string{"release", "push", "workflow_run", "issues", "ping", "installation", "future_event"} {
		t.Run(event, func(t *testing.T) {
			body := []byte(`{"action":"completed","repository":{"full_name":"owner/another-repo"},"sender":{"login":"alice"},"custom":{"keep":"this"}}`)
			now := time.Date(2026, 10, 23, 14, 0, 0, 0, time.UTC)
			message := GitHubTaskPreview(body, event, "delivery-123", now)
			raw, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := contracts.DecodeTaskQueueMessage(raw)
			if err != nil {
				t.Fatal(err)
			}
			envelope := decoded.Envelope
			if decoded.TaskID != "task:github-webhook:delivery-123" || envelope.DedupeKey != envelope.ID || envelope.Source != "webhook" || *envelope.Actor != "alice" || envelope.ReceivedAt.Time != now {
				t.Fatalf("incorrect task: %s", raw)
			}
			if envelope.ReplyChannel.Type != "log" || envelope.ReplyChannel.Metadata["repository"] != "owner/another-repo" || envelope.ReplyChannel.Metadata["github_event"] != event || !strings.Contains(envelope.Content, string(body)) {
				t.Fatalf("missing event data: %s", raw)
			}
			retry := GitHubTaskPreview(body, event, "delivery-123", now.Add(time.Minute))
			if retry.TaskID != decoded.TaskID || retry.TraceID != decoded.TraceID || retry.Envelope.DedupeKey != envelope.DedupeKey {
				t.Fatal("redelivery changed stable identity")
			}
			if GitHubTaskPreview(body, event, "delivery-456", now).TaskID == decoded.TaskID {
				t.Fatal("distinct deliveries collapsed")
			}
		})
	}
}

func TestGitHubTaskPreviewIncompleteAndUnexpectedPayloads(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `[]`, `{"sender":true,"action":1,"repository":"other"}`, `{"text":"` + strings.Repeat("界", MaxLogBodyBytes) + `"}`} {
		message := GitHubTaskPreview([]byte(body), "", "", time.Now())
		if err := message.Validate(); err != nil {
			t.Fatal(err)
		}
		retry := GitHubTaskPreview([]byte(body), "", "", time.Now())
		if message.TaskID != retry.TaskID || !strings.Contains(message.TaskID, "sha256:") {
			t.Fatal("missing-header fallback is not stable")
		}
		truncated := len(body) > MaxLogBodyBytes
		if message.Envelope.ReplyChannel.Metadata["body_truncated"] != truncated || strings.Contains(message.Envelope.Content, "(truncated)") != truncated {
			t.Fatal("incorrect truncation evidence")
		}
		if _, err := json.Marshal(message); err != nil {
			t.Fatal(err)
		}
	}
	if GitHubTaskPreview([]byte(`{}`), "ping", "", time.Now()).TaskID == GitHubTaskPreview([]byte(`{}`), "release", "", time.Now()).TaskID {
		t.Fatal("fallback ignored event type")
	}
}
