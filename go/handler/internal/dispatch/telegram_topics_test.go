package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestTelegramTopicsAcrossTextMediaAndFormattingFallback(t *testing.T) {
	for _, topic := range []int64{0, 7, 8} {
		for _, media := range []string{"text", "photo.png", "report.pdf"} {
			t.Run(fmt.Sprintf("%d/%s", topic, media), func(t *testing.T) {
				dir := t.TempDir()
				body := "part of an answer"
				message := contracts.OutboundMessage{Channel: "telegram", Target: "-100", Body: &body}
				if topic != 0 {
					message.Metadata = map[string]any{"message_thread_id": topic}
				}
				if media != "text" {
					path := filepath.Join(dir, media)
					if err := os.WriteFile(path, []byte("attachment bytes"), 0o600); err != nil {
						t.Fatal(err)
					}
					message.Attachment = &contracts.AttachmentRef{URI: path}
				}
				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					var got string
					if media == "text" {
						call := recordTelegramJSONCall(t, r)
						if v, ok := call.payload["message_thread_id"]; ok {
							got = fmt.Sprint(v)
						}
					} else {
						call := recordTelegramMultipartCall(t, r)
						got = call.fields["message_thread_id"]
						wantMethod := "/bottoken/sendDocument"
						if media == "photo.png" {
							wantMethod = "/bottoken/sendPhoto"
						}
						if call.path != wantMethod {
							t.Errorf("method = %s", call.path)
						}
					}
					want := ""
					if topic != 0 {
						want = fmt.Sprint(topic)
					}
					if got != want {
						t.Errorf("topic = %q, want %q", got, want)
					}
					if calls == 1 {
						_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "description": "Bad Request: can't parse entities"})
						return
					}
					result := map[string]any{"message_id": 100 + calls}
					if topic != 0 {
						result["message_thread_id"] = topic
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": result})
				}))
				defer server.Close()
				sender := newTestTelegramSender(t, server.URL, dir)
				// An explicit General send must not inherit this inbound topic.
				envelope := contracts.TaskEnvelope{ReplyChannel: contracts.ReplyChannel{
					Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": int64(99)},
				}}
				sent, err := (Dispatcher{TelegramSender: sender}).Dispatch(context.Background(), message, envelope)
				if err != nil {
					t.Fatal(err)
				}
				if calls != 2 {
					t.Fatalf("calls = %d", calls)
				}
				got, present := sent.Metadata["message_thread_id"]
				if topic == 0 && present {
					t.Fatalf("General inherited topic: %v", got)
				}
				if topic != 0 && got != topic {
					t.Fatalf("confirmed topic = %v, want %d", got, topic)
				}
				if _, mutated := message.Metadata["message_id"]; mutated {
					t.Fatal("dispatch mutated input metadata")
				}
			})
		}
	}
}

func TestTelegramInvalidOrUnavailableTopicNeverFallsBackToGeneral(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "description": "Bad Request: message thread not found"})
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL)
	body := "hello"
	for _, topic := range []any{true, -1, 0, "7", 1.5} {
		err := sender.Send(context.Background(), "-100", contracts.OutboundMessage{
			Channel: "telegram", Target: "-100", Body: &body, Metadata: map[string]any{"message_thread_id": topic},
		})
		if err == nil {
			t.Fatalf("accepted invalid topic %v", topic)
		}
	}
	if calls != 0 {
		t.Fatalf("invalid topic made %d API calls", calls)
	}
	err := sender.Send(context.Background(), "-100", contracts.OutboundMessage{
		Channel: "telegram", Target: "-100", Body: &body, Metadata: map[string]any{"message_thread_id": 7},
	})
	if err == nil || calls != 1 {
		t.Fatalf("unavailable topic: calls=%d err=%v", calls, err)
	}
}

func TestTelegramLegacyFinalPreservesEventTopic(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := recordTelegramJSONCall(t, r)
		if call.payload["message_thread_id"] != float64(7) {
			t.Errorf("topic lost: %#v", call.payload)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1, "message_thread_id": 7}})
	}))
	defer server.Close()
	body := "fallback notice"
	_, err := (Dispatcher{TelegramSender: newTestTelegramSender(t, server.URL)}).Dispatch(context.Background(),
		contracts.OutboundMessage{Channel: "final", Target: "unused", Body: &body},
		contracts.TaskEnvelope{ReplyChannel: contracts.ReplyChannel{Type: "telegram", Target: "-100", Metadata: map[string]any{"message_thread_id": 7}}})
	if err != nil {
		t.Fatal(err)
	}
}
