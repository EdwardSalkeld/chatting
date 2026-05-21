package telegram

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestPollNormalizesAllowedMessageAndObservesChatBeforeAllowlist(t *testing.T) {
	var requestedURL string
	client := &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) {
		requestedURL = req.URL.String()
		return jsonResponse(`{
			"ok": true,
			"result": [
				{
					"update_id": 1001,
					"message": {
						"message_id": 55,
						"date": 1779345600,
						"chat": {"id": 12345, "type": "private", "username": "edward"},
						"from": {"id": 7, "username": "sender"},
						"text": "hello from telegram"
					}
				},
				{
					"update_id": 1002,
					"message": {
						"message_id": 56,
						"date": 1779345660,
						"chat": {"id": 99999, "type": "private", "username": "other"},
						"from": {"id": 8},
						"text": "ignored"
					}
				}
			]
		}`), nil
	}}
	observed := []ChatObservation{}
	connector, err := New(Config{
		BotToken:           "token",
		APIBaseURL:         "https://telegram.example.test",
		PollTimeoutSeconds: 12,
		AllowedChatIDs:     []string{"12345"},
		ContextRefs:        []string{"repo:/workspace/chatting"},
		PromptContext: contracts.PromptContext{
			GlobalInstructions:       []string{"global"},
			ReplyChannelInstructions: []string{"telegram"},
		},
		HTTPClient: client,
		ObserveChat: func(ctx context.Context, observation ChatObservation) error {
			observed = append(observed, observation)
			return nil
		},
		Now: func() time.Time { return mustTime(t, "2026-05-21T06:40:00Z") },
	})
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(requestedURL, "/bottoken/getUpdates?") || !strings.Contains(requestedURL, "timeout=12") {
		t.Fatalf("requested URL = %q", requestedURL)
	}
	if len(envelopes) != 1 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	envelope := envelopes[0]
	if envelope.ID != "telegram:1001" || envelope.Source != "im" || envelope.DedupeKey != "telegram:1001" {
		t.Fatalf("envelope identity = %#v", envelope)
	}
	if envelope.Content != "hello from telegram" {
		t.Fatalf("content = %q", envelope.Content)
	}
	if envelope.ReplyChannel.Type != "telegram" || envelope.ReplyChannel.Target != "12345" {
		t.Fatalf("reply channel = %#v", envelope.ReplyChannel)
	}
	if envelope.ReplyChannel.Metadata["message_id"] != float64(55) && envelope.ReplyChannel.Metadata["message_id"] != int64(55) {
		t.Fatalf("metadata = %#v", envelope.ReplyChannel.Metadata)
	}
	if deref(envelope.Actor) != "7:sender" {
		t.Fatalf("actor = %#v", envelope.Actor)
	}
	if !reflect.DeepEqual(envelope.ContextRefs, []string{"repo:/workspace/chatting"}) {
		t.Fatalf("context refs = %#v", envelope.ContextRefs)
	}
	if envelope.PromptContext == nil || !reflect.DeepEqual(envelope.PromptContext.AssembledInstructions(), []string{"global", "telegram"}) {
		t.Fatalf("prompt context = %#v", envelope.PromptContext)
	}
	if len(observed) != 2 {
		t.Fatalf("observed chats = %#v", observed)
	}
	if observed[0].ChatID != "12345" || deref(observed[0].Username) != "edward" || observed[0].UpdateKind != "message" {
		t.Fatalf("first observation = %#v", observed[0])
	}
	if observed[1].ChatID != "99999" {
		t.Fatalf("disallowed chat was not observed before allowlist: %#v", observed)
	}

	requestedURL = ""
	client.do = func(req *http.Request) (*http.Response, error) {
		requestedURL = req.URL.String()
		return jsonResponse(`{"ok": true, "result": []}`), nil
	}
	if _, err := connector.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(requestedURL, "offset=1003") {
		t.Fatalf("second requested URL = %q", requestedURL)
	}
}

func TestPollNormalizesAllowedChannelPostAndMyChatMemberObservation(t *testing.T) {
	client := &fakeHTTPClient{do: func(req *http.Request) (*http.Response, error) {
		return jsonResponse(`{
			"ok": true,
			"result": [
				{
					"update_id": 2001,
					"channel_post": {
						"message_id": 9,
						"date": 1779345600,
						"chat": {"id": -100999, "type": "channel", "title": "Deploys", "username": "deploys"},
						"sender_chat": {"id": -100999, "type": "channel", "title": "Deploys"},
						"text": "release shipped"
					}
				},
				{
					"update_id": 2002,
					"my_chat_member": {
						"chat": {"id": -100123, "type": "supergroup", "title": "Test Group"}
					}
				}
			]
		}`), nil
	}}
	observed := []ChatObservation{}
	connector, err := New(Config{
		BotToken:          "token",
		AllowedChannelIDs: []string{"-100999"},
		HTTPClient:        client,
		ObserveChat: func(ctx context.Context, observation ChatObservation) error {
			observed = append(observed, observation)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(envelopes) != 1 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	if envelopes[0].ReplyChannel.Target != "-100999" || envelopes[0].Content != "release shipped" {
		t.Fatalf("channel envelope = %#v", envelopes[0])
	}
	if len(observed) != 2 {
		t.Fatalf("observed = %#v", observed)
	}
	if observed[0].UpdateKind != "channel_post" || observed[0].ChatID != "-100999" {
		t.Fatalf("channel observation = %#v", observed[0])
	}
	if observed[1].UpdateKind != "my_chat_member" || observed[1].ChatID != "-100123" || deref(observed[1].Title) != "Test Group" {
		t.Fatalf("membership observation = %#v", observed[1])
	}
}

type fakeHTTPClient struct {
	do func(*http.Request) (*http.Response, error)
}

func (client *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return client.do(req)
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
