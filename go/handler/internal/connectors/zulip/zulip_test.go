package zulip

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type memoryCursor struct{ value *int64 }

func (store *memoryCursor) GetZulipCursor(context.Context, string) (*int64, error) {
	return store.value, nil
}
func (store *memoryCursor) SetZulipCursor(_ context.Context, _ string, id int64) error {
	store.value = &id
	return nil
}

func TestPollingRestartsAndAcknowledgesOnlyAcceptedMessages(t *testing.T) {
	ctx := context.Background()
	var messages []message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, key, ok := r.BasicAuth()
		if !ok || user != "bot@example.com" || key != "secret" {
			t.Error("missing credentials")
		}
		if r.URL.Path != "/api/v1/messages" || r.URL.Query().Get("apply_markdown") != "false" {
			t.Error("unexpected request")
		}
		json.NewEncoder(w).Encode(map[string]any{"result": "success", "messages": messages})
	}))
	defer server.Close()
	config := Config{Site: server.URL, Email: "bot@example.com", APIKey: "secret", AllowedChannelIDs: []string{"12"}, AllowPrivateMessages: true}
	cursor := &memoryCursor{}
	connector, err := New(config, cursor)
	if err != nil {
		t.Fatal(err)
	}
	messages = []message{{ID: 10, Type: "stream", StreamID: 12, Content: "old history"}}
	got, err := connector.Poll(ctx)
	if err != nil || len(got) != 0 || *cursor.value != 10 {
		t.Fatalf("initial baseline: %v %v", got, err)
	}
	messages = []message{
		{ID: 11, Type: "stream", StreamID: 12, Subject: "PR #1 / café", Content: "**hello**", SenderEmail: "human@example.com", Timestamp: 123},
		{ID: 12, Type: "stream", StreamID: 12, Subject: "PR #2", Content: "second", SenderEmail: "human@example.com", Timestamp: 124},
		{ID: 13, Type: "stream", StreamID: 12, Content: "own reply", SenderEmail: config.Email},
		{ID: 14, Type: "stream", StreamID: 99, Content: "not allowed"},
	}
	got, err = connector.Poll(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("poll: %v %v", got, err)
	}
	if *cursor.value != 10 {
		t.Fatal("cursor advanced before acceptance")
	}
	target, err := ParseTarget(got[0].ReplyChannel.Target)
	if err != nil || target.Topic != "PR #1 / café" || target.ChannelID != 12 || got[0].Content != "**hello**" {
		t.Fatalf("bad target/content: %#v %v", got[0], err)
	}
	if err := got[0].Validate(); err != nil {
		t.Fatal(err)
	}
	if err = connector.AckEnvelope(ctx, got[0].ID); err != nil {
		t.Fatal(err)
	}
	if *cursor.value != 11 {
		t.Fatal("skipped unacknowledged message")
	}
	// Simulate a restart after only the first task reached the broker.
	connector, err = New(config, cursor)
	if err != nil {
		t.Fatal(err)
	}
	got, err = connector.Poll(ctx)
	if err != nil || len(got) != 1 {
		t.Fatalf("restart: %v %v", got, err)
	}
	if err = connector.AckEnvelope(ctx, got[0].ID); err != nil || *cursor.value != 14 {
		t.Fatalf("final ack: %v", err)
	}
	// Other participants, not the bot, form a stable private conversation key.
	msg := message{ID: 15, Type: "private", Content: "dm", RawRecipient: jsonRecipient{users: []recipient{{ID: 7, Email: "person@example.com"}, {ID: 8, Email: config.Email}}}}
	envelope, ok := connector.normalize(msg)
	if !ok || envelope.ReplyChannel.Target != (Target{Site: server.URL, UserIDs: []int64{7}}).String() {
		t.Fatalf("private target: %#v", envelope)
	}
	config.AllowPrivateMessages = false
	connector, _ = New(config, cursor)
	if _, ok := connector.normalize(msg); ok {
		t.Fatal("private messages enabled without consent")
	}
}

func TestRepliesPreserveTopicAndMarkdown(t *testing.T) {
	body := "**hello**\n```python\nprint(r'\\n')\n```"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Error("wrong method")
		}
		r.ParseForm()
		if r.Form.Get("content") != body {
			t.Errorf("modified body: %q", r.Form.Get("content"))
		}
		if r.Form.Get("type") == "stream" && (r.Form.Get("to") != "12" || r.Form.Get("topic") != "PR / café #1") {
			t.Error("wrong topic")
		}
		if r.Form.Get("type") == "direct" && r.Form.Get("to") != "[7,9]" {
			t.Error("wrong recipients")
		}
		fmt.Fprint(w, `{"result":"success","id":99}`)
	}))
	defer server.Close()
	client, err := NewClient(Config{Site: server.URL, Email: "bot@example.com", APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{(Target{Site: server.URL, ChannelID: 12, Topic: "PR / café #1"}).String(), (Target{Site: server.URL, UserIDs: []int64{9, 7}}).String()} {
		if err := client.Send(context.Background(), target, body); err != nil {
			t.Fatal(err)
		}
	}
	for _, target := range []string{`{}`, (Target{Site: server.URL, ChannelID: -1}).String(), (Target{Site: server.URL, UserIDs: []int64{0}}).String(), (Target{Site: server.URL, ChannelID: 12, UserIDs: []int64{7}}).String(), (Target{Site: "https://wrong.example", ChannelID: 12}).String()} {
		if err := client.Send(context.Background(), target, body); err == nil {
			t.Fatalf("accepted %s", target)
		}
	}
}

func TestAPIErrorDoesNotExposeCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "secret", http.StatusUnauthorized) }))
	defer server.Close()
	client, _ := NewClient(Config{Site: server.URL, Email: "bot@example.com", APIKey: "secret"})
	err := client.Send(context.Background(), (Target{Site: server.URL, ChannelID: 12, Topic: "test"}).String(), "hi")
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
	for _, site := range []string{"", "https://user:password@example.com", "https://example.com?key=secret", "file:///tmp/zulip"} {
		if _, err := NewClient(Config{Site: site, Email: "bot@example.com", APIKey: "secret"}); err == nil {
			t.Fatalf("accepted %s", site)
		}
	}
}
