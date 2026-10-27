package dispatch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/zulip"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestZulipDispatchUsesExplicitTopicAndFailsWhenUnavailable(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		r.ParseForm()
		if r.Form.Get("to") != "42" || r.Form.Get("topic") != "PR #3" || r.Form.Get("content") != "**Done**" {
			t.Fatalf("wrong destination or content: %v", r.Form)
		}
		fmt.Fprint(w, `{"result":"success","id":100}`)
	}))
	defer server.Close()
	sender, err := zulip.NewClient(zulip.Config{Site: server.URL, Email: "bot@example.com", APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := Dispatcher{ZulipSender: sender}
	body := "**Done**"
	target := (zulip.Target{Site: server.URL, ChannelID: 42, Topic: "PR #3"}).String()
	// An explicit Zulip destination works even for a GitHub-origin task.
	envelope := contracts.TaskEnvelope{ReplyChannel: contracts.ReplyChannel{Type: "github", Target: "https://github.com/example/repo/pull/3"}}
	message := contracts.OutboundMessage{Channel: "zulip", Target: target, Body: &body}
	got, err := dispatcher.Dispatch(context.Background(), message, envelope)
	if err != nil || got == nil || got.Target != target || calls != 1 {
		t.Fatal(got, err, calls)
	}
	message.Channel = "final"
	envelope.ReplyChannel = contracts.ReplyChannel{Type: "zulip", Target: target}
	if _, err = dispatcher.Dispatch(context.Background(), message, envelope); err != nil || calls != 2 {
		t.Fatal(err, calls)
	}
	dispatcher.ZulipSender = nil
	if _, err = dispatcher.Dispatch(context.Background(), message, envelope); err == nil {
		t.Fatal("silently accepted unconfigured Zulip")
	}
	dispatcher.ZulipSender = sender
	message.Attachment = &contracts.AttachmentRef{URI: "file:///tmp/attachment"}
	if _, err = dispatcher.Dispatch(context.Background(), message, envelope); err == nil || calls != 2 {
		t.Fatal("silently ignored attachment")
	}
}
