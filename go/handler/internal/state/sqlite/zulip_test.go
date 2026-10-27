package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/zulip"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestZulipCursorPersistsAndNeverRegresses(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := store.GetZulipCursor(ctx, "server|bot")
	if err != nil || cursor != nil {
		t.Fatal(cursor, err)
	}
	if err = store.SetZulipCursor(ctx, "server|bot", 99); err != nil {
		t.Fatal(err)
	}
	store.Close()
	store, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.SetZulipCursor(ctx, "server|bot", 50); err != nil {
		t.Fatal(err)
	}
	cursor, err = store.GetZulipCursor(ctx, "server|bot")
	if err != nil || cursor == nil || *cursor != 99 {
		t.Fatal(cursor, err)
	}
	cursor, err = store.GetZulipCursor(ctx, "another|bot")
	if err != nil || cursor != nil {
		t.Fatal(cursor, err)
	}
}

func TestZulipTopicsUseSeparateWorkspacesAndReturnGitHubEvents(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	base := testTaskMessage(t)
	base.Envelope.Source = "im"
	assign := func(id, target, site string) contracts.TaskQueueMessage {
		var address zulip.Target
		if err := json.Unmarshal([]byte(target), &address); err != nil {
			t.Fatal(err)
		}
		address.Site = site
		target = address.String()
		task := base
		task.TaskID = id
		task.Envelope.ID = id
		task.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "zulip", Target: target, Metadata: map[string]any{"zulip_site": site, "topic": "PR1", "message_id": 77}}
		result, err := store.AssignTask(ctx, task)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := assign("one", `{"channel_id":12,"topic":"PR1"}`, "https://one.example")
	same := assign("two", `{"topic":"pr1","channel_id":12}`, "https://one.example")
	other := assign("three", `{"channel_id":12,"topic":"PR2"}`, "https://one.example")
	otherSite := assign("four", `{"channel_id":12,"topic":"PR1"}`, "https://two.example")
	if first.WorkItemID != same.WorkItemID || first.WorkItemID == other.WorkItemID || first.WorkItemID == otherSite.WorkItemID {
		t.Fatal("topic/site isolation failed")
	}
	if err := store.RegisterPR(ctx, first.TaskID, "https://github.com/example/repo/pull/42"); err != nil {
		t.Fatal(err)
	}
	task := base
	task.TaskID = "github"
	task.Envelope.Source = "webhook"
	task.Envelope.ReplyChannel = contracts.ReplyChannel{Type: "github", Target: "https://github.com/example/repo/pull/42", Metadata: map[string]any{"github_subject": "https://github.com/example/repo/pull/42"}}
	got, err := store.AssignTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkItemID != first.WorkItemID || len(got.Envelope.ConversationChannels) != 1 {
		t.Fatalf("lost Zulip context: %#v", got)
	}
	origin := got.Envelope.ConversationChannels[0]
	if origin.Type != "zulip" || origin.Target != first.Envelope.ReplyChannel.Target || origin.Metadata["zulip_site"] != "https://one.example" || origin.Metadata["message_id"] != nil {
		t.Fatalf("bad context: %#v", origin)
	}
}
