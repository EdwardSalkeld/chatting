package webhooks

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

func TestNotificationCandidateTrustAndTagBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, authorType, sender, body string
		watched, candidate             bool
	}{
		{"bot update", "Bot", "builder[bot]", "Build done", true, true},
		{"human comment", "User", "builder[bot]", "Build done", true, false},
		{"human edited bot comment", "Bot", "alice", "Build done", true, false},
		{"explicit mention", "Bot", "builder[bot]", "@custom-login investigate", true, false},
		{"new watch", "Bot", "builder[bot]", "@custom-login investigate", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "handler.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			f := &Filter{Store: store, Login: "custom-login", DeliveryEnabled: true}
			if tc.watched {
				seed := []byte(`{"action":"created","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"issue":{"number":1},"comment":{"id":9,"body":"@custom-login inspect"}}`)
				if _, _, err := ReceiveGitHub(ctx, store, f, seed, "issue_comment", "seed", time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			body, _ := json.Marshal(map[string]any{
				"action": "created", "repository": map[string]any{"full_name": "o/r"},
				"sender": map[string]any{"login": tc.sender}, "issue": map[string]any{"number": 1},
				"comment": map[string]any{"id": 10, "body": tc.body, "updated_at": "2026-11-03T10:00:00Z",
					"user": map[string]any{"login": "builder[bot]", "type": tc.authorType}},
			})
			_, task, err := ReceiveGitHub(ctx, store, f, body, "issue_comment", "test", time.Now())
			if err != nil || task == nil {
				t.Fatalf("task=%v err=%v", task, err)
			}
			_, ok := task.Envelope.ReplyChannel.Metadata["notification_candidate"]
			if ok != tc.candidate {
				t.Fatalf("candidate=%v want=%v", ok, tc.candidate)
			}
			pending, err := store.PendingGitHub(ctx)
			if err != nil || len(pending) == 0 {
				t.Fatal(err)
			}
			found := false
			for _, persisted := range pending {
				if persisted.ID == task.Envelope.ID {
					_, candidate := persisted.ReplyChannel.Metadata["notification_candidate"]
					if candidate != tc.candidate {
						t.Fatal("pending storage lost candidate")
					}
					found = true
				}
			}
			if !found {
				t.Fatal("candidate task not pending")
			}
			// Broker-bound metadata also survives work-item assignment.
			assigned, err := store.AssignTask(ctx, *task)
			if err != nil {
				t.Fatal(err)
			}
			_, ok = assigned.Envelope.ReplyChannel.Metadata["notification_candidate"]
			if ok != tc.candidate {
				t.Fatal("assignment lost candidate")
			}
		})
	}
}
