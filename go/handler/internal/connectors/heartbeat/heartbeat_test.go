package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestPollEmitsInternalHeartbeatWithUniqueIDs(t *testing.T) {
	now := mustTime(t, "2026-03-09T12:00:00Z")
	connector := New(func() time.Time { return now })

	first, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("poll results = %#v %#v", first, second)
	}
	if first[0].Source != "internal" {
		t.Fatalf("source = %q", first[0].Source)
	}
	if first[0].Actor == nil || *first[0].Actor != "message-handler" {
		t.Fatalf("actor = %#v", first[0].Actor)
	}
	if first[0].ReplyChannel.Type != "internal" || first[0].ReplyChannel.Target != "heartbeat" {
		t.Fatalf("reply channel = %#v", first[0].ReplyChannel)
	}
	if first[0].ID == second[0].ID {
		t.Fatalf("heartbeat IDs should be unique: %q", first[0].ID)
	}
	if first[0].DedupeKey != first[0].ID || second[0].DedupeKey != second[0].ID {
		t.Fatalf("dedupe keys = %q %q", first[0].DedupeKey, second[0].DedupeKey)
	}
	if first[0].ID != "internal-heartbeat:2026-03-09T12:00:00+00:00:1" {
		t.Fatalf("first ID = %q", first[0].ID)
	}
}

func TestIsLogPongRecognizesOnlyInternalHeartbeatLogTarget(t *testing.T) {
	envelope := BuildEnvelope(1, mustTime(t, "2026-03-09T12:00:00Z"))
	body := "{}"

	if !IsLogPong(testMessage("log", "heartbeat", body), envelope) {
		t.Fatal("expected heartbeat log pong")
	}
	if IsLogPong(testMessage("log", "ops", body), envelope) {
		t.Fatal("ops log should not be a heartbeat pong")
	}
	envelope.ReplyChannel.Type = "log"
	if IsLogPong(testMessage("log", "heartbeat", body), envelope) {
		t.Fatal("non-internal reply channel should not be a heartbeat pong")
	}
}

func testMessage(channel string, target string, body string) contracts.OutboundMessage {
	return contracts.OutboundMessage{Channel: channel, Target: target, Body: &body}
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
