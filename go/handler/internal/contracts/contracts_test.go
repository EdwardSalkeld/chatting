package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestTaskQueueMessageRoundTrip(t *testing.T) {
	name := "msg1.txt"
	actor := "alice@example.com"
	message := NewTaskQueueMessage(TaskEnvelope{
		SchemaVersion: SchemaVersion,
		ID:            "email:1",
		Source:        "email",
		ReceivedAt:    mustTimestamp(t, "2026-03-06T11:00:00Z"),
		Actor:         &actor,
		Content:       "hello",
		Attachments: []AttachmentRef{{
			URI:  "file://inbox/msg1.txt",
			Name: &name,
		}},
		ContextRefs: []string{"repo:/home/edward/develop/chatting"},
		PromptContext: &PromptContext{
			GlobalInstructions:       []string{"Keep replies concise."},
			ReplyChannelInstructions: []string{"Use email formatting."},
		},
		ReplyChannel: ReplyChannel{
			Type:     "email",
			Target:   "alice@example.com",
			Metadata: map[string]any{"thread_id": "abc"},
		},
		DedupeKey: "email:1",
	}, "trace:email:1", mustTime(t, "2026-03-06T11:00:01Z"))

	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := DecodeTaskQueueMessage(raw)
	if err != nil {
		t.Fatal(err)
	}

	if parsed.TraceID != "trace:email:1" {
		t.Fatalf("TraceID = %q", parsed.TraceID)
	}
	if parsed.TaskID != "task:email:1" {
		t.Fatalf("TaskID = %q", parsed.TaskID)
	}
	if parsed.Envelope.Attachments[0].URI != "file://inbox/msg1.txt" {
		t.Fatalf("attachment URI = %q", parsed.Envelope.Attachments[0].URI)
	}
	if got := parsed.Envelope.PromptContext.AssembledInstructions(); strings.Join(got, "|") != "Keep replies concise.|Use email formatting." {
		t.Fatalf("assembled instructions = %#v", got)
	}
}

func TestTaskQueueMessageRejectsWrongType(t *testing.T) {
	_, err := DecodeTaskQueueMessage([]byte(`{"message_type":"chatting.egress.v1"}`))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestEgressQueueMessageRoundTrip(t *testing.T) {
	body := "hello"
	sequence := 0
	message := EgressQueueMessage{
		SchemaVersion: SchemaVersion,
		MessageType:   EgressMessageTypeV2,
		TaskID:        "task:email:1",
		EnvelopeID:    "email:1",
		TraceID:       "trace:email:1",
		EventID:       "evt:task:email:1:0",
		EventKind:     "message",
		Sequence:      &sequence,
		EmittedAt:     mustTimestamp(t, "2026-03-06T11:01:00Z"),
		Message: OutboundMessage{
			Channel:  "email",
			Target:   "alice@example.com",
			Body:     &body,
			Metadata: map[string]any{"thread_id": "abc"},
		},
	}

	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := DecodeEgressQueueMessage(raw)
	if err != nil {
		t.Fatal(err)
	}

	if parsed.TaskID != message.TaskID {
		t.Fatalf("TaskID = %q", parsed.TaskID)
	}
	if parsed.Sequence == nil || *parsed.Sequence != 0 {
		t.Fatalf("Sequence = %#v", parsed.Sequence)
	}
	if parsed.Message.Target != "alice@example.com" {
		t.Fatalf("target = %q", parsed.Message.Target)
	}
}

func TestEgressQueueMessageAllowsUnsequencedIncremental(t *testing.T) {
	body := "working on it"
	message := EgressQueueMessage{
		SchemaVersion: SchemaVersion,
		MessageType:   EgressMessageTypeV2,
		TaskID:        "task:email:2",
		EnvelopeID:    "email:2",
		TraceID:       "trace:email:2",
		EventID:       "evt:task:email:2:adhoc",
		EventKind:     "incremental",
		EmittedAt:     mustTimestamp(t, "2026-03-06T11:01:00Z"),
		Message: OutboundMessage{
			Channel: "email",
			Target:  "alice@example.com",
			Body:    &body,
		},
	}

	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sequence") {
		t.Fatalf("unsequenced incremental included sequence: %s", raw)
	}
	if _, err := DecodeEgressQueueMessage(raw); err != nil {
		t.Fatal(err)
	}
}

func TestEgressQueueMessageRequiresSequenceForCompletion(t *testing.T) {
	body := "done"
	message := EgressQueueMessage{
		SchemaVersion: SchemaVersion,
		MessageType:   EgressMessageTypeV2,
		TaskID:        "task:email:2",
		EnvelopeID:    "email:2",
		TraceID:       "trace:email:2",
		EventID:       "evt:task:email:2:completion",
		EventKind:     "completion",
		EmittedAt:     mustTimestamp(t, "2026-03-06T11:01:00Z"),
		Message: OutboundMessage{
			Channel: "internal",
			Target:  "task",
			Body:    &body,
		},
	}

	if err := message.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestAuxiliaryIngressQueueMessageRoundTrip(t *testing.T) {
	message := AuxiliaryIngressQueueMessage{
		SchemaVersion: SchemaVersion,
		MessageType:   AuxiliaryIngressMessageType,
		EventID:       "aux:1",
		ReceivedAt:    mustTimestamp(t, "2026-04-01T12:00:00Z"),
		Body: map[string]any{
			"hello":  "world",
			"nested": []any{float64(1), float64(2)},
		},
	}

	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := DecodeAuxiliaryIngressQueueMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.EventID != "aux:1" {
		t.Fatalf("EventID = %q", parsed.EventID)
	}
}

func TestGoldenContractFixtures(t *testing.T) {
	tests := []struct {
		name   string
		decode func([]byte) ([]byte, error)
	}{
		{
			name: "task.v1.json",
			decode: func(raw []byte) ([]byte, error) {
				parsed, err := DecodeTaskQueueMessage(raw)
				if err != nil {
					return nil, err
				}
				return json.Marshal(parsed)
			},
		},
		{
			name: "egress.v2.sequenced.json",
			decode: func(raw []byte) ([]byte, error) {
				parsed, err := DecodeEgressQueueMessage(raw)
				if err != nil {
					return nil, err
				}
				if parsed.Sequence == nil || *parsed.Sequence != 0 {
					t.Fatalf("%s sequence = %#v", "egress.v2.sequenced.json", parsed.Sequence)
				}
				return json.Marshal(parsed)
			},
		},
		{
			name: "egress.v2.incremental.json",
			decode: func(raw []byte) ([]byte, error) {
				parsed, err := DecodeEgressQueueMessage(raw)
				if err != nil {
					return nil, err
				}
				if parsed.Sequence != nil {
					t.Fatalf("%s sequence = %#v", "egress.v2.incremental.json", parsed.Sequence)
				}
				return json.Marshal(parsed)
			},
		},
		{
			name: "auxiliary_ingress.v1.json",
			decode: func(raw []byte) ([]byte, error) {
				parsed, err := DecodeAuxiliaryIngressQueueMessage(raw)
				if err != nil {
					return nil, err
				}
				return json.Marshal(parsed)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := readFixture(t, test.name)
			encoded, err := test.decode(raw)
			if err != nil {
				t.Fatal(err)
			}

			var original map[string]any
			if err := json.Unmarshal(raw, &original); err != nil {
				t.Fatal(err)
			}
			var roundTripped map[string]any
			if err := json.Unmarshal(encoded, &roundTripped); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(roundTripped, original) {
				t.Fatalf("round trip changed fixture\noriginal: %#v\nroundTripped: %#v", original, roundTripped)
			}
		})
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate current test file")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "..", "tests", "fixtures", "contracts", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mustTimestamp(t *testing.T, raw string) Timestamp {
	t.Helper()
	return NewTimestamp(mustTime(t, raw))
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
