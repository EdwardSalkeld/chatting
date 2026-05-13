package auxiliary

import (
	"context"
	"reflect"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestPollDrainsAuxiliaryIngressQueueIntoWebhookEnvelope(t *testing.T) {
	broker := &fakeBroker{
		picked: []*bbmb.PickedMessage{
			{
				GUID: "guid-1",
				Payload: map[string]any{
					"schema_version": "1.0",
					"message_type":   "chatting.auxiliary_ingress.v1",
					"event_id":       "aux:1",
					"received_at":    "2026-04-01T12:00:00Z",
					"body": map[string]any{
						"hello":  "world",
						"nested": []any{float64(1), float64(2)},
					},
				},
			},
		},
	}
	connector, err := New(
		broker,
		"chatting.auxiliary-ingress.v1",
		[]string{"repo:/workspace/chatting"},
		contracts.PromptContext{GlobalInstructions: []string{"Keep it terse."}},
	)
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
	envelope := envelopes[0]
	if envelope.ID != "webhook:aux:1" {
		t.Fatalf("ID = %q", envelope.ID)
	}
	if envelope.Source != Source {
		t.Fatalf("Source = %q", envelope.Source)
	}
	if envelope.ReplyChannel.Type != ReplyChannelType || envelope.ReplyChannel.Target != "chatting.auxiliary-ingress.v1" {
		t.Fatalf("ReplyChannel = %#v", envelope.ReplyChannel)
	}
	if !reflect.DeepEqual(envelope.ContextRefs, []string{"repo:/workspace/chatting"}) {
		t.Fatalf("ContextRefs = %#v", envelope.ContextRefs)
	}
	if envelope.PromptContext == nil || !reflect.DeepEqual(envelope.PromptContext.GlobalInstructions, []string{"Keep it terse."}) {
		t.Fatalf("PromptContext = %#v", envelope.PromptContext)
	}
	expectedContent := "{\n  \"hello\": \"world\",\n  \"nested\": [\n    1,\n    2\n  ]\n}"
	if envelope.Content != expectedContent {
		t.Fatalf("Content = %q, want %q", envelope.Content, expectedContent)
	}
	if len(broker.acks) != 0 {
		t.Fatalf("acks before AckEnvelope = %#v", broker.acks)
	}

	if err := connector.AckEnvelope(context.Background(), envelope.ID); err != nil {
		t.Fatal(err)
	}
	if got, want := broker.acks, []ackCall{{queue: "chatting.auxiliary-ingress.v1", guid: "guid-1"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acks = %#v, want %#v", got, want)
	}
}

func TestPollKeepsPendingEnvelopeUntilAcked(t *testing.T) {
	broker := &fakeBroker{
		picked: []*bbmb.PickedMessage{
			{
				GUID: "guid-1",
				Payload: map[string]any{
					"schema_version": "1.0",
					"message_type":   "chatting.auxiliary_ingress.v1",
					"event_id":       "aux:1",
					"received_at":    "2026-04-01T12:00:00Z",
					"body":           "hello",
				},
			},
		},
	}
	connector, err := New(broker, "aux", nil, contracts.PromptContext{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("first = %#v, second = %#v", first, second)
	}
	if second[0].ID != first[0].ID {
		t.Fatalf("pending ID = %q, want %q", second[0].ID, first[0].ID)
	}
}

type fakeBroker struct {
	picked []*bbmb.PickedMessage
	acks   []ackCall
}

type ackCall struct {
	queue string
	guid  string
}

func (broker *fakeBroker) PickupJSON(ctx context.Context, queueName string, timeoutSeconds int, waitSeconds int) (*bbmb.PickedMessage, error) {
	if timeoutSeconds != 0 || waitSeconds != 0 {
		panic("auxiliary connector must use nonblocking pickup")
	}
	if len(broker.picked) == 0 {
		return nil, nil
	}
	picked := broker.picked[0]
	broker.picked = broker.picked[1:]
	return picked, nil
}

func (broker *fakeBroker) Ack(ctx context.Context, queueName string, guid string) error {
	broker.acks = append(broker.acks, ackCall{queue: queueName, guid: guid})
	return nil
}
