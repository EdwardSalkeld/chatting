package auxiliary

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const (
	Source           = "webhook"
	ReplyChannelType = "webhook"
)

type Broker interface {
	PickupJSON(ctx context.Context, queueName string, timeoutSeconds int, waitSeconds int) (*bbmb.PickedMessage, error)
	Ack(ctx context.Context, queueName string, guid string) error
}

type Connector struct {
	broker      Broker
	queueName   string
	replyTarget string
	contextRefs []string
	prompt      *contracts.PromptContext

	pendingByEnvelopeID map[string]string
	pendingEnvelopes    map[string]contracts.TaskEnvelope
}

func New(broker Broker, queueName string, contextRefs []string, prompt contracts.PromptContext) (*Connector, error) {
	if broker == nil {
		return nil, errors.New("broker is required")
	}
	if queueName == "" {
		return nil, errors.New("queue name is required")
	}
	var promptContext *contracts.PromptContext
	if prompt.HasContent() {
		promptCopy := prompt
		promptContext = &promptCopy
	}
	return &Connector{
		broker:              broker,
		queueName:           queueName,
		replyTarget:         queueName,
		contextRefs:         append([]string{}, contextRefs...),
		prompt:              promptContext,
		pendingByEnvelopeID: map[string]string{},
		pendingEnvelopes:    map[string]contracts.TaskEnvelope{},
	}, nil
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	envelopes := make([]contracts.TaskEnvelope, 0, len(connector.pendingEnvelopes))
	for _, envelope := range connector.pendingEnvelopes {
		envelopes = append(envelopes, envelope)
	}
	for {
		picked, err := connector.broker.PickupJSON(ctx, connector.queueName, 0, 0)
		if err != nil {
			return envelopes, err
		}
		if picked == nil {
			return envelopes, nil
		}
		raw, err := json.Marshal(picked.Payload)
		if err != nil {
			return envelopes, err
		}
		message, err := contracts.DecodeAuxiliaryIngressQueueMessage(raw)
		if err != nil {
			return envelopes, err
		}
		envelope, err := connector.envelopeFromMessage(message)
		if err != nil {
			return envelopes, err
		}
		if _, exists := connector.pendingEnvelopes[envelope.ID]; exists {
			continue
		}
		connector.pendingByEnvelopeID[envelope.ID] = picked.GUID
		connector.pendingEnvelopes[envelope.ID] = envelope
		envelopes = append(envelopes, envelope)
	}
}

func (connector *Connector) AckEnvelope(ctx context.Context, envelopeID string) error {
	guid, ok := connector.pendingByEnvelopeID[envelopeID]
	delete(connector.pendingByEnvelopeID, envelopeID)
	delete(connector.pendingEnvelopes, envelopeID)
	if !ok {
		return nil
	}
	return connector.broker.Ack(ctx, connector.queueName, guid)
}

func (connector *Connector) envelopeFromMessage(message contracts.AuxiliaryIngressQueueMessage) (contracts.TaskEnvelope, error) {
	content, err := renderBodyContent(message.Body)
	if err != nil {
		return contracts.TaskEnvelope{}, err
	}
	envelopeID := "webhook:" + message.EventID
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            envelopeID,
		Source:        Source,
		ReceivedAt:    message.ReceivedAt,
		Actor:         nil,
		Content:       content,
		Attachments:   []contracts.AttachmentRef{},
		ContextRefs:   append([]string{}, connector.contextRefs...),
		PromptContext: connector.prompt,
		ReplyChannel: contracts.ReplyChannel{
			Type:   ReplyChannelType,
			Target: connector.replyTarget,
		},
		DedupeKey: envelopeID,
	}, nil
}

func renderBodyContent(body any) (string, error) {
	switch body.(type) {
	case map[string]any, []any:
		raw, err := json.MarshalIndent(body, "", "  ")
		if err != nil {
			return "", err
		}
		return string(raw), nil
	default:
		raw, err := json.Marshal(body)
		if err != nil {
			return "", err
		}
		return string(raw), nil
	}
}
