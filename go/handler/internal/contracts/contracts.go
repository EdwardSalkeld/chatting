package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	SchemaVersion               = "1.0"
	TaskMessageType             = "chatting.task.v1"
	EgressMessageTypeV2         = "chatting.egress.v2"
	AuxiliaryIngressMessageType = "chatting.auxiliary_ingress.v1"
)

var validSources = map[string]bool{
	"cron":     true,
	"email":    true,
	"im":       true,
	"webhook":  true,
	"internal": true,
}

type Timestamp struct {
	time.Time
}

func NewTimestamp(value time.Time) Timestamp {
	return Timestamp{Time: value.UTC()}
}

func (value Timestamp) MarshalJSON() ([]byte, error) {
	if value.Time.IsZero() {
		return nil, errors.New("timestamp is required")
	}
	return json.Marshal(value.UTC().Format("2006-01-02T15:04:05Z"))
}

func (value *Timestamp) UnmarshalJSON(raw []byte) error {
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339, encoded)
	if err != nil {
		return err
	}
	value.Time = parsed.UTC()
	return nil
}

type AttachmentRef struct {
	URI  string  `json:"uri"`
	Name *string `json:"name"`
}

func (value AttachmentRef) Validate() error {
	if strings.TrimSpace(value.URI) == "" {
		return errors.New("uri is required")
	}
	if value.Name != nil && strings.TrimSpace(*value.Name) == "" {
		return errors.New("name is required")
	}
	return nil
}

type ReplyChannel struct {
	Type     string         `json:"type"`
	Target   string         `json:"target"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func (value ReplyChannel) Validate() error {
	if strings.TrimSpace(value.Type) == "" {
		return errors.New("reply_channel.type is required")
	}
	if strings.TrimSpace(value.Target) == "" {
		return errors.New("reply_channel.target is required")
	}
	return validateMetadata(value.Metadata, "reply_channel.metadata")
}

type PromptContext struct {
	GlobalInstructions       []string `json:"global_instructions"`
	SourceInstructions       []string `json:"source_instructions"`
	ReplyChannelInstructions []string `json:"reply_channel_instructions"`
	TaskInstructions         []string `json:"task_instructions"`
}

func (value PromptContext) Validate() error {
	if err := validateStringList(value.GlobalInstructions, "prompt_context.global_instructions"); err != nil {
		return err
	}
	if err := validateStringList(value.SourceInstructions, "prompt_context.source_instructions"); err != nil {
		return err
	}
	if err := validateStringList(value.ReplyChannelInstructions, "prompt_context.reply_channel_instructions"); err != nil {
		return err
	}
	return validateStringList(value.TaskInstructions, "prompt_context.task_instructions")
}

func (value PromptContext) HasContent() bool {
	return len(value.GlobalInstructions) > 0 ||
		len(value.SourceInstructions) > 0 ||
		len(value.ReplyChannelInstructions) > 0 ||
		len(value.TaskInstructions) > 0
}

func (value PromptContext) AssembledInstructions() []string {
	result := make([]string, 0,
		len(value.GlobalInstructions)+
			len(value.SourceInstructions)+
			len(value.ReplyChannelInstructions)+
			len(value.TaskInstructions),
	)
	result = append(result, value.GlobalInstructions...)
	result = append(result, value.SourceInstructions...)
	result = append(result, value.ReplyChannelInstructions...)
	result = append(result, value.TaskInstructions...)
	return result
}

func (value PromptContext) MarshalJSON() ([]byte, error) {
	type promptContextJSON struct {
		GlobalInstructions       []string `json:"global_instructions"`
		SourceInstructions       []string `json:"source_instructions"`
		ReplyChannelInstructions []string `json:"reply_channel_instructions"`
		TaskInstructions         []string `json:"task_instructions"`
		AssembledInstructions    []string `json:"assembled_instructions"`
	}
	return json.Marshal(promptContextJSON{
		GlobalInstructions:       nonNilStrings(value.GlobalInstructions),
		SourceInstructions:       nonNilStrings(value.SourceInstructions),
		ReplyChannelInstructions: nonNilStrings(value.ReplyChannelInstructions),
		TaskInstructions:         nonNilStrings(value.TaskInstructions),
		AssembledInstructions:    value.AssembledInstructions(),
	})
}

func (value *PromptContext) UnmarshalJSON(raw []byte) error {
	type promptContextJSON struct {
		GlobalInstructions       []string `json:"global_instructions"`
		SourceInstructions       []string `json:"source_instructions"`
		ReplyChannelInstructions []string `json:"reply_channel_instructions"`
		TaskInstructions         []string `json:"task_instructions"`
	}
	var decoded promptContextJSON
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*value = PromptContext{
		GlobalInstructions:       decoded.GlobalInstructions,
		SourceInstructions:       decoded.SourceInstructions,
		ReplyChannelInstructions: decoded.ReplyChannelInstructions,
		TaskInstructions:         decoded.TaskInstructions,
	}
	return value.Validate()
}

type TaskEnvelope struct {
	SchemaVersion string          `json:"schema_version"`
	ID            string          `json:"id"`
	Source        string          `json:"source"`
	ReceivedAt    Timestamp       `json:"received_at"`
	Actor         *string         `json:"actor"`
	Content       string          `json:"content"`
	Attachments   []AttachmentRef `json:"attachments"`
	ContextRefs   []string        `json:"context_refs"`
	PromptContext *PromptContext  `json:"prompt_context,omitempty"`
	ReplyChannel  ReplyChannel    `json:"reply_channel"`
	DedupeKey     string          `json:"dedupe_key"`
}

func (value TaskEnvelope) MarshalJSON() ([]byte, error) {
	type taskEnvelopeJSON struct {
		SchemaVersion string          `json:"schema_version"`
		ID            string          `json:"id"`
		Source        string          `json:"source"`
		ReceivedAt    Timestamp       `json:"received_at"`
		Actor         *string         `json:"actor"`
		Content       string          `json:"content"`
		Attachments   []AttachmentRef `json:"attachments"`
		ContextRefs   []string        `json:"context_refs"`
		PromptContext *PromptContext  `json:"prompt_context,omitempty"`
		ReplyChannel  ReplyChannel    `json:"reply_channel"`
		DedupeKey     string          `json:"dedupe_key"`
	}
	return json.Marshal(taskEnvelopeJSON{
		SchemaVersion: value.SchemaVersion,
		ID:            value.ID,
		Source:        value.Source,
		ReceivedAt:    value.ReceivedAt,
		Actor:         value.Actor,
		Content:       value.Content,
		Attachments:   nonNilAttachments(value.Attachments),
		ContextRefs:   nonNilStrings(value.ContextRefs),
		PromptContext: value.PromptContext,
		ReplyChannel:  value.ReplyChannel,
		DedupeKey:     value.DedupeKey,
	})
}

func (value TaskEnvelope) Validate() error {
	if value.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported_schema_version:%s", value.SchemaVersion)
	}
	if strings.TrimSpace(value.ID) == "" {
		return errors.New("id is required")
	}
	if !validSources[value.Source] {
		return errors.New("source is invalid")
	}
	if value.ReceivedAt.Time.IsZero() {
		return errors.New("received_at is required")
	}
	if strings.TrimSpace(value.Content) == "" {
		return errors.New("content is required")
	}
	for _, attachment := range value.Attachments {
		if err := attachment.Validate(); err != nil {
			return err
		}
	}
	if err := validateStringList(value.ContextRefs, "context_refs"); err != nil {
		return err
	}
	if value.PromptContext != nil {
		if err := value.PromptContext.Validate(); err != nil {
			return err
		}
	}
	if err := value.ReplyChannel.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(value.DedupeKey) == "" {
		return errors.New("dedupe_key is required")
	}
	return nil
}

type OutboundMessage struct {
	Channel    string         `json:"channel"`
	Target     string         `json:"target"`
	Body       *string        `json:"body,omitempty"`
	Attachment *AttachmentRef `json:"attachment,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

func (value OutboundMessage) Validate() error {
	if strings.TrimSpace(value.Channel) == "" {
		return errors.New("message.channel is required")
	}
	if strings.TrimSpace(value.Target) == "" {
		return errors.New("message.target is required")
	}
	if value.Body != nil && strings.TrimSpace(*value.Body) == "" {
		return errors.New("message.body is required")
	}
	if value.Attachment != nil {
		if err := value.Attachment.Validate(); err != nil {
			return err
		}
	}
	if value.Body == nil && value.Attachment == nil {
		return errors.New("message body or attachment is required")
	}
	return validateMetadata(value.Metadata, "message.metadata")
}

type TaskQueueMessage struct {
	SchemaVersion string       `json:"schema_version"`
	MessageType   string       `json:"message_type"`
	TraceID       string       `json:"trace_id"`
	TaskID        string       `json:"task_id"`
	EmittedAt     Timestamp    `json:"emitted_at"`
	Envelope      TaskEnvelope `json:"envelope"`
}

func NewTaskQueueMessage(envelope TaskEnvelope, traceID string, emittedAt time.Time) TaskQueueMessage {
	return TaskQueueMessage{
		SchemaVersion: SchemaVersion,
		MessageType:   TaskMessageType,
		TraceID:       traceID,
		TaskID:        "task:" + envelope.ID,
		EmittedAt:     NewTimestamp(emittedAt),
		Envelope:      envelope,
	}
}

func (value TaskQueueMessage) Validate() error {
	if value.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported_schema_version:%s", value.SchemaVersion)
	}
	if value.MessageType != TaskMessageType {
		return errors.New("task_message_type_invalid")
	}
	if strings.TrimSpace(value.TraceID) == "" {
		return errors.New("trace_id is required")
	}
	if strings.TrimSpace(value.TaskID) == "" {
		return errors.New("task_id is required")
	}
	if value.EmittedAt.Time.IsZero() {
		return errors.New("emitted_at is required")
	}
	return value.Envelope.Validate()
}

type EgressQueueMessage struct {
	SchemaVersion string          `json:"schema_version"`
	MessageType   string          `json:"message_type"`
	TaskID        string          `json:"task_id"`
	EnvelopeID    string          `json:"envelope_id"`
	TraceID       string          `json:"trace_id"`
	EventID       string          `json:"event_id"`
	EventKind     string          `json:"event_kind"`
	EmittedAt     Timestamp       `json:"emitted_at"`
	Message       OutboundMessage `json:"message"`
	Sequence      *int            `json:"sequence,omitempty"`
}

func (value EgressQueueMessage) Validate() error {
	if value.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported_schema_version:%s", value.SchemaVersion)
	}
	if value.MessageType != EgressMessageTypeV2 {
		return errors.New("egress_message_type_invalid")
	}
	if strings.TrimSpace(value.TaskID) == "" {
		return errors.New("task_id is required")
	}
	if strings.TrimSpace(value.EnvelopeID) == "" {
		return errors.New("envelope_id is required")
	}
	if strings.TrimSpace(value.TraceID) == "" {
		return errors.New("trace_id is required")
	}
	if strings.TrimSpace(value.EventID) == "" {
		return errors.New("event_id is required")
	}
	if value.EventKind != "message" && value.EventKind != "completion" && value.EventKind != "incremental" {
		return errors.New("event_kind is invalid")
	}
	if value.Sequence == nil && value.EventKind != "incremental" {
		return errors.New("sequence is required for message and completion events")
	}
	if value.Sequence != nil && *value.Sequence < 0 {
		return errors.New("sequence must be non-negative")
	}
	if value.EmittedAt.Time.IsZero() {
		return errors.New("emitted_at is required")
	}
	return value.Message.Validate()
}

type AuxiliaryIngressQueueMessage struct {
	SchemaVersion string    `json:"schema_version"`
	MessageType   string    `json:"message_type"`
	EventID       string    `json:"event_id"`
	ReceivedAt    Timestamp `json:"received_at"`
	Body          any       `json:"body"`
}

func (value AuxiliaryIngressQueueMessage) Validate() error {
	if value.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported_schema_version:%s", value.SchemaVersion)
	}
	if value.MessageType != AuxiliaryIngressMessageType {
		return errors.New("auxiliary_ingress_message_type_invalid")
	}
	if strings.TrimSpace(value.EventID) == "" {
		return errors.New("event_id is required")
	}
	if value.ReceivedAt.Time.IsZero() {
		return errors.New("received_at is required")
	}
	return validateJSONValue(value.Body, "body")
}

func DecodeTaskQueueMessage(raw []byte) (TaskQueueMessage, error) {
	var message TaskQueueMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return TaskQueueMessage{}, err
	}
	return message, message.Validate()
}

func DecodeEgressQueueMessage(raw []byte) (EgressQueueMessage, error) {
	var message EgressQueueMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return EgressQueueMessage{}, err
	}
	return message, message.Validate()
}

func DecodeAuxiliaryIngressQueueMessage(raw []byte) (AuxiliaryIngressQueueMessage, error) {
	var message AuxiliaryIngressQueueMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return AuxiliaryIngressQueueMessage{}, err
	}
	return message, message.Validate()
}

func validateStringList(values []string, fieldName string) error {
	if values == nil {
		return nil
	}
	for _, item := range values {
		if strings.TrimSpace(item) == "" {
			return fmt.Errorf("%s items must be non-empty strings", fieldName)
		}
	}
	return nil
}

func validateMetadata(values map[string]any, fieldName string) error {
	for key := range values {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s keys must be non-empty strings", fieldName)
		}
	}
	return nil
}

func validateJSONValue(value any, fieldName string) error {
	switch typed := value.(type) {
	case nil, string, bool, float64:
		return nil
	case []any:
		for _, item := range typed {
			if err := validateJSONValue(item, fieldName); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		for key, item := range typed {
			if strings.TrimSpace(key) == "" {
				return fmt.Errorf("%s object keys must be non-empty strings", fieldName)
			}
			if err := validateJSONValue(item, fieldName); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%s must be valid JSON data", fieldName)
	}
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func nonNilAttachments(values []AttachmentRef) []AttachmentRef {
	if values == nil {
		return []AttachmentRef{}
	}
	return values
}
