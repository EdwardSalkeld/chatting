package reminder

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const Source = "reminder"

type NowFunc func() time.Time

type Reminder struct {
	ReminderID    string
	Revision      int
	RunAt         time.Time
	Prompt        string
	ContextRefs   []string
	PromptContext []string
	ReplyChannel  contracts.ReplyChannel
}

type SourceStore interface {
	DueReminders(ctx context.Context, now time.Time) ([]Reminder, error)
	MarkFired(ctx context.Context, reminderID string, revision int, firedAt time.Time) (bool, error)
}

type pendingReminder struct {
	reminderID string
	revision   int
}

type Connector struct {
	store               SourceStore
	globalPromptContext []string
	sourcePromptContext []string
	now                 NowFunc
	mu                  sync.Mutex
	pending             map[string]pendingReminder
	metrics             Metrics
}

type Metrics interface {
	RecordReminderDue(late bool, retry bool)
	RecordReminderFired()
}

func New(store SourceStore, globalPromptContext []string, sourcePromptContext []string, now NowFunc, metricRecorders ...Metrics) (*Connector, error) {
	if store == nil {
		return nil, errors.New("reminder store is required")
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	connector := &Connector{
		store: store, globalPromptContext: append([]string{}, globalPromptContext...),
		sourcePromptContext: append([]string{}, sourcePromptContext...), now: now,
		pending: map[string]pendingReminder{},
	}
	if len(metricRecorders) > 0 {
		connector.metrics = metricRecorders[0]
	}
	return connector, nil
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := connector.now().UTC()
	records, err := connector.store.DueReminders(ctx, now)
	if err != nil {
		return nil, err
	}
	envelopes := make([]contracts.TaskEnvelope, 0, len(records))
	for _, record := range records {
		if err := Validate(record); err != nil {
			log.Printf("reminder_skip_invalid reminder_id=%q revision=%d err=%v", record.ReminderID, record.Revision, err)
			continue
		}
		eventID := fmt.Sprintf("reminder:%s:%d", record.ReminderID, record.Revision)
		connector.mu.Lock()
		_, retry := connector.pending[eventID]
		connector.mu.Unlock()
		if connector.metrics != nil {
			connector.metrics.RecordReminderDue(now.After(record.RunAt), retry)
		}
		envelopes = append(envelopes, contracts.TaskEnvelope{
			SchemaVersion: contracts.SchemaVersion,
			ID:            eventID,
			Source:        Source,
			ReceivedAt:    contracts.NewTimestamp(now),
			Actor:         nil,
			Content:       record.Prompt,
			Attachments:   []contracts.AttachmentRef{},
			ContextRefs:   append([]string{}, record.ContextRefs...),
			PromptContext: &contracts.PromptContext{
				GlobalInstructions: append([]string{}, connector.globalPromptContext...),
				SourceInstructions: append([]string{}, connector.sourcePromptContext...),
				TaskInstructions:   append([]string{}, record.PromptContext...),
			},
			ReplyChannel: record.ReplyChannel,
			DedupeKey:    eventID,
		})
		connector.mu.Lock()
		connector.pending[eventID] = pendingReminder{reminderID: record.ReminderID, revision: record.Revision}
		connector.mu.Unlock()
	}
	return envelopes, nil
}

func (connector *Connector) AckEnvelope(ctx context.Context, envelopeID string) error {
	connector.mu.Lock()
	pending, ok := connector.pending[envelopeID]
	connector.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown reminder envelope: %s", envelopeID)
	}
	changed, err := connector.store.MarkFired(ctx, pending.reminderID, pending.revision, connector.now().UTC())
	if err != nil {
		return err
	}
	if changed {
		if connector.metrics != nil {
			connector.metrics.RecordReminderFired()
		}
		log.Printf("reminder_fired reminder_id=%q revision=%d envelope_id=%q", pending.reminderID, pending.revision, envelopeID)
	}
	connector.mu.Lock()
	delete(connector.pending, envelopeID)
	connector.mu.Unlock()
	return nil
}

func Validate(record Reminder) error {
	if strings.TrimSpace(record.ReminderID) == "" {
		return errors.New("reminder_id is required")
	}
	if record.Revision <= 0 {
		return errors.New("revision must be a positive integer")
	}
	if record.RunAt.IsZero() {
		return errors.New("run_at is required")
	}
	if strings.TrimSpace(record.Prompt) == "" {
		return errors.New("prompt is required")
	}
	for _, value := range record.ContextRefs {
		if strings.TrimSpace(value) == "" {
			return errors.New("context_refs must contain non-empty strings")
		}
	}
	for _, value := range record.PromptContext {
		if strings.TrimSpace(value) == "" {
			return errors.New("prompt_context must contain non-empty strings")
		}
	}
	return record.ReplyChannel.Validate()
}
