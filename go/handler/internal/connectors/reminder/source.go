package reminder

import (
	"context"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

type StoreSource struct {
	store *sqlitestate.Store
}

func NewStoreSource(store *sqlitestate.Store) *StoreSource {
	return &StoreSource{store: store}
}

func (source *StoreSource) DueReminders(ctx context.Context, now time.Time) ([]Reminder, error) {
	records, err := source.store.ListDueReminders(ctx, now)
	if err != nil {
		return nil, err
	}
	result := make([]Reminder, 0, len(records))
	for _, record := range records {
		result = append(result, Reminder{
			ReminderID: record.ReminderID, Revision: record.Revision, RunAt: record.RunAt,
			Prompt: record.Prompt, ContextRefs: record.ContextRefs, PromptContext: record.PromptContext,
			ReplyChannel: contracts.ReplyChannel{
				Type: record.ReplyChannelType, Target: record.ReplyChannelTarget,
				Metadata: record.ReplyChannelMetadata,
			},
		})
	}
	return result, nil
}

func (source *StoreSource) MarkFired(ctx context.Context, reminderID string, revision int, firedAt time.Time) (bool, error) {
	return source.store.MarkReminderFired(ctx, reminderID, revision, firedAt)
}
