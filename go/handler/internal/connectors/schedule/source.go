package schedule

import (
	"context"

	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

// StoreSource adapts the sqlite store to a ScheduleSource, exposing the active
// schedule set so the connector can live-reload from the authoritative DB.
type StoreSource struct {
	store *sqlitestate.Store
}

func NewStoreSource(store *sqlitestate.Store) *StoreSource {
	return &StoreSource{store: store}
}

func (source *StoreSource) ActiveSchedules(ctx context.Context) ([]Scheduled, error) {
	records, err := source.store.ListActiveSchedules(ctx)
	if err != nil {
		return nil, err
	}
	scheduled := make([]Scheduled, 0, len(records))
	for _, record := range records {
		scheduled = append(scheduled, Scheduled{
			ScheduleID: record.ScheduleID,
			Job: Job{
				JobName:            record.JobName,
				Content:            record.Content,
				ContextRefs:        record.ContextRefs,
				Cron:               record.Cron,
				PromptContext:      record.PromptContext,
				TimezoneName:       record.Timezone,
				ReplyChannelType:   record.ReplyChannelType,
				ReplyChannelTarget: record.ReplyChannelTarget,
			},
		})
	}
	return scheduled, nil
}
