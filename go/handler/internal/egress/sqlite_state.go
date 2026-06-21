package egress

import (
	"context"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

type SQLiteState struct {
	store *sqlitestate.Store
}

func NewSQLiteState(store *sqlitestate.Store) *SQLiteState {
	return &SQLiteState{store: store}
}

func (state *SQLiteState) GetTask(ctx context.Context, taskID string) (*TaskRecord, error) {
	record, err := state.store.GetTask(ctx, taskID)
	if err != nil || record == nil {
		return nil, err
	}
	return &TaskRecord{
		TaskID:      record.TaskID,
		EnvelopeID:  record.EnvelopeID,
		TraceID:     record.TraceID,
		TaskMessage: record.TaskMessage,
	}, nil
}

func (state *SQLiteState) IsTaskCompleted(ctx context.Context, taskID string, envelopeID string) (bool, error) {
	return state.store.IsTaskCompleted(ctx, taskID, envelopeID)
}

func (state *SQLiteState) MarkTaskCompleted(ctx context.Context, taskID string, envelopeID string, traceID string) error {
	return state.store.MarkTaskCompleted(ctx, taskID, envelopeID, traceID)
}

func (state *SQLiteState) HasDispatchedEventID(ctx context.Context, taskID string, eventID string) (bool, error) {
	return state.store.HasDispatchedEventID(ctx, taskID, eventID)
}

func (state *SQLiteState) MarkDispatchedEventID(ctx context.Context, taskID string, eventID string) error {
	return state.store.MarkDispatchedEventID(ctx, taskID, eventID)
}

func (state *SQLiteState) StageEgressEvent(ctx context.Context, message contracts.EgressQueueMessage) error {
	return state.store.StageEgressEvent(ctx, message)
}

func (state *SQLiteState) ExpectedSequence(ctx context.Context, taskID string) (int, error) {
	return state.store.ExpectedSequence(ctx, taskID)
}

func (state *SQLiteState) GetStagedEventBySequence(ctx context.Context, taskID string, sequence int) (*StagedRecord, error) {
	record, err := state.store.GetStagedEventBySequence(ctx, taskID, sequence)
	if err != nil || record == nil {
		return nil, err
	}
	return &StagedRecord{
		TaskID:        record.TaskID,
		EventID:       record.EventID,
		Sequence:      record.Sequence,
		EgressMessage: record.EgressMessage,
	}, nil
}

func (state *SQLiteState) MarkStagedEventDispatched(ctx context.Context, taskID string, eventID string, sequence int) error {
	return state.store.MarkStagedEventDispatched(ctx, taskID, eventID, sequence)
}

func (state *SQLiteState) AppendConversationTurn(ctx context.Context, channel string, target string, role string, content string, runID string) error {
	return state.store.AppendConversationTurn(ctx, channel, target, role, content, runID)
}
