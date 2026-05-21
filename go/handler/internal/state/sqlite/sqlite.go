package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type TaskLedgerRecord struct {
	TaskID      string
	EnvelopeID  string
	TraceID     string
	TaskMessage contracts.TaskQueueMessage
	CreatedAt   time.Time
}

type CompletedTaskRecord struct {
	TaskID      string
	EnvelopeID  string
	TraceID     string
	CompletedAt time.Time
}

type StagedEgressRecord struct {
	TaskID        string
	EventID       string
	Sequence      int
	EgressMessage contracts.EgressQueueMessage
	CreatedAt     time.Time
}

type TelegramChatObservation struct {
	ChatID      string
	ChatType    *string
	Title       *string
	Username    *string
	UpdateID    int64
	UpdateKind  string
	MessageDate *time.Time
	RetrievedAt time.Time
}

type TelegramChatRecord struct {
	ChatID          string
	ChatType        *string
	Title           *string
	Username        *string
	FirstSeenAt     time.Time
	LastRetrievedAt time.Time
	LastMessageAt   *time.Time
	LastUpdateID    int64
	LastUpdateKind  string
}

func Open(ctx context.Context, dbPath string) (*Store, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, errors.New("db_path is required")
	}
	dir := filepath.Dir(dbPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (store *Store) Close() error {
	return store.db.Close()
}

func (store *Store) initialize(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS idempotency_keys (
			source TEXT NOT NULL,
			dedupe_key TEXT NOT NULL,
			seen_at TEXT NOT NULL,
			PRIMARY KEY (source, dedupe_key)
		)`,
		`CREATE TABLE IF NOT EXISTS task_ledger (
			task_id TEXT PRIMARY KEY,
			envelope_id TEXT NOT NULL,
			trace_id TEXT NOT NULL,
			task_payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS completed_task_ledger (
			task_id TEXT PRIMARY KEY,
			envelope_id TEXT NOT NULL,
			trace_id TEXT NOT NULL,
			completed_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS dispatched_event_ids (
			task_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			dispatched_at TEXT NOT NULL,
			PRIMARY KEY (task_id, event_id)
		)`,
		`CREATE TABLE IF NOT EXISTS egress_sequence_state (
			task_id TEXT PRIMARY KEY,
			next_sequence INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS staged_egress_events (
			task_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			sequence INTEGER NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (task_id, event_id)
		)`,
		`CREATE TABLE IF NOT EXISTS telegram_chat_registry (
			chat_id TEXT PRIMARY KEY,
			chat_type TEXT,
			title TEXT,
			username TEXT,
			first_seen_at TEXT NOT NULL,
			last_retrieved_at TEXT NOT NULL,
			last_message_at TEXT,
			last_update_id INTEGER NOT NULL,
			last_update_kind TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) RecordTelegramChat(ctx context.Context, observation TelegramChatObservation) error {
	if strings.TrimSpace(observation.ChatID) == "" {
		return errors.New("chat_id is required")
	}
	if strings.TrimSpace(observation.UpdateKind) == "" {
		return errors.New("update_kind is required")
	}
	retrievedAt := observation.RetrievedAt
	if retrievedAt.IsZero() {
		retrievedAt = time.Now().UTC()
	}
	var messageDate any
	if observation.MessageDate != nil {
		messageDate = formatTimestamp(*observation.MessageDate)
	}
	_, err := store.db.ExecContext(
		ctx,
		`INSERT INTO telegram_chat_registry (
			chat_id,
			chat_type,
			title,
			username,
			first_seen_at,
			last_retrieved_at,
			last_message_at,
			last_update_id,
			last_update_kind
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(chat_id) DO UPDATE SET
			chat_type = COALESCE(excluded.chat_type, telegram_chat_registry.chat_type),
			title = COALESCE(excluded.title, telegram_chat_registry.title),
			username = COALESCE(excluded.username, telegram_chat_registry.username),
			last_retrieved_at = excluded.last_retrieved_at,
			last_message_at = COALESCE(excluded.last_message_at, telegram_chat_registry.last_message_at),
			last_update_id = excluded.last_update_id,
			last_update_kind = excluded.last_update_kind`,
		observation.ChatID,
		nullableString(observation.ChatType),
		nullableString(observation.Title),
		nullableString(observation.Username),
		formatTimestamp(retrievedAt),
		formatTimestamp(retrievedAt),
		messageDate,
		observation.UpdateID,
		observation.UpdateKind,
	)
	return err
}

func (store *Store) ListTelegramChats(ctx context.Context) ([]TelegramChatRecord, error) {
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT
			chat_id,
			chat_type,
			title,
			username,
			first_seen_at,
			last_retrieved_at,
			last_message_at,
			last_update_id,
			last_update_kind
		FROM telegram_chat_registry
		ORDER BY last_retrieved_at DESC, chat_id ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := []TelegramChatRecord{}
	for rows.Next() {
		var chatType, title, username, lastMessageAt sql.NullString
		var firstSeenAt, lastRetrievedAt string
		record := TelegramChatRecord{}
		if err := rows.Scan(
			&record.ChatID,
			&chatType,
			&title,
			&username,
			&firstSeenAt,
			&lastRetrievedAt,
			&lastMessageAt,
			&record.LastUpdateID,
			&record.LastUpdateKind,
		); err != nil {
			return nil, err
		}
		record.ChatType = nullStringPointer(chatType)
		record.Title = nullStringPointer(title)
		record.Username = nullStringPointer(username)
		parsedFirstSeenAt, err := parseTimestamp(firstSeenAt)
		if err != nil {
			return nil, err
		}
		record.FirstSeenAt = parsedFirstSeenAt
		parsedLastRetrievedAt, err := parseTimestamp(lastRetrievedAt)
		if err != nil {
			return nil, err
		}
		record.LastRetrievedAt = parsedLastRetrievedAt
		if lastMessageAt.Valid {
			parsedLastMessageAt, err := parseTimestamp(lastMessageAt.String)
			if err != nil {
				return nil, err
			}
			record.LastMessageAt = &parsedLastMessageAt
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (store *Store) Seen(ctx context.Context, source string, dedupeKey string) (bool, error) {
	if strings.TrimSpace(source) == "" {
		return false, errors.New("source is required")
	}
	if strings.TrimSpace(dedupeKey) == "" {
		return false, errors.New("dedupe_key is required")
	}
	var found int
	err := store.db.QueryRowContext(
		ctx,
		`SELECT 1 FROM idempotency_keys WHERE source = ? AND dedupe_key = ?`,
		source,
		dedupeKey,
	).Scan(&found)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func (store *Store) MarkSeen(ctx context.Context, source string, dedupeKey string) error {
	if strings.TrimSpace(source) == "" {
		return errors.New("source is required")
	}
	if strings.TrimSpace(dedupeKey) == "" {
		return errors.New("dedupe_key is required")
	}
	_, err := store.db.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO idempotency_keys (source, dedupe_key, seen_at)
		VALUES (?, ?, ?)`,
		source,
		dedupeKey,
		formatTimestamp(time.Now()),
	)
	return err
}

func (store *Store) RecordTask(ctx context.Context, taskMessage contracts.TaskQueueMessage) error {
	if err := taskMessage.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(taskMessage)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(tx)

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM completed_task_ledger WHERE task_id = ?`,
		taskMessage.TaskID,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR REPLACE INTO task_ledger (
			task_id,
			envelope_id,
			trace_id,
			task_payload_json,
			created_at
		)
		VALUES (?, ?, ?, ?, ?)`,
		taskMessage.TaskID,
		taskMessage.Envelope.ID,
		taskMessage.TraceID,
		string(payload),
		formatTimestamp(time.Now()),
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) GetTask(ctx context.Context, taskID string) (*TaskLedgerRecord, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, errors.New("task_id is required")
	}
	var payload string
	var createdAt string
	record := TaskLedgerRecord{}
	err := store.db.QueryRowContext(
		ctx,
		`SELECT task_id, envelope_id, trace_id, task_payload_json, created_at
		FROM task_ledger
		WHERE task_id = ?`,
		taskID,
	).Scan(&record.TaskID, &record.EnvelopeID, &record.TraceID, &payload, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	taskMessage, err := contracts.DecodeTaskQueueMessage([]byte(payload))
	if err != nil {
		return nil, err
	}
	parsedCreatedAt, err := parseTimestamp(createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse task_ledger.created_at: %w", err)
	}
	record.TaskMessage = taskMessage
	record.CreatedAt = parsedCreatedAt
	return &record, nil
}

func (store *Store) MarkTaskCompleted(ctx context.Context, taskID string, envelopeID string, traceID string) error {
	if strings.TrimSpace(taskID) == "" {
		return errors.New("task_id is required")
	}
	if strings.TrimSpace(envelopeID) == "" {
		return errors.New("envelope_id is required")
	}
	if strings.TrimSpace(traceID) == "" {
		return errors.New("trace_id is required")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(tx)

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR REPLACE INTO completed_task_ledger (
			task_id,
			envelope_id,
			trace_id,
			completed_at
		)
		VALUES (?, ?, ?, ?)`,
		taskID,
		envelopeID,
		traceID,
		formatTimestamp(time.Now()),
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_ledger WHERE task_id = ?`, taskID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM egress_sequence_state WHERE task_id = ?`, taskID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM staged_egress_events WHERE task_id = ?`, taskID); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) GetCompletedTask(ctx context.Context, taskID string) (*CompletedTaskRecord, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, errors.New("task_id is required")
	}
	record := CompletedTaskRecord{}
	var completedAt string
	err := store.db.QueryRowContext(
		ctx,
		`SELECT task_id, envelope_id, trace_id, completed_at
		FROM completed_task_ledger
		WHERE task_id = ?`,
		taskID,
	).Scan(&record.TaskID, &record.EnvelopeID, &record.TraceID, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	parsedCompletedAt, err := parseTimestamp(completedAt)
	if err != nil {
		return nil, fmt.Errorf("parse completed_task_ledger.completed_at: %w", err)
	}
	record.CompletedAt = parsedCompletedAt
	return &record, nil
}

func (store *Store) IsTaskCompleted(ctx context.Context, taskID string, envelopeID string) (bool, error) {
	record, err := store.GetCompletedTask(ctx, taskID)
	if err != nil || record == nil {
		return false, err
	}
	return record.EnvelopeID == envelopeID, nil
}

func (store *Store) MarkDispatchedEventID(ctx context.Context, taskID string, eventID string) error {
	if strings.TrimSpace(taskID) == "" {
		return errors.New("task_id is required")
	}
	if strings.TrimSpace(eventID) == "" {
		return errors.New("event_id is required")
	}
	_, err := store.db.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO dispatched_event_ids (task_id, event_id, dispatched_at)
		VALUES (?, ?, ?)`,
		taskID,
		eventID,
		formatTimestamp(time.Now()),
	)
	return err
}

func (store *Store) HasDispatchedEventID(ctx context.Context, taskID string, eventID string) (bool, error) {
	if strings.TrimSpace(taskID) == "" {
		return false, errors.New("task_id is required")
	}
	if strings.TrimSpace(eventID) == "" {
		return false, errors.New("event_id is required")
	}
	var found int
	err := store.db.QueryRowContext(
		ctx,
		`SELECT 1 FROM dispatched_event_ids WHERE task_id = ? AND event_id = ?`,
		taskID,
		eventID,
	).Scan(&found)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func (store *Store) StageEgressEvent(ctx context.Context, egressMessage contracts.EgressQueueMessage) error {
	if err := egressMessage.Validate(); err != nil {
		return err
	}
	if egressMessage.Sequence == nil {
		return errors.New("sequence is required")
	}
	payload, err := json.Marshal(egressMessage)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(tx)

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO staged_egress_events (
			task_id,
			event_id,
			sequence,
			payload_json,
			created_at
		)
		VALUES (?, ?, ?, ?, ?)`,
		egressMessage.TaskID,
		egressMessage.EventID,
		*egressMessage.Sequence,
		string(payload),
		formatTimestamp(time.Now()),
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO egress_sequence_state (task_id, next_sequence)
		VALUES (?, 0)`,
		egressMessage.TaskID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) ExpectedSequence(ctx context.Context, taskID string) (int, error) {
	if strings.TrimSpace(taskID) == "" {
		return 0, errors.New("task_id is required")
	}
	var nextSequence int
	err := store.db.QueryRowContext(
		ctx,
		`SELECT next_sequence FROM egress_sequence_state WHERE task_id = ?`,
		taskID,
	).Scan(&nextSequence)
	if err == nil {
		return nextSequence, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return 0, err
}

func (store *Store) GetStagedEventBySequence(ctx context.Context, taskID string, sequence int) (*StagedEgressRecord, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, errors.New("task_id is required")
	}
	if sequence < 0 {
		return nil, errors.New("sequence must be non-negative")
	}
	var payload string
	var createdAt string
	record := StagedEgressRecord{}
	err := store.db.QueryRowContext(
		ctx,
		`SELECT task_id, event_id, sequence, payload_json, created_at
		FROM staged_egress_events
		WHERE task_id = ? AND sequence = ?`,
		taskID,
		sequence,
	).Scan(&record.TaskID, &record.EventID, &record.Sequence, &payload, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	egressMessage, err := contracts.DecodeEgressQueueMessage([]byte(payload))
	if err != nil {
		return nil, err
	}
	parsedCreatedAt, err := parseTimestamp(createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse staged_egress_events.created_at: %w", err)
	}
	record.EgressMessage = egressMessage
	record.CreatedAt = parsedCreatedAt
	return &record, nil
}

func (store *Store) MarkStagedEventDispatched(ctx context.Context, taskID string, eventID string, sequence int) error {
	if strings.TrimSpace(taskID) == "" {
		return errors.New("task_id is required")
	}
	if strings.TrimSpace(eventID) == "" {
		return errors.New("event_id is required")
	}
	if sequence < 0 {
		return errors.New("sequence must be non-negative")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(tx)

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM staged_egress_events
		WHERE task_id = ? AND event_id = ? AND sequence = ?`,
		taskID,
		eventID,
		sequence,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO egress_sequence_state (task_id, next_sequence)
		VALUES (?, ?)
		ON CONFLICT(task_id) DO UPDATE SET
			next_sequence = CASE
				WHEN egress_sequence_state.next_sequence <= excluded.next_sequence
				THEN excluded.next_sequence
				ELSE egress_sequence_state.next_sequence
			END`,
		taskID,
		sequence+1,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func rollbackUnlessCommitted(tx *sql.Tx) {
	_ = tx.Rollback()
}

func formatTimestamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTimestamp(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullStringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}
