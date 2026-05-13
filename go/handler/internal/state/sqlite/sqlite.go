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
	}
	for _, statement := range statements {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
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
