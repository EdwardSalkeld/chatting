package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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

type TelegramAttachmentRecord struct {
	AttachmentPath   string
	AttachmentURI    string
	TaskID           string
	EnvelopeID       string
	CreatedAt        time.Time
	EligibleAfter    *time.Time
	DeletedAt        *time.Time
	CleanupAttempts  int
	LastCleanupError *string
}

type TelegramAttachmentCleanupResult struct {
	DeletedCount   int
	MissingCount   int
	FailedCount    int
	ReclaimedBytes int64
}

type GitHubAssignmentCheckpoint struct {
	EventCreatedAt time.Time
	EventID        string
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
		`CREATE TABLE IF NOT EXISTS telegram_attachment_ledger (
			attachment_path TEXT PRIMARY KEY,
			attachment_uri TEXT NOT NULL,
			task_id TEXT NOT NULL,
			envelope_id TEXT NOT NULL,
			created_at TEXT NOT NULL,
			eligible_after TEXT,
			deleted_at TEXT,
			cleanup_attempts INTEGER NOT NULL,
			last_cleanup_error TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS github_assignment_checkpoints (
			scope_key TEXT PRIMARY KEY,
			event_created_at TEXT NOT NULL,
			event_id TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) GetGitHubAssignmentCheckpoint(ctx context.Context, scopeKey string) (*GitHubAssignmentCheckpoint, error) {
	if strings.TrimSpace(scopeKey) == "" {
		return nil, errors.New("scope_key is required")
	}
	var eventCreatedAt string
	checkpoint := GitHubAssignmentCheckpoint{}
	err := store.db.QueryRowContext(
		ctx,
		`SELECT event_created_at, event_id
		FROM github_assignment_checkpoints
		WHERE scope_key = ?`,
		scopeKey,
	).Scan(&eventCreatedAt, &checkpoint.EventID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	parsed, err := parseTimestamp(eventCreatedAt)
	if err != nil {
		return nil, err
	}
	checkpoint.EventCreatedAt = parsed
	return &checkpoint, nil
}

func (store *Store) SetGitHubAssignmentCheckpoint(ctx context.Context, scopeKey string, checkpoint GitHubAssignmentCheckpoint) error {
	if strings.TrimSpace(scopeKey) == "" {
		return errors.New("scope_key is required")
	}
	if strings.TrimSpace(checkpoint.EventID) == "" {
		return errors.New("event_id is required")
	}
	if checkpoint.EventCreatedAt.IsZero() {
		return errors.New("event_created_at is required")
	}
	_, err := store.db.ExecContext(
		ctx,
		`INSERT INTO github_assignment_checkpoints (
			scope_key,
			event_created_at,
			event_id,
			updated_at
		)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(scope_key) DO UPDATE SET
			event_created_at = excluded.event_created_at,
			event_id = excluded.event_id,
			updated_at = excluded.updated_at`,
		scopeKey,
		formatTimestamp(checkpoint.EventCreatedAt),
		checkpoint.EventID,
		formatTimestamp(time.Now()),
	)
	return err
}

func (store *Store) RecordTelegramTaskAttachments(ctx context.Context, taskMessage contracts.TaskQueueMessage, attachmentRootDir string) (int, error) {
	if taskMessage.Envelope.Source != "im" || taskMessage.Envelope.ReplyChannel.Type != "telegram" {
		return 0, nil
	}
	rootDir, err := filepath.Abs(defaultTelegramAttachmentRoot(attachmentRootDir))
	if err != nil {
		return 0, err
	}
	type trackedAttachment struct {
		path string
		uri  string
	}
	tracked := []trackedAttachment{}
	for _, attachment := range taskMessage.Envelope.Attachments {
		path, ok := trackedTelegramAttachmentPath(attachment.URI, rootDir)
		if ok {
			tracked = append(tracked, trackedAttachment{path: path, uri: attachment.URI})
		}
	}
	if len(tracked) == 0 {
		return 0, nil
	}
	createdAt := formatTimestamp(time.Now())
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer rollbackUnlessCommitted(tx)
	inserted := 0
	for _, attachment := range tracked {
		result, err := tx.ExecContext(
			ctx,
			`INSERT OR IGNORE INTO telegram_attachment_ledger (
				attachment_path,
				attachment_uri,
				task_id,
				envelope_id,
				created_at,
				eligible_after,
				deleted_at,
				cleanup_attempts,
				last_cleanup_error
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			attachment.path,
			attachment.uri,
			taskMessage.TaskID,
			taskMessage.Envelope.ID,
			createdAt,
			nil,
			nil,
			0,
			nil,
		)
		if err != nil {
			return 0, err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		inserted += int(rows)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return inserted, nil
}

func (store *Store) MarkTelegramTaskAttachmentsEligible(ctx context.Context, taskID string, eligibleAfter time.Time) (int, error) {
	if strings.TrimSpace(taskID) == "" {
		return 0, errors.New("task_id is required")
	}
	result, err := store.db.ExecContext(
		ctx,
		`UPDATE telegram_attachment_ledger
		SET eligible_after = ?, last_cleanup_error = NULL
		WHERE task_id = ? AND deleted_at IS NULL`,
		formatTimestamp(eligibleAfter),
		taskID,
	)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rows), nil
}

func (store *Store) ListTelegramAttachmentRecords(ctx context.Context) ([]TelegramAttachmentRecord, error) {
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT
			attachment_path,
			attachment_uri,
			task_id,
			envelope_id,
			created_at,
			eligible_after,
			deleted_at,
			cleanup_attempts,
			last_cleanup_error
		FROM telegram_attachment_ledger
		ORDER BY created_at ASC, attachment_path ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTelegramAttachmentRecords(rows)
}

func (store *Store) CleanupTelegramAttachments(ctx context.Context, attachmentRootDir string, notAfter time.Time, maxAgeCutoff time.Time) (TelegramAttachmentCleanupResult, error) {
	rootDir, err := filepath.Abs(defaultTelegramAttachmentRoot(attachmentRootDir))
	if err != nil {
		return TelegramAttachmentCleanupResult{}, err
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT
			attachment_path,
			attachment_uri,
			task_id,
			envelope_id,
			created_at,
			eligible_after,
			deleted_at,
			cleanup_attempts,
			last_cleanup_error
		FROM telegram_attachment_ledger
		WHERE deleted_at IS NULL
		  AND (
			(eligible_after IS NOT NULL AND eligible_after <= ?)
			OR created_at <= ?
		  )
		ORDER BY created_at ASC`,
		formatTimestamp(notAfter),
		formatTimestamp(maxAgeCutoff),
	)
	if err != nil {
		return TelegramAttachmentCleanupResult{}, err
	}
	candidates, err := scanTelegramAttachmentRecords(rows)
	if closeErr := rows.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return TelegramAttachmentCleanupResult{}, err
	}
	result := TelegramAttachmentCleanupResult{}
	for _, candidate := range candidates {
		if !pathWithinRoot(candidate.AttachmentPath, rootDir) {
			if markErr := store.markTelegramAttachmentCleanupFailed(ctx, candidate.AttachmentPath, "attachment_path_outside_root"); markErr != nil {
				return result, markErr
			}
			result.FailedCount++
			continue
		}
		info, statErr := os.Stat(candidate.AttachmentPath)
		if errors.Is(statErr, os.ErrNotExist) {
			if err := store.markTelegramAttachmentDeleted(ctx, candidate.AttachmentPath); err != nil {
				return result, err
			}
			result.MissingCount++
			continue
		}
		if statErr != nil {
			if markErr := store.markTelegramAttachmentCleanupFailed(ctx, candidate.AttachmentPath, statErr.Error()); markErr != nil {
				return result, markErr
			}
			result.FailedCount++
			continue
		}
		reclaimedBytes := int64(0)
		if info.Mode().IsRegular() {
			reclaimedBytes = info.Size()
		}
		if err := os.Remove(candidate.AttachmentPath); err != nil {
			if markErr := store.markTelegramAttachmentCleanupFailed(ctx, candidate.AttachmentPath, err.Error()); markErr != nil {
				return result, markErr
			}
			result.FailedCount++
			continue
		}
		if err := store.markTelegramAttachmentDeleted(ctx, candidate.AttachmentPath); err != nil {
			return result, err
		}
		result.DeletedCount++
		result.ReclaimedBytes += reclaimedBytes
	}
	return result, nil
}

func (store *Store) CleanupTelegramAttachmentsForRuntime(ctx context.Context, attachmentRootDir string, notAfter time.Time, maxAgeCutoff time.Time) error {
	_, err := store.CleanupTelegramAttachments(ctx, attachmentRootDir, notAfter, maxAgeCutoff)
	return err
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

func scanTelegramAttachmentRecords(rows *sql.Rows) ([]TelegramAttachmentRecord, error) {
	records := []TelegramAttachmentRecord{}
	for rows.Next() {
		var createdAt string
		var eligibleAfter, deletedAt, lastCleanupError sql.NullString
		record := TelegramAttachmentRecord{}
		if err := rows.Scan(
			&record.AttachmentPath,
			&record.AttachmentURI,
			&record.TaskID,
			&record.EnvelopeID,
			&createdAt,
			&eligibleAfter,
			&deletedAt,
			&record.CleanupAttempts,
			&lastCleanupError,
		); err != nil {
			return nil, err
		}
		parsedCreatedAt, err := parseTimestamp(createdAt)
		if err != nil {
			return nil, err
		}
		record.CreatedAt = parsedCreatedAt
		if eligibleAfter.Valid {
			parsed, err := parseTimestamp(eligibleAfter.String)
			if err != nil {
				return nil, err
			}
			record.EligibleAfter = &parsed
		}
		if deletedAt.Valid {
			parsed, err := parseTimestamp(deletedAt.String)
			if err != nil {
				return nil, err
			}
			record.DeletedAt = &parsed
		}
		record.LastCleanupError = nullStringPointer(lastCleanupError)
		records = append(records, record)
	}
	return records, rows.Err()
}

func trackedTelegramAttachmentPath(rawURI string, rootDir string) (string, bool) {
	parsed, err := url.Parse(rawURI)
	if err != nil || parsed.Scheme != "file" {
		return "", false
	}
	candidate, err := filepath.Abs(parsed.Path)
	if err != nil {
		return "", false
	}
	if !pathWithinRoot(candidate, rootDir) {
		return "", false
	}
	return candidate, true
}

func defaultTelegramAttachmentRoot(rootDir string) string {
	if strings.TrimSpace(rootDir) == "" {
		return filepath.Join(os.TempDir(), "chatting-telegram-attachments")
	}
	return rootDir
}

func pathWithinRoot(candidate string, rootDir string) bool {
	relative, err := filepath.Rel(rootDir, candidate)
	if err != nil {
		return false
	}
	return relative == "." || (!strings.HasPrefix(relative, ".."+string(filepath.Separator)) && relative != "..")
}

func (store *Store) markTelegramAttachmentDeleted(ctx context.Context, attachmentPath string) error {
	_, err := store.db.ExecContext(
		ctx,
		`UPDATE telegram_attachment_ledger
		SET deleted_at = ?, last_cleanup_error = NULL
		WHERE attachment_path = ?`,
		formatTimestamp(time.Now()),
		attachmentPath,
	)
	return err
}

func (store *Store) markTelegramAttachmentCleanupFailed(ctx context.Context, attachmentPath string, cleanupError string) error {
	_, err := store.db.ExecContext(
		ctx,
		`UPDATE telegram_attachment_ledger
		SET cleanup_attempts = cleanup_attempts + 1, last_cleanup_error = ?
		WHERE attachment_path = ?`,
		cleanupError,
		attachmentPath,
	)
	return err
}
