package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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

// ErrScheduleNotFound is returned when an operation targets a schedule that has no active version.
var ErrScheduleNotFound = errors.New("schedule not found")

// Reminder lifecycle errors are stable sentinels for API status mapping.
var (
	ErrReminderNotFound            = errors.New("reminder not found")
	ErrReminderIdempotencyConflict = errors.New("reminder idempotency key conflicts with a different request")
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

type ConversationTurn struct {
	Role      string
	Content   string
	Sender    string
	CreatedAt time.Time
}

type GitHubAssignmentCheckpoint struct {
	EventCreatedAt time.Time
	EventID        string
}

type ScheduleRecord struct {
	ScheduleID         string
	Version            int
	Status             string
	JobName            string
	Content            string
	Cron               string
	Timezone           string
	ContextRefs        []string
	PromptContext      []string
	ReplyChannelType   string
	ReplyChannelTarget string
	CreatedAt          time.Time
	CreatedBy          string
	SupersededAt       *time.Time
}

type ScheduleInput struct {
	JobName            string
	Content            string
	Cron               string
	Timezone           string
	ContextRefs        []string
	PromptContext      []string
	ReplyChannelType   string
	ReplyChannelTarget string
	CreatedBy          string
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
	// The handler writes to this DB from two places concurrently now: the
	// ingress Run loop and the synchronous egress HTTP endpoint. Without a busy
	// timeout the second writer fails immediately with SQLITE_BUSY ("database is
	// locked"); busy_timeout makes it wait for the lock instead, and WAL lets
	// reads proceed alongside a writer. Applied per-connection via the modernc
	// DSN so every pooled connection gets them.
	separator := "?"
	if strings.Contains(dbPath, "?") {
		separator = "&"
	}
	dsn := dbPath + separator + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
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
		`CREATE TABLE IF NOT EXISTS conversation_turns (
			turn_id INTEGER PRIMARY KEY AUTOINCREMENT,
			channel TEXT NOT NULL,
			target TEXT NOT NULL,
			role TEXT NOT NULL,
			content TEXT NOT NULL,
			sender TEXT,
			run_id TEXT,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS github_assignment_checkpoints (
			scope_key TEXT PRIMARY KEY,
			event_created_at TEXT NOT NULL,
			event_id TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS schedules (
			row_id INTEGER PRIMARY KEY AUTOINCREMENT,
			schedule_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			status TEXT NOT NULL,
			job_name TEXT NOT NULL,
			content TEXT NOT NULL,
			cron TEXT NOT NULL,
			timezone TEXT NOT NULL,
			context_refs TEXT NOT NULL,
			prompt_context TEXT NOT NULL,
			reply_channel_type TEXT NOT NULL,
			reply_channel_target TEXT NOT NULL,
			created_at TEXT NOT NULL,
			created_by TEXT NOT NULL,
			superseded_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_schedules_schedule_id_status ON schedules (schedule_id, status)`,
		`CREATE TABLE IF NOT EXISTS reminders (
			row_id INTEGER PRIMARY KEY AUTOINCREMENT,
			reminder_id TEXT NOT NULL,
			revision INTEGER NOT NULL,
			status TEXT NOT NULL,
			run_at TEXT NOT NULL,
			prompt TEXT NOT NULL,
			context_refs TEXT NOT NULL,
			prompt_context TEXT NOT NULL,
			reply_channel_type TEXT NOT NULL,
			reply_channel_target TEXT NOT NULL,
			reply_channel_metadata TEXT NOT NULL,
			created_from_task_id TEXT NOT NULL,
			created_by TEXT NOT NULL,
			idempotency_key TEXT NOT NULL UNIQUE,
			request_fingerprint TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			fired_at TEXT,
			cancelled_at TEXT,
			UNIQUE (reminder_id, revision)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_reminders_status_run_at ON reminders (status, run_at)`,
		`CREATE INDEX IF NOT EXISTS idx_reminders_reminder_id_revision ON reminders (reminder_id, revision DESC)`,
	}
	for _, statement := range statements {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	// conversation_turns.sender was added after the table shipped. CREATE TABLE
	// IF NOT EXISTS won't add a column to a pre-existing table, so migrate it in
	// explicitly. Idempotent: only ALTER when the column is absent.
	if err := store.ensureColumn(ctx, "conversation_turns", "sender", "TEXT"); err != nil {
		return err
	}
	return nil
}

func (store *Store) ensureColumn(ctx context.Context, table string, column string, columnType string) error {
	rows, err := store.db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid          int
			name         string
			colType      string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Close before the ALTER: an open result set can hold a read lock that
	// blocks the write on some SQLite drivers.
	if err := rows.Close(); err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+column+" "+columnType)
	return err
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

const scheduleSelectColumns = `SELECT
	schedule_id,
	version,
	status,
	job_name,
	content,
	cron,
	timezone,
	context_refs,
	prompt_context,
	reply_channel_type,
	reply_channel_target,
	created_at,
	created_by,
	superseded_at`

func (store *Store) ListActiveSchedules(ctx context.Context) ([]ScheduleRecord, error) {
	rows, err := store.db.QueryContext(
		ctx,
		scheduleSelectColumns+`
		FROM schedules
		WHERE status = 'active'
		ORDER BY job_name ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanScheduleRecords(rows)
}

func (store *Store) GetActiveSchedule(ctx context.Context, scheduleID string) (*ScheduleRecord, error) {
	if strings.TrimSpace(scheduleID) == "" {
		return nil, errors.New("schedule_id is required")
	}
	record, err := scanScheduleRecord(store.db.QueryRowContext(
		ctx,
		scheduleSelectColumns+`
		FROM schedules
		WHERE schedule_id = ? AND status = 'active'`,
		scheduleID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (store *Store) GetScheduleHistory(ctx context.Context, scheduleID string) ([]ScheduleRecord, error) {
	if strings.TrimSpace(scheduleID) == "" {
		return nil, errors.New("schedule_id is required")
	}
	rows, err := store.db.QueryContext(
		ctx,
		scheduleSelectColumns+`
		FROM schedules
		WHERE schedule_id = ?
		ORDER BY version DESC`,
		scheduleID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanScheduleRecords(rows)
}

func (store *Store) CreateSchedule(ctx context.Context, input ScheduleInput) (ScheduleRecord, error) {
	scheduleID, err := generateScheduleID()
	if err != nil {
		return ScheduleRecord{}, err
	}
	record := scheduleRecordFromInput(scheduleID, 1, input, time.Now())
	if err := store.insertScheduleRow(ctx, store.db, record); err != nil {
		return ScheduleRecord{}, err
	}
	return record, nil
}

func (store *Store) ReplaceSchedule(ctx context.Context, scheduleID string, input ScheduleInput) (ScheduleRecord, error) {
	if strings.TrimSpace(scheduleID) == "" {
		return ScheduleRecord{}, errors.New("schedule_id is required")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ScheduleRecord{}, err
	}
	defer rollbackUnlessCommitted(tx)

	current, err := scanScheduleRecord(tx.QueryRowContext(
		ctx,
		scheduleSelectColumns+`
		FROM schedules
		WHERE schedule_id = ? AND status = 'active'`,
		scheduleID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return ScheduleRecord{}, ErrScheduleNotFound
	}
	if err != nil {
		return ScheduleRecord{}, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE schedules
		SET status = 'dead', superseded_at = ?
		WHERE schedule_id = ? AND status = 'active'`,
		formatTimestamp(now),
		scheduleID,
	); err != nil {
		return ScheduleRecord{}, err
	}
	record := scheduleRecordFromInput(scheduleID, current.Version+1, input, now)
	if err := store.insertScheduleRow(ctx, tx, record); err != nil {
		return ScheduleRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return ScheduleRecord{}, err
	}
	return record, nil
}

func (store *Store) MarkScheduleDead(ctx context.Context, scheduleID string) (bool, error) {
	if strings.TrimSpace(scheduleID) == "" {
		return false, errors.New("schedule_id is required")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer rollbackUnlessCommitted(tx)

	result, err := tx.ExecContext(
		ctx,
		`UPDATE schedules
		SET status = 'dead', superseded_at = ?
		WHERE schedule_id = ? AND status = 'active'`,
		formatTimestamp(time.Now()),
		scheduleID,
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (store *Store) insertScheduleRow(ctx context.Context, exec scheduleExecer, record ScheduleRecord) error {
	contextRefs, err := encodeStringList(record.ContextRefs)
	if err != nil {
		return err
	}
	promptContext, err := encodeStringList(record.PromptContext)
	if err != nil {
		return err
	}
	var supersededAt any
	if record.SupersededAt != nil {
		supersededAt = formatTimestamp(*record.SupersededAt)
	}
	_, err = exec.ExecContext(
		ctx,
		`INSERT INTO schedules (
			schedule_id,
			version,
			status,
			job_name,
			content,
			cron,
			timezone,
			context_refs,
			prompt_context,
			reply_channel_type,
			reply_channel_target,
			created_at,
			created_by,
			superseded_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ScheduleID,
		record.Version,
		record.Status,
		record.JobName,
		record.Content,
		record.Cron,
		record.Timezone,
		contextRefs,
		promptContext,
		record.ReplyChannelType,
		record.ReplyChannelTarget,
		formatTimestamp(record.CreatedAt),
		record.CreatedBy,
		supersededAt,
	)
	return err
}

type scheduleExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type scheduleScanner interface {
	Scan(dest ...any) error
}

func scheduleRecordFromInput(scheduleID string, version int, input ScheduleInput, now time.Time) ScheduleRecord {
	timezone := strings.TrimSpace(input.Timezone)
	if timezone == "" {
		timezone = "UTC"
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		createdBy = "api"
	}
	contextRefs := input.ContextRefs
	if contextRefs == nil {
		contextRefs = []string{}
	}
	promptContext := input.PromptContext
	if promptContext == nil {
		promptContext = []string{}
	}
	return ScheduleRecord{
		ScheduleID:         scheduleID,
		Version:            version,
		Status:             "active",
		JobName:            input.JobName,
		Content:            input.Content,
		Cron:               input.Cron,
		Timezone:           timezone,
		ContextRefs:        contextRefs,
		PromptContext:      promptContext,
		ReplyChannelType:   input.ReplyChannelType,
		ReplyChannelTarget: input.ReplyChannelTarget,
		CreatedAt:          now.UTC(),
		CreatedBy:          createdBy,
	}
}

func scanScheduleRecords(rows *sql.Rows) ([]ScheduleRecord, error) {
	records := []ScheduleRecord{}
	for rows.Next() {
		record, err := scanScheduleRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func scanScheduleRecord(scanner scheduleScanner) (ScheduleRecord, error) {
	record := ScheduleRecord{}
	var contextRefs, promptContext, createdAt string
	var supersededAt sql.NullString
	if err := scanner.Scan(
		&record.ScheduleID,
		&record.Version,
		&record.Status,
		&record.JobName,
		&record.Content,
		&record.Cron,
		&record.Timezone,
		&contextRefs,
		&promptContext,
		&record.ReplyChannelType,
		&record.ReplyChannelTarget,
		&createdAt,
		&record.CreatedBy,
		&supersededAt,
	); err != nil {
		return ScheduleRecord{}, err
	}
	decodedContextRefs, err := decodeStringList(contextRefs)
	if err != nil {
		return ScheduleRecord{}, err
	}
	record.ContextRefs = decodedContextRefs
	decodedPromptContext, err := decodeStringList(promptContext)
	if err != nil {
		return ScheduleRecord{}, err
	}
	record.PromptContext = decodedPromptContext
	parsedCreatedAt, err := parseTimestamp(createdAt)
	if err != nil {
		return ScheduleRecord{}, err
	}
	record.CreatedAt = parsedCreatedAt
	if supersededAt.Valid {
		parsed, err := parseTimestamp(supersededAt.String)
		if err != nil {
			return ScheduleRecord{}, err
		}
		record.SupersededAt = &parsed
	}
	return record, nil
}

func generateScheduleID() (string, error) {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return "sched_" + hex.EncodeToString(buffer), nil
}

func encodeStringList(values []string) (string, error) {
	if values == nil {
		values = []string{}
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeStringList(raw string) ([]string, error) {
	values := []string{}
	if strings.TrimSpace(raw) == "" {
		return values, nil
	}
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, err
	}
	if values == nil {
		values = []string{}
	}
	return values, nil
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

func (store *Store) AppendConversationTurn(ctx context.Context, channel string, target string, role string, content string, sender string, runID string) error {
	if strings.TrimSpace(channel) == "" {
		return errors.New("channel is required")
	}
	if strings.TrimSpace(target) == "" {
		return errors.New("target is required")
	}
	if role != "user" && role != "assistant" {
		return errors.New("role must be user or assistant")
	}
	if strings.TrimSpace(content) == "" {
		return errors.New("content is required")
	}
	if runID != "" && strings.TrimSpace(runID) == "" {
		return errors.New("run_id must not be empty")
	}
	_, err := store.db.ExecContext(
		ctx,
		`INSERT INTO conversation_turns (
			channel,
			target,
			role,
			content,
			sender,
			run_id,
			created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		channel,
		target,
		role,
		content,
		nullIfEmpty(sender),
		nullIfEmpty(runID),
		formatTimestamp(time.Now()),
	)
	return err
}

func (store *Store) ListRecentConversationTurns(ctx context.Context, channel string, target string, limit int) ([]ConversationTurn, error) {
	if strings.TrimSpace(channel) == "" {
		return nil, errors.New("channel is required")
	}
	if strings.TrimSpace(target) == "" {
		return nil, errors.New("target is required")
	}
	if limit <= 0 {
		return nil, errors.New("limit must be positive")
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT role, content, sender, created_at
		FROM conversation_turns
		WHERE channel = ? AND target = ?
		ORDER BY turn_id DESC
		LIMIT ?`,
		channel,
		target,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	reversed := make([]ConversationTurn, 0, limit)
	for rows.Next() {
		turn := ConversationTurn{}
		var sender sql.NullString
		var createdAt string
		if err := rows.Scan(&turn.Role, &turn.Content, &sender, &createdAt); err != nil {
			return nil, err
		}
		turn.Sender = sender.String
		turn.CreatedAt, err = parseTimestamp(createdAt)
		if err != nil {
			return nil, err
		}
		reversed = append(reversed, turn)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	turns := make([]ConversationTurn, 0, len(reversed))
	for index := len(reversed) - 1; index >= 0; index-- {
		turns = append(turns, reversed[index])
	}
	return turns, nil
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

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
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
