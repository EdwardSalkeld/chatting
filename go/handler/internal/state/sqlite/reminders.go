package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ReminderStatusScheduled = "scheduled"
	ReminderStatusFired     = "fired"
	ReminderStatusCancelled = "cancelled"
)

type ReminderRecord struct {
	ReminderID           string
	Revision             int
	Status               string
	RunAt                time.Time
	Prompt               string
	ContextRefs          []string
	PromptContext        []string
	ReplyChannelType     string
	ReplyChannelTarget   string
	ReplyChannelMetadata map[string]any
	CreatedFromTaskID    string
	CreatedBy            string
	IdempotencyKey       string
	RequestFingerprint   string
	CreatedAt            time.Time
	UpdatedAt            time.Time
	FiredAt              *time.Time
	CancelledAt          *time.Time
}

type ReminderInput struct {
	RunAt                time.Time
	Prompt               string
	ContextRefs          []string
	PromptContext        []string
	ReplyChannelType     string
	ReplyChannelTarget   string
	ReplyChannelMetadata map[string]any
	CreatedFromTaskID    string
	CreatedBy            string
	IdempotencyKey       string
}

type reminderFingerprint struct {
	ReminderID           string         `json:"reminder_id,omitempty"`
	RunAt                string         `json:"run_at"`
	Prompt               string         `json:"prompt"`
	ContextRefs          []string       `json:"context_refs"`
	PromptContext        []string       `json:"prompt_context"`
	ReplyChannelType     string         `json:"reply_channel_type"`
	ReplyChannelTarget   string         `json:"reply_channel_target"`
	ReplyChannelMetadata map[string]any `json:"reply_channel_metadata"`
	CreatedFromTaskID    string         `json:"created_from_task_id"`
	CreatedBy            string         `json:"created_by"`
}

const reminderSelectColumns = `SELECT
	reminder_id,
	revision,
	status,
	run_at,
	prompt,
	context_refs,
	prompt_context,
	reply_channel_type,
	reply_channel_target,
	reply_channel_metadata,
	created_from_task_id,
	created_by,
	idempotency_key,
	request_fingerprint,
	created_at,
	updated_at,
	fired_at,
	cancelled_at`

func (store *Store) CreateReminder(ctx context.Context, input ReminderInput) (ReminderRecord, bool, error) {
	input = normalizeReminderInput(input)
	fingerprint, err := reminderRequestFingerprint("", input)
	if err != nil {
		return ReminderRecord{}, false, err
	}
	if existing, err := store.GetReminderByIdempotencyKey(ctx, input.IdempotencyKey); err != nil {
		return ReminderRecord{}, false, err
	} else if existing != nil {
		if existing.RequestFingerprint != fingerprint {
			return ReminderRecord{}, false, ErrReminderIdempotencyConflict
		}
		return *existing, true, nil
	}
	reminderID, err := generateReminderID()
	if err != nil {
		return ReminderRecord{}, false, err
	}
	now := time.Now().UTC()
	record := reminderRecordFromInput(reminderID, 1, input, fingerprint, now)
	if err := store.insertReminderRow(ctx, store.db, record); err != nil {
		// A concurrent retry can win the unique idempotency-key insert.
		if existing, lookupErr := store.GetReminderByIdempotencyKey(ctx, input.IdempotencyKey); lookupErr == nil && existing != nil {
			if existing.RequestFingerprint == fingerprint {
				return *existing, true, nil
			}
			return ReminderRecord{}, false, ErrReminderIdempotencyConflict
		}
		return ReminderRecord{}, false, err
	}
	return record, false, nil
}

func (store *Store) ReplaceReminder(ctx context.Context, reminderID string, input ReminderInput) (ReminderRecord, bool, error) {
	if strings.TrimSpace(reminderID) == "" {
		return ReminderRecord{}, false, errors.New("reminder_id is required")
	}
	input = normalizeReminderInput(input)
	fingerprint, err := reminderRequestFingerprint(reminderID, input)
	if err != nil {
		return ReminderRecord{}, false, err
	}
	if existing, err := store.GetReminderByIdempotencyKey(ctx, input.IdempotencyKey); err != nil {
		return ReminderRecord{}, false, err
	} else if existing != nil {
		if existing.RequestFingerprint != fingerprint || existing.ReminderID != reminderID {
			return ReminderRecord{}, false, ErrReminderIdempotencyConflict
		}
		return *existing, true, nil
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ReminderRecord{}, false, err
	}
	defer rollbackUnlessCommitted(tx)
	current, err := scanReminderRecord(tx.QueryRowContext(ctx, reminderSelectColumns+`
		FROM reminders
		WHERE reminder_id = ? AND status = 'scheduled'`, reminderID))
	if errors.Is(err, sql.ErrNoRows) {
		return ReminderRecord{}, false, ErrReminderNotFound
	}
	if err != nil {
		return ReminderRecord{}, false, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE reminders
		SET status = 'cancelled', updated_at = ?, cancelled_at = ?
		WHERE reminder_id = ? AND revision = ? AND status = 'scheduled'`,
		formatTimestamp(now), formatTimestamp(now), reminderID, current.Revision); err != nil {
		return ReminderRecord{}, false, err
	}
	record := reminderRecordFromInput(reminderID, current.Revision+1, input, fingerprint, now)
	if err := store.insertReminderRow(ctx, tx, record); err != nil {
		return ReminderRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return ReminderRecord{}, false, err
	}
	return record, false, nil
}

func (store *Store) GetReminderByIdempotencyKey(ctx context.Context, key string) (*ReminderRecord, error) {
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("idempotency_key is required")
	}
	record, err := scanReminderRecord(store.db.QueryRowContext(ctx, reminderSelectColumns+`
		FROM reminders WHERE idempotency_key = ?`, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (store *Store) GetLatestReminder(ctx context.Context, reminderID string) (*ReminderRecord, error) {
	if strings.TrimSpace(reminderID) == "" {
		return nil, errors.New("reminder_id is required")
	}
	record, err := scanReminderRecord(store.db.QueryRowContext(ctx, reminderSelectColumns+`
		FROM reminders WHERE reminder_id = ? ORDER BY revision DESC LIMIT 1`, reminderID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (store *Store) GetReminderHistory(ctx context.Context, reminderID string) ([]ReminderRecord, error) {
	if strings.TrimSpace(reminderID) == "" {
		return nil, errors.New("reminder_id is required")
	}
	rows, err := store.db.QueryContext(ctx, reminderSelectColumns+`
		FROM reminders WHERE reminder_id = ? ORDER BY revision DESC`, reminderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanReminderRecords(rows)
}

func (store *Store) ListReminders(ctx context.Context, status string) ([]ReminderRecord, error) {
	query := reminderSelectColumns + ` FROM reminders`
	args := []any{}
	if status != "" && status != "all" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY run_at ASC, reminder_id ASC, revision DESC`
	rows, err := store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanReminderRecords(rows)
}

func (store *Store) ListDueReminders(ctx context.Context, now time.Time) ([]ReminderRecord, error) {
	rows, err := store.db.QueryContext(ctx, reminderSelectColumns+`
		FROM reminders
		WHERE status = 'scheduled' AND run_at <= ?
		ORDER BY run_at ASC, reminder_id ASC`, formatTimestamp(now.UTC()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanReminderRecords(rows)
}

func (store *Store) CancelReminder(ctx context.Context, reminderID string) (bool, error) {
	if strings.TrimSpace(reminderID) == "" {
		return false, errors.New("reminder_id is required")
	}
	now := time.Now().UTC()
	result, err := store.db.ExecContext(ctx, `UPDATE reminders
		SET status = 'cancelled', updated_at = ?, cancelled_at = ?
		WHERE reminder_id = ? AND status = 'scheduled'`,
		formatTimestamp(now), formatTimestamp(now), reminderID)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

func (store *Store) MarkReminderFired(ctx context.Context, reminderID string, revision int, firedAt time.Time) (bool, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE reminders
		SET status = 'fired', updated_at = ?, fired_at = ?
		WHERE reminder_id = ? AND revision = ? AND status = 'scheduled'`,
		formatTimestamp(firedAt.UTC()), formatTimestamp(firedAt.UTC()), reminderID, revision)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

func reminderRequestFingerprint(reminderID string, input ReminderInput) (string, error) {
	payload := reminderFingerprint{
		ReminderID: reminderID, RunAt: formatTimestamp(input.RunAt), Prompt: input.Prompt,
		ContextRefs: input.ContextRefs, PromptContext: input.PromptContext,
		ReplyChannelType: input.ReplyChannelType, ReplyChannelTarget: input.ReplyChannelTarget,
		ReplyChannelMetadata: input.ReplyChannelMetadata, CreatedFromTaskID: input.CreatedFromTaskID,
		CreatedBy: input.CreatedBy,
	}
	raw, err := json.Marshal(payload)
	return string(raw), err
}

func normalizeReminderInput(input ReminderInput) ReminderInput {
	input.RunAt = input.RunAt.UTC()
	input.Prompt = strings.TrimSpace(input.Prompt)
	input.ReplyChannelType = strings.TrimSpace(input.ReplyChannelType)
	input.ReplyChannelTarget = strings.TrimSpace(input.ReplyChannelTarget)
	input.CreatedFromTaskID = strings.TrimSpace(input.CreatedFromTaskID)
	input.CreatedBy = strings.TrimSpace(input.CreatedBy)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.CreatedBy == "" {
		input.CreatedBy = "api"
	}
	if input.ContextRefs == nil {
		input.ContextRefs = []string{}
	}
	if input.PromptContext == nil {
		input.PromptContext = []string{}
	}
	if input.ReplyChannelMetadata == nil {
		input.ReplyChannelMetadata = map[string]any{}
	}
	return input
}

func reminderRecordFromInput(reminderID string, revision int, input ReminderInput, fingerprint string, now time.Time) ReminderRecord {
	return ReminderRecord{
		ReminderID: reminderID, Revision: revision, Status: ReminderStatusScheduled,
		RunAt: input.RunAt, Prompt: input.Prompt, ContextRefs: input.ContextRefs,
		PromptContext: input.PromptContext, ReplyChannelType: input.ReplyChannelType,
		ReplyChannelTarget: input.ReplyChannelTarget, ReplyChannelMetadata: input.ReplyChannelMetadata,
		CreatedFromTaskID: input.CreatedFromTaskID, CreatedBy: input.CreatedBy,
		IdempotencyKey: input.IdempotencyKey, RequestFingerprint: fingerprint,
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
}

func (store *Store) insertReminderRow(ctx context.Context, exec scheduleExecer, record ReminderRecord) error {
	contextRefs, err := encodeStringList(record.ContextRefs)
	if err != nil {
		return err
	}
	promptContext, err := encodeStringList(record.PromptContext)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(record.ReplyChannelMetadata)
	if err != nil {
		return err
	}
	_, err = exec.ExecContext(ctx, `INSERT INTO reminders (
		reminder_id, revision, status, run_at, prompt, context_refs, prompt_context,
		reply_channel_type, reply_channel_target, reply_channel_metadata,
		created_from_task_id, created_by, idempotency_key, request_fingerprint,
		created_at, updated_at, fired_at, cancelled_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ReminderID, record.Revision, record.Status, formatTimestamp(record.RunAt),
		record.Prompt, contextRefs, promptContext, record.ReplyChannelType,
		record.ReplyChannelTarget, string(metadata), record.CreatedFromTaskID,
		record.CreatedBy, record.IdempotencyKey, record.RequestFingerprint,
		formatTimestamp(record.CreatedAt), formatTimestamp(record.UpdatedAt), nil, nil)
	return err
}

func scanReminderRecords(rows *sql.Rows) ([]ReminderRecord, error) {
	records := []ReminderRecord{}
	for rows.Next() {
		record, err := scanReminderRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func scanReminderRecord(scanner scheduleScanner) (ReminderRecord, error) {
	record := ReminderRecord{}
	var runAt, contextRefs, promptContext, metadata, createdAt, updatedAt string
	var firedAt, cancelledAt sql.NullString
	if err := scanner.Scan(
		&record.ReminderID, &record.Revision, &record.Status, &runAt, &record.Prompt,
		&contextRefs, &promptContext, &record.ReplyChannelType, &record.ReplyChannelTarget,
		&metadata, &record.CreatedFromTaskID, &record.CreatedBy, &record.IdempotencyKey,
		&record.RequestFingerprint, &createdAt, &updatedAt, &firedAt, &cancelledAt,
	); err != nil {
		return ReminderRecord{}, err
	}
	var err error
	if record.RunAt, err = parseTimestamp(runAt); err != nil {
		return ReminderRecord{}, err
	}
	if record.ContextRefs, err = decodeStringList(contextRefs); err != nil {
		return ReminderRecord{}, err
	}
	if record.PromptContext, err = decodeStringList(promptContext); err != nil {
		return ReminderRecord{}, err
	}
	if err := json.Unmarshal([]byte(metadata), &record.ReplyChannelMetadata); err != nil {
		return ReminderRecord{}, err
	}
	if record.CreatedAt, err = parseTimestamp(createdAt); err != nil {
		return ReminderRecord{}, err
	}
	if record.UpdatedAt, err = parseTimestamp(updatedAt); err != nil {
		return ReminderRecord{}, err
	}
	if firedAt.Valid {
		parsed, err := parseTimestamp(firedAt.String)
		if err != nil {
			return ReminderRecord{}, err
		}
		record.FiredAt = &parsed
	}
	if cancelledAt.Valid {
		parsed, err := parseTimestamp(cancelledAt.String)
		if err != nil {
			return ReminderRecord{}, err
		}
		record.CancelledAt = &parsed
	}
	return record, nil
}

func generateReminderID() (string, error) {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return "rem_" + hex.EncodeToString(buffer), nil
}

func ValidateReminderStatus(status string) error {
	switch status {
	case "", "all", ReminderStatusScheduled, ReminderStatusFired, ReminderStatusCancelled:
		return nil
	default:
		return fmt.Errorf("status must be one of scheduled, fired, cancelled, all")
	}
}
