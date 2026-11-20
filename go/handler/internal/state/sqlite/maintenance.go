package sqlite

import (
	"context"
	"fmt"
	"time"
)

// PruneDiagnostics owns the handler's retention SQL, including optional tables
// left by older handler versions. Each statement commits at most 100 rows.
func (store *Store) PruneDiagnostics(ctx context.Context, cutoff time.Time) (map[string]int64, error) {
	// Expire unresolved holds in bounded transactions, preserving a summary for
	// another retention period so audits can distinguish expiry from delivery.
	var expired int64
	for {
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO github_webhook_hold_outcomes
			SELECT event_key,event,delivery,reason,received_at,'expired','retention_limit','',?
			FROM github_webhook_held WHERE julianday(received_at) < julianday(?) ORDER BY received_at,event_key LIMIT 100`, formatTimestamp(time.Now()), formatTimestamp(cutoff))
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM github_webhook_held WHERE event_key IN
			(SELECT event_key FROM github_webhook_held WHERE julianday(received_at) < julianday(?) ORDER BY received_at,event_key LIMIT 100)`, formatTimestamp(cutoff))
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		expired += n
		if n < 100 {
			break
		}
	}
	tables := []struct {
		name, column, guard string
	}{
		{"audit_events", "created_at", "1"},
		{"run_records", "created_at", "1"},
		{"dispatched_event_ids", "dispatched_at", "NOT EXISTS (SELECT 1 FROM task_ledger WHERE task_id = dispatched_event_ids.task_id)"},
		{"idempotency_keys", "seen_at", `NOT EXISTS (SELECT 1 FROM task_ledger
            WHERE json_extract(task_payload_json, '$.envelope.source') = idempotency_keys.source
              AND json_extract(task_payload_json, '$.envelope.dedupe_key') = idempotency_keys.dedupe_key)`},
		{"dispatched_events", "dispatched_at", "1"},
		{"completed_task_ledger", "completed_at", "NOT EXISTS (SELECT 1 FROM task_ledger WHERE task_id = completed_task_ledger.task_id)"},
		{"github_webhook_hold_outcomes", "resolved_at", "1"},
	}
	counts := map[string]int64{"github_webhook_held_expired": expired}
	for _, table := range tables {
		var exists int
		if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", table.name).Scan(&exists); err != nil {
			return nil, err
		}
		if exists == 0 {
			continue
		}
		counts[table.name] = 0
		for {
			// Table/column/guard come only from the fixed schema list above.
			query := fmt.Sprintf("DELETE FROM %s WHERE rowid IN (SELECT rowid FROM %s WHERE julianday(%s) < julianday(?) AND (%s) LIMIT 100)", table.name, table.name, table.column, table.guard)
			result, err := store.db.ExecContext(ctx, query, formatTimestamp(cutoff))
			if err != nil {
				return nil, fmt.Errorf("prune %s: %w", table.name, err)
			}
			n, err := result.RowsAffected()
			if err != nil {
				return nil, err
			}
			counts[table.name] += n
			if n < 100 {
				break
			}
		}
	}
	return counts, nil
}
