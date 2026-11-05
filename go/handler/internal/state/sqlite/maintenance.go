package sqlite

import (
	"context"
	"fmt"
	"time"
)

// PruneDiagnostics owns the handler's retention SQL, including optional tables
// left by older handler versions. Each statement commits at most 100 rows.
func (store *Store) PruneDiagnostics(ctx context.Context, diagnosticCutoff, deliveryCutoff time.Time) (map[string]int64, error) {
	tables := []struct {
		name, column, guard string
		cutoff              time.Time
	}{
		{"audit_events", "created_at", "1", diagnosticCutoff},
		{"run_records", "created_at", "1", diagnosticCutoff},
		{"dispatched_event_ids", "dispatched_at", "NOT EXISTS (SELECT 1 FROM task_ledger WHERE task_id = dispatched_event_ids.task_id)", deliveryCutoff},
		{"idempotency_keys", "seen_at", `NOT EXISTS (SELECT 1 FROM task_ledger
            WHERE json_extract(task_payload_json, '$.envelope.source') = idempotency_keys.source
              AND json_extract(task_payload_json, '$.envelope.dedupe_key') = idempotency_keys.dedupe_key)`, deliveryCutoff},
		{"dispatched_events", "dispatched_at", "1", deliveryCutoff},
		{"completed_task_ledger", "completed_at", "NOT EXISTS (SELECT 1 FROM task_ledger WHERE task_id = completed_task_ledger.task_id)", deliveryCutoff},
	}
	counts := map[string]int64{}
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
			result, err := store.db.ExecContext(ctx, query, formatTimestamp(table.cutoff))
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
