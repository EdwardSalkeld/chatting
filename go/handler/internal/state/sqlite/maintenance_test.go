package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestMaintenanceHandlesFreshAndLegacySchemasAndPreservesActiveDelivery(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	old := "2020-01-01T00:00:00Z"
	cutoff := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if _, err := store.PruneDiagnostics(ctx, cutoff); err != nil {
		t.Fatal(err)
	}
	statements := []string{
		`CREATE TABLE audit_events (created_at TEXT, detail_json TEXT)`,
		`CREATE TABLE run_records (created_at TEXT)`,
		`CREATE TABLE dispatched_events (dispatched_at TEXT)`,
		`INSERT INTO task_ledger (task_id,envelope_id,trace_id,task_payload_json,created_at)
          VALUES ('active','e','t','{"envelope":{"source":"im","dedupe_key":"active-key"}}','2020-01-01T00:00:00Z')`,
	}
	for _, query := range statements {
		if _, err := store.db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"old-key", "active-key"} {
		if _, err := store.db.ExecContext(ctx, `INSERT INTO idempotency_keys VALUES ('im',?,?)`, key, old); err != nil {
			t.Fatal(err)
		}
	}
	for _, task := range []string{"done", "active"} {
		if _, err := store.db.ExecContext(ctx, `INSERT INTO dispatched_event_ids(task_id,event_id,dispatched_at) VALUES (?,?,?)`, task, task, old); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 105; i++ {
		if _, err := store.db.ExecContext(ctx, `INSERT INTO audit_events VALUES (?, '{}')`, old); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO audit_events VALUES (?, '{}')`, cutoff.Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	counts, err := store.PruneDiagnostics(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if counts["audit_events"] != 105 || counts["idempotency_keys"] != 1 || counts["dispatched_event_ids"] != 1 {
		t.Fatal(counts)
	}
	for _, table := range []string{"audit_events", "idempotency_keys", "dispatched_event_ids", "task_ledger"} {
		var n int
		if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s: %d %v", table, n, err)
		}
	}
	counts, err = store.PruneDiagnostics(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	for table, count := range counts {
		if count != 0 {
			t.Fatalf("repeat %s=%d", table, count)
		}
	}
}
