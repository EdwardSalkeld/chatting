package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type webhookTx struct{ tx *sql.Tx }

func (s webhookTx) WebhookState(ctx context.Context, kind, key string) (string, error) {
	table, err := webhookTable(kind)
	if err != nil {
		return "", err
	}
	var value string
	err = s.tx.QueryRowContext(ctx, "SELECT state FROM "+table+" WHERE "+kind+"_key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}
func (s webhookTx) PutWebhookState(ctx context.Context, kind, key, value string) error {
	table, err := webhookTable(kind)
	if err != nil {
		return err
	}
	_, err = s.tx.ExecContext(ctx, "INSERT INTO "+table+" ("+kind+"_key, state) VALUES (?, ?) ON CONFLICT ("+kind+"_key) DO UPDATE SET state = excluded.state", key, value)
	return err
}
func (s webhookTx) Seen(ctx context.Context, source, key string) (bool, error) {
	return exists(s.tx.QueryRowContext(ctx, `SELECT 1 FROM idempotency_keys WHERE source = ? AND dedupe_key = ?`, source, key))
}
func (s webhookTx) MarkSeen(ctx context.Context, source, key string) error {
	_, err := s.tx.ExecContext(ctx, `INSERT OR IGNORE INTO idempotency_keys VALUES (?, ?, ?)`, source, key, formatTimestamp(time.Now()))
	return err
}

// WebhookTransaction commits filtering, dedupe and staging together.
func (store *Store) WebhookTransaction(ctx context.Context, receive func(contracts.WebhookTransaction) error) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(tx)
	if err := receive(webhookTx{tx}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s webhookTx) StageGitHub(ctx context.Context, envelope contracts.TaskEnvelope, enabled bool) error {
	if repo, ok := envelope.ReplyChannel.Metadata["github_repository"].(string); ok {
		var item string
		err := s.tx.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_artifacts WHERE kind = 'github_repository' AND artifact_key = ?`, strings.ToLower(repo)).Scan(&item)
		if err == sql.ErrNoRows {
			return contracts.ErrGitHubNeedsContext
		}
		if err != nil {
			return err
		}
	}
	if !enabled {
		return nil
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = s.tx.ExecContext(ctx, `INSERT OR IGNORE INTO github_webhook_pending VALUES (?, ?, ?)`, envelope.ID, string(encoded), formatTimestamp(time.Now()))
	return err
}

func (store *Store) PendingGitHub(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT envelope_json FROM github_webhook_pending ORDER BY created_at, envelope_id LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []contracts.TaskEnvelope
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var envelope contracts.TaskEnvelope
		if err := json.Unmarshal([]byte(encoded), &envelope); err != nil {
			return nil, err
		}
		result = append(result, envelope)
	}
	return result, rows.Err()
}

func (store *Store) AckGitHub(ctx context.Context, id string) error {
	_, err := store.db.ExecContext(ctx, `DELETE FROM github_webhook_pending WHERE envelope_id = ?`, id)
	return err
}

func (store *Store) HoldGitHub(ctx context.Context, key, event, delivery string, body []byte, reason string, now time.Time) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO github_webhook_held VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_key) DO UPDATE SET reason = excluded.reason`, key, event, delivery, body, reason, formatTimestamp(now))
	return err
}

func (store *Store) ClearHeldGitHub(ctx context.Context, key string) error {
	_, err := store.db.ExecContext(ctx, `DELETE FROM github_webhook_held WHERE event_key = ?`, key)
	return err
}
