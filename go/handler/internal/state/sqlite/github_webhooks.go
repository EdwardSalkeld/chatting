package sqlite

import (
	"context"
	"database/sql"
	"errors"
)

// WebhookState is separate from task/work-item state: this receiver only logs.
func (store *Store) WebhookState(ctx context.Context, kind, key string) (string, error) {
	table, err := webhookTable(kind)
	if err != nil {
		return "", err
	}
	var value string
	err = store.db.QueryRowContext(ctx, "SELECT state FROM "+table+" WHERE "+kind+"_key = ?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (store *Store) PutWebhookState(ctx context.Context, kind, key, value string) error {
	table, err := webhookTable(kind)
	if err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, "INSERT INTO "+table+" ("+kind+"_key, state) VALUES (?, ?) ON CONFLICT ("+kind+"_key) DO UPDATE SET state = excluded.state", key, value)
	return err
}

func webhookTable(kind string) (string, error) {
	switch kind {
	case "watch":
		return "github_webhook_watches", nil
	case "workflow":
		return "github_webhook_workflows", nil
	default:
		return "", errors.New("unknown webhook state kind")
	}
}
