package sqlite

import (
	"context"
	"database/sql"
	"errors"
)

func (store *Store) GetZulipCursor(ctx context.Context, scope string) (*int64, error) {
	var cursor int64
	err := store.db.QueryRowContext(ctx, `SELECT message_id FROM zulip_cursors WHERE scope = ?`, scope).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cursor, nil
}

func (store *Store) SetZulipCursor(ctx context.Context, scope string, cursor int64) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO zulip_cursors(scope,message_id) VALUES (?,?) ON CONFLICT(scope) DO UPDATE SET message_id=MAX(message_id,excluded.message_id)`, scope, cursor)
	return err
}
