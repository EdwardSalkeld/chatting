package sqlite

import (
	"context"
	"database/sql"
)

// RecordInstructionsUpdate records a hint to refresh, not a trusted Git revision.
func (store *Store) RecordInstructionsUpdate(ctx context.Context) error {
	_, err := store.db.ExecContext(ctx, `INSERT INTO instructions_updates (id, generation) VALUES (1, 1) ON CONFLICT(id) DO UPDATE SET generation = generation + 1`)
	return err
}

func (store *Store) InstructionsGeneration(ctx context.Context) (int64, error) {
	var generation int64
	err := store.db.QueryRowContext(ctx, `SELECT generation FROM instructions_updates WHERE id = 1`).Scan(&generation)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return generation, err
}
