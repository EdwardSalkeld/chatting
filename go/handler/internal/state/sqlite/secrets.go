package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type SecretRequest struct {
	ID, TokenHash, WorkItemID, Label, ExpiresAt, Status string
	Reply                                               contracts.ReplyChannel
}

var ErrSecretUsed = errors.New("request already used")

func (s *Store) CreateSecretRequest(ctx context.Context, taskID string, r SecretRequest) error {
	result, err := s.db.ExecContext(ctx, `INSERT INTO secret_requests SELECT ?,?,a.work_item_id,?,?,?,w.preferred_reply_json FROM task_assignments a JOIN work_items w USING(work_item_id) WHERE a.task_id=?`, r.ID, r.TokenHash, r.Label, r.ExpiresAt, "pending", taskID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func scanSecret(row interface{ Scan(...any) error }) (SecretRequest, error) {
	var r SecretRequest
	var reply string
	err := row.Scan(&r.ID, &r.TokenHash, &r.WorkItemID, &r.Label, &r.ExpiresAt, &r.Status, &reply)
	if err == nil {
		err = json.Unmarshal([]byte(reply), &r.Reply)
	}
	return r, err
}

const secretColumns = `id,token_hash,work_item_id,label,expires_at,status,reply_json`

func (s *Store) SecretByHash(ctx context.Context, hash string) (SecretRequest, error) {
	return scanSecret(s.db.QueryRowContext(ctx, `SELECT `+secretColumns+` FROM secret_requests WHERE token_hash=?`, hash))
}

// CompleteSecret serializes submission with all other operations on this store.
// save must atomically publish the file; it may report that a previous attempt
// already published it after a crash before the database commit.
func (s *Store) CompleteSecret(ctx context.Context, hash string, save func(SecretRequest) error) (SecretRequest, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SecretRequest{}, err
	}
	defer rollbackUnlessCommitted(tx)
	r, err := scanSecret(tx.QueryRowContext(ctx, `SELECT `+secretColumns+` FROM secret_requests WHERE token_hash=?`, hash))
	if err != nil {
		return r, err
	}
	if r.Status != "pending" {
		return r, ErrSecretUsed
	}
	if err = save(r); err != nil {
		return r, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE secret_requests SET status='ready' WHERE id=?`, r.ID); err != nil {
		return r, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO task_assignments VALUES (?,?,?)`, "task:secret:"+r.ID, r.WorkItemID, "secret_request"); err != nil {
		return r, err
	}
	err = tx.Commit()
	return r, err
}

func (s *Store) PendingSecrets(ctx context.Context) ([]SecretRequest, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+secretColumns+` FROM secret_requests WHERE status IN ('pending','ready')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SecretRequest
	for rows.Next() {
		r, err := scanSecret(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) AckSecret(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE secret_requests SET status='delivered' WHERE id=? AND status='ready'`, id)
	return err
}

func (s *Store) ExpireSecret(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE secret_requests SET status='expired' WHERE id=? AND status='pending'`, id)
	return err
}

func (r SecretRequest) Expired(now time.Time) bool {
	t, err := time.Parse(time.RFC3339, r.ExpiresAt)
	return err != nil || !now.Before(t)
}
