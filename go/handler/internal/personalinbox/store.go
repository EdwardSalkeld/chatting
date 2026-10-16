package personalinbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	_ "modernc.org/sqlite"
)

type Message struct {
	ID       string `json:"id"`
	ThreadID string `json:"thread_id,omitempty"`
	Mail     Mail   `json:"mail"`
	// What the body looked like before the model saw it, for diagnosis.
	BodyHTML       bool            `json:"body_html,omitempty"`
	BodyTruncated  bool            `json:"body_truncated,omitempty"`
	BodyChars      int             `json:"body_chars,omitempty"`
	Classification *Classification `json:"classification,omitempty"`
	Created        int64           `json:"created"`
	Published      int64           `json:"published"`
	DigestID       string          `json:"digest_id,omitempty"`
}

type Effect struct {
	ID        string `json:"id"`
	MessageID string `json:"message_id"`
	Kind      string `json:"kind"`
	Category  string `json:"category,omitempty"`
	Target    string `json:"target"`
	Body      string `json:"body"`
	Status    string `json:"status"`
	Receipt   string `json:"receipt,omitempty"`
	Note      string `json:"note,omitempty"`
}

type Store struct {
	db   *sql.DB
	lock *os.File
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	// Create with private permissions before SQLite opens it. Do not change a
	// pre-existing file's permissions behind the operator's back.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	_ = file.Close()
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errors.New("personal inbox database already owned by another process")
	}
	opened := false
	defer func() {
		if !opened {
			_ = lock.Close()
		}
	}()
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path}).String()+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS inbox_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS inbox_messages (id TEXT PRIMARY KEY, data TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS inbox_effects (id TEXT PRIMARY KEY, data TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS inbox_digests (id TEXT PRIMARY KEY, data TEXT NOT NULL);
		CREATE INDEX IF NOT EXISTS inbox_messages_unclassified ON inbox_messages(json_extract(data,'$.classification'),json_extract(data,'$.published'));
		CREATE INDEX IF NOT EXISTS inbox_messages_digest ON inbox_messages(json_extract(data,'$.classification.category'),json_extract(data,'$.digest_id'),json_extract(data,'$.created'));
		CREATE INDEX IF NOT EXISTS inbox_effects_status ON inbox_effects(json_extract(data,'$.status'));
		CREATE INDEX IF NOT EXISTS inbox_effects_message ON inbox_effects(json_extract(data,'$.message_id'));
		UPDATE inbox_effects SET data=json_set(data, '$.status', 'uncertain', '$.note', 'interrupted before durable receipt') WHERE json_extract(data, '$.status')='inflight';`)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	opened = true
	return &Store{db: db, lock: lock}, nil
}

func (store *Store) Close() error { err := store.db.Close(); _ = store.lock.Close(); return err }

func (store *Store) getMeta(ctx context.Context, key string) (string, error) {
	var value string
	err := store.db.QueryRowContext(ctx, "SELECT value FROM inbox_meta WHERE key=?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

// Health of each cycle step, so a reader can tell a workflow that is wholly
// down from one where only ingestion is degraded.
func (store *Store) stepHealth(ctx context.Context) (map[string]map[string]string, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT key,value FROM inbox_meta WHERE key LIKE 'step_success:%' OR key LIKE 'step_error:%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	health := map[string]map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		field, step, found := strings.Cut(key, ":")
		if !found {
			continue
		}
		if health[step] == nil {
			health[step] = map[string]string{}
		}
		health[step][strings.TrimPrefix(field, "step_")] = value
	}
	return health, rows.Err()
}

func (store *Store) putMeta(ctx context.Context, key, value string) error {
	_, err := store.db.ExecContext(ctx, "INSERT INTO inbox_meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value)
	return err
}

func saveJSON(ctx context.Context, tx *sql.Tx, table, id string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+table+" (id,data) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", id, string(encoded))
	return err
}

func (store *Store) saveMessage(ctx context.Context, message Message, effects []Effect) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := saveJSON(ctx, tx, "inbox_messages", message.ID, message); err != nil {
		return err
	}
	for _, effect := range effects {
		if err := saveJSON(ctx, tx, "inbox_effects", effect.ID, effect); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// insertMessage keeps the first record of a message. A replayed change feed
// must not overwrite a classification that has already been acted on.
func (store *Store) insertMessage(ctx context.Context, message Message) error {
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, "INSERT OR IGNORE INTO inbox_messages (id,data) VALUES (?,?)", message.ID, string(encoded))
	return err
}

func (store *Store) messages(ctx context.Context, predicate string, args ...any) ([]Message, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT data FROM inbox_messages WHERE "+predicate+" ORDER BY json_extract(data,'$.created'), id LIMIT 100", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Message{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value Message
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (store *Store) effects(ctx context.Context, predicate string, args ...any) ([]Effect, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT data FROM inbox_effects WHERE "+predicate+" ORDER BY id LIMIT 100", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Effect{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value Effect
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (store *Store) saveEffect(ctx context.Context, effect Effect) error {
	encoded, err := json.Marshal(effect)
	if err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, "UPDATE inbox_effects SET data=? WHERE id=?", string(encoded), effect.ID)
	return err
}

// DigestLine is one message rendered for the daily round-up: who sent it and
// what it was actually about, which the subject line frequently does not say.
type DigestLine struct {
	Ref    string `json:"ref"`
	Sender string `json:"sender"`
	Line   string `json:"line"`
}

type Digest struct {
	ID        string       `json:"id"`
	Messages  []string     `json:"messages"`
	Created   int64        `json:"created"`
	Published int64        `json:"published"`
	Lines     []DigestLine `json:"lines,omitempty"`
	Sent      bool         `json:"sent"`
}

// createDigest freezes the membership of one digest and stamps every message in
// it, in one transaction, so a replay cannot regroup messages into a second
// round-up or leave a message belonging to two.
func (store *Store) createDigest(ctx context.Context, digest Digest, messages []Message) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := saveJSON(ctx, tx, "inbox_digests", digest.ID, digest); err != nil {
		return err
	}
	for _, message := range messages {
		message.DigestID = digest.ID
		if err := saveJSON(ctx, tx, "inbox_messages", message.ID, message); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO inbox_meta VALUES ('digest_occurrence',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", digest.ID); err != nil {
		return err
	}
	return tx.Commit()
}

// putEffect writes an effect that may not exist yet; saveEffect only updates.
func putEffect(ctx context.Context, tx *sql.Tx, effect Effect) error {
	return saveJSON(ctx, tx, "inbox_effects", effect.ID, effect)
}

// completeDigest records the rendered round-up and the one message that sends
// it together, so a crash cannot leave a digest marked sent with nothing queued.
func (store *Store) completeDigest(ctx context.Context, digest Digest, effect Effect) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := putEffect(ctx, tx, effect); err != nil {
		return err
	}
	if err := saveJSON(ctx, tx, "inbox_digests", digest.ID, digest); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) saveDigest(ctx context.Context, digest Digest) error {
	encoded, err := json.Marshal(digest)
	if err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, "INSERT INTO inbox_digests (id,data) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", digest.ID, string(encoded))
	return err
}

func (store *Store) digests(ctx context.Context, predicate string, args ...any) ([]Digest, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT data FROM inbox_digests WHERE "+predicate+" ORDER BY id LIMIT 100", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Digest{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var value Digest
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}
