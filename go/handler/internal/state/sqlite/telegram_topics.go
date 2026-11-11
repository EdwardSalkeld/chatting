package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func (store *Store) TelegramConversation(ctx context.Context, taskID string) (*contracts.ReplyChannel, error) {
	var itemID, encoded string
	if err := store.db.QueryRowContext(ctx, `SELECT a.work_item_id,w.preferred_reply_json FROM task_assignments a JOIN work_items w USING(work_item_id) WHERE a.task_id=?`, taskID).Scan(&itemID, &encoded); err != nil {
		return nil, err
	}
	var origin contracts.ReplyChannel
	if err := json.Unmarshal([]byte(encoded), &origin); err != nil {
		return nil, err
	}
	if origin.Type == "telegram" {
		metadata := map[string]any{}
		for _, key := range []string{"chat_title", "chat_type", "message_thread_id", "topic_name"} {
			if value, ok := origin.Metadata[key]; ok {
				metadata[key] = value
			}
		}
		origin.Metadata = metadata
		return &origin, nil
	}
	var chatID, name string
	var topicID int64
	err := store.db.QueryRowContext(ctx, `SELECT chat_id,name,topic_id FROM work_item_telegram_topics WHERE work_item_id=? AND topic_id IS NOT NULL`, itemID).Scan(&chatID, &name, &topicID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &contracts.ReplyChannel{Type: "telegram", Target: chatID, Metadata: map[string]any{"message_thread_id": topicID, "topic_name": name}}, nil
}

func (store *Store) ReserveTelegramTopic(ctx context.Context, taskID, chatID, name string) (bool, error) {
	result, err := store.db.ExecContext(ctx, `INSERT OR IGNORE INTO work_item_telegram_topics(work_item_id,chat_id,name) SELECT work_item_id,?,? FROM task_assignments WHERE task_id=?`, chatID, name, taskID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (store *Store) RejectTelegramTopic(ctx context.Context, taskID string) error {
	_, err := store.db.ExecContext(ctx, `DELETE FROM work_item_telegram_topics WHERE work_item_id=(SELECT work_item_id FROM task_assignments WHERE task_id=?) AND topic_id IS NULL`, taskID)
	return err
}

func (store *Store) BindTelegramTopic(ctx context.Context, taskID, chatID, name string, topicID int64) error {
	if topicID <= 0 {
		return fmt.Errorf("topic_id must be positive")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(tx)
	var itemID string
	if err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM task_assignments WHERE task_id=?`, taskID).Scan(&itemID); err != nil {
		return err
	}
	key, _ := json.Marshal(map[string]any{"chat": chatID, "topic": topicID})
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO work_item_routes VALUES('telegram',?,?)`, string(key), itemID); err != nil {
		return err
	}
	var owner string
	if err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_routes WHERE route_kind='telegram' AND route_key=?`, string(key)).Scan(&owner); err != nil {
		return err
	}
	if owner != itemID {
		return fmt.Errorf("topic already belongs to another workspace")
	}
	// A recovery may supply the known topic ID after a lost creation response.
	// It must not replace an already attached conversation or change forums.
	result, err := tx.ExecContext(ctx, `INSERT INTO work_item_telegram_topics(work_item_id,chat_id,name,topic_id) VALUES(?,?,?,?)
        ON CONFLICT(work_item_id) DO UPDATE SET topic_id=excluded.topic_id,name=excluded.name
        WHERE work_item_telegram_topics.chat_id=excluded.chat_id AND (work_item_telegram_topics.topic_id IS NULL OR work_item_telegram_topics.topic_id=excluded.topic_id)`, itemID, chatID, name, topicID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("workspace already has a different topic reservation")
	}
	return tx.Commit()
}
