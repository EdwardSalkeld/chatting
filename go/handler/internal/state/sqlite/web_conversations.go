package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

var ErrWebConflict = errors.New("message id already used with different content")

type WebConversation struct {
	ID         string `json:"id"`
	WorkItemID string `json:"work_item_id"`
	Target     string `json:"target"`
	TopicID    *int64 `json:"topic_id"`
	Title      string `json:"title"`
}
type WebSubmission struct {
	TaskID    string `json:"task_id"`
	Content   string `json:"content"`
	CreatedAt string `json:"occurred_at"`
	State     string `json:"state"`
}

func (store *Store) ListWebConversations(ctx context.Context) ([]WebConversation, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT r.route_key,r.work_item_id, w.preferred_reply_json, COALESCE(c.title,'') FROM work_item_routes r JOIN work_items w USING(work_item_id) LEFT JOIN telegram_chat_registry c ON c.chat_id=json_extract(r.route_key,'$.chat') WHERE r.route_kind='telegram' ORDER BY r.work_item_id,r.route_key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []WebConversation{}
	for rows.Next() {
		var key, origin, label string
		var c WebConversation
		if err := rows.Scan(&key, &c.WorkItemID, &origin, &label); err != nil {
			return nil, err
		}
		var route struct {
			Chat  string `json:"chat"`
			Topic *int64 `json:"topic"`
		}
		if err := json.Unmarshal([]byte(key), &route); err != nil {
			return nil, err
		}
		c.ID = base64.RawURLEncoding.EncodeToString([]byte(key))
		c.Target = route.Chat
		c.TopicID = route.Topic
		c.Title = label
		var reply contracts.ReplyChannel
		if json.Unmarshal([]byte(origin), &reply) == nil && c.Title == "" {
			c.Title, _ = reply.Metadata["chat_title"].(string)
		}
		if c.Title == "" {
			c.Title = c.Target
		}
		if c.TopicID != nil {
			c.Title += fmt.Sprintf(" · topic %d", *c.TopicID)
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// QueueWebMessage atomically records the chosen existing lane and a durable ingress
// submission. The browser cannot supply task instructions, paths, actor or lane ID.
func (store *Store) QueueWebMessage(ctx context.Context, id, clientID, content string, refs []string) (WebSubmission, error) {
	conversations, err := store.ListWebConversations(ctx)
	if err != nil {
		return WebSubmission{}, err
	}
	var selected *WebConversation
	for _, c := range conversations {
		if c.ID == id {
			v := c
			selected = &v
			break
		}
	}
	if selected == nil {
		return WebSubmission{}, sql.ErrNoRows
	}
	envelopeID := "web:" + id + ":" + clientID
	taskID := "task:" + envelopeID
	now := time.Now().UTC()
	actor := "web operator"
	metadata := map[string]any{"transport": "web", "telegram_target": selected.Target, "chat_title": selected.Title}
	if selected.TopicID != nil {
		metadata["message_thread_id"] = *selected.TopicID
	}
	envelope := contracts.ReplyChannel{
		Type: "telegram", Target: selected.Target,
		Metadata: map[string]any{"chat_title": selected.Title},
	}
	e := contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            envelopeID, Source: "im", ReceivedAt: contracts.NewTimestamp(now),
		Actor: &actor, Content: content, DedupeKey: envelopeID,
		Attachments:          []contracts.AttachmentRef{},
		ContextRefs:          append([]string{}, refs...),
		ConversationChannels: []contracts.ReplyChannel{envelope},
		ReplyChannel:         contracts.ReplyChannel{Type: "web", Target: id, Metadata: metadata},
		PromptContext: &contracts.PromptContext{ReplyChannelInstructions: []string{
			"This message was sent through the web conversation. Answer with app.main_reply using channel web and this event's target. " +
				"Web replies are text-only. Do not send the answer to Telegram unless the user explicitly asks. " +
				"Existing Telegram conversations provide context, not a request to cross-post.",
		}},
	}
	if selected.TopicID != nil {
		e.ConversationChannels[0].Metadata["message_thread_id"] = *selected.TopicID
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return WebSubmission{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return WebSubmission{}, err
	}
	defer rollbackUnlessCommitted(tx)
	var existing string
	err = tx.QueryRowContext(ctx, "SELECT envelope_json FROM web_ingress WHERE envelope_id=?", envelopeID).Scan(&existing)
	if err == nil {
		var previous contracts.TaskEnvelope
		if json.Unmarshal([]byte(existing), &previous) != nil || previous.Content != content {
			return WebSubmission{}, ErrWebConflict
		}
		return WebSubmission{TaskID: taskID, Content: content, CreatedAt: formatTimestamp(previous.ReceivedAt.Time), State: "queued"}, nil
	}
	if err != sql.ErrNoRows {
		return WebSubmission{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO task_assignments(task_id,work_item_id,route_reason) VALUES(?,?,'web_conversation')", taskID, selected.WorkItemID); err != nil {
		return WebSubmission{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO web_ingress(envelope_id,conversation_id,envelope_json,published_at) VALUES(?,?,?,NULL)", envelopeID, id, string(raw)); err != nil {
		return WebSubmission{}, err
	}
	if err = tx.Commit(); err != nil {
		return WebSubmission{}, err
	}
	return WebSubmission{TaskID: taskID, Content: content, CreatedAt: formatTimestamp(now), State: "queued"}, nil
}
func (store *Store) PendingWebEnvelopes(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT envelope_json FROM web_ingress WHERE published_at IS NULL ORDER BY rowid LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []contracts.TaskEnvelope{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var e contracts.TaskEnvelope
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
func (store *Store) MarkWebPublished(ctx context.Context, id string) error {
	_, err := store.db.ExecContext(ctx, "UPDATE web_ingress SET published_at=? WHERE envelope_id=?", formatTimestamp(time.Now()), id)
	return err
}
func (store *Store) WebSubmissions(ctx context.Context, id string) ([]WebSubmission, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT w.envelope_id,w.envelope_json FROM web_ingress w WHERE conversation_id=? AND (published_at IS NULL OR EXISTS(SELECT 1 FROM task_ledger t WHERE t.task_id='task:'||w.envelope_id)) ORDER BY w.rowid DESC LIMIT 100", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []WebSubmission{}
	for rows.Next() {
		var eid, raw string
		if err := rows.Scan(&eid, &raw); err != nil {
			return nil, err
		}
		var e contracts.TaskEnvelope
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, err
		}
		result = append(result, WebSubmission{TaskID: "task:" + eid, Content: e.Content, CreatedAt: formatTimestamp(e.ReceivedAt.Time), State: "queued"})
	}
	return result, rows.Err()
}
