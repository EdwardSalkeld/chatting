package personalinbox

import (
	"context"
	"encoding/json"
)

type MessageView struct {
	Message
	Status string `json:"status"`
}

func (store *Store) messageViews(ctx context.Context, after string) ([]MessageView, error) {
	rows, err := store.db.QueryContext(ctx, "SELECT data FROM inbox_messages WHERE id>? ORDER BY id LIMIT 100", after)
	if err != nil {
		return nil, err
	}
	views := []MessageView{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return nil, err
		}
		var value MessageView
		if err := json.Unmarshal([]byte(raw), &value.Message); err != nil {
			_ = rows.Close()
			return nil, err
		}
		views = append(views, value)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	for index := range views {
		view := &views[index]
		if view.Classification == nil {
			view.Status = "awaiting_classification"
			continue
		}
		effects, err := store.effects(ctx, "json_extract(data,'$.message_id')=? OR id=?", view.ID, view.DigestID)
		if err != nil {
			return nil, err
		}
		statuses := map[string]bool{}
		for _, effect := range effects {
			statuses[effect.Status] = true
		}
		switch {
		case statuses["uncertain"] || statuses["inflight"]:
			view.Status = "needs_reconciliation"
		case statuses["preview"]:
			view.Status = "preview"
		case statuses["pending"]:
			view.Status = "processing"
		case view.Classification.Category == "review":
			view.Status = "needs_review"
		case view.Classification.Category == "news" && view.DigestID == "":
			view.Status = "awaiting_digest"
		default:
			view.Status = "processed"
		}
	}
	return views, nil
}
