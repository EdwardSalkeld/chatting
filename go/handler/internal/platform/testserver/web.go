package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func seedWebConversations(path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	for i, title := range []string{"Technical", "Music"} {
		item := fmt.Sprintf("item_fixture_%d", i)
		topic := i + 7
		origin, _ := json.Marshal(map[string]any{"type": "telegram", "target": "fixture-chat", "metadata": map[string]any{"message_thread_id": topic, "chat_title": title}})
		route, _ := json.Marshal(map[string]any{"chat": "fixture-chat", "topic": topic})
		if _, err = db.Exec("INSERT OR IGNORE INTO work_items VALUES(?,?,?)", item, string(origin), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if _, err = db.Exec("INSERT OR IGNORE INTO work_item_routes VALUES('telegram',?,?)", string(route), item); err != nil {
			return err
		}
	}
	return nil
}
