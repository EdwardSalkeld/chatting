package workitems

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type TopicStore interface {
	TelegramConversation(context.Context, string) (*contracts.ReplyChannel, error)
	ReserveTelegramTopic(context.Context, string, string, string) (bool, error)
	BindTelegramTopic(context.Context, string, string, string, int64) error
	RejectTelegramTopic(context.Context, string) error
}

type TopicCreator interface {
	// rejected is true only when Telegram explicitly rejected creation. A lost
	// response is ambiguous and must not lead to another creation request.
	CreateForumTopic(context.Context, string, string) (topicID int64, rejected bool, err error)
}

type TelegramTopics struct {
	Store          TopicStore
	Creator        TopicCreator
	DefaultChatID  string
	AllowedChatIDs []string
}

func (service TelegramTopics) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/work-items/telegram-topic", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, `{"reason":"POST required"}`, 405)
			return
		}
		var input struct {
			TaskID  string `json:"task_id"`
			ChatID  string `json:"chat_id"`
			Name    string `json:"name"`
			TopicID int64  `json:"topic_id"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input); err != nil || input.TaskID == "" {
			http.Error(w, `{"reason":"task_id is required; name, chat_id and topic_id are optional"}`, 400)
			return
		}
		conversation, err := service.Ensure(r.Context(), input.TaskID, input.ChatID, input.Name, input.TopicID)
		if err != nil {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"reason": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"conversation": conversation})
	})
}

func (service TelegramTopics) Ensure(ctx context.Context, taskID, chatID, name string, existingTopicID int64) (*contracts.ReplyChannel, error) {
	conversation, err := service.Store.TelegramConversation(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if conversation != nil {
		return conversation, nil
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		chatID = service.DefaultChatID
	}
	allowed := false
	for _, id := range service.AllowedChatIDs {
		if id == chatID {
			allowed = true
		}
	}
	if chatID == "" || !allowed {
		return nil, fmt.Errorf("choose an allowed forum chat_id or configure telegram_workspace_forum_chat_id")
	}
	name = strings.TrimSpace(name)
	if existingTopicID < 0 || (existingTopicID == 0 && (name == "" || utf8.RuneCountInString(name) > 128)) {
		return nil, fmt.Errorf("name must contain 1–128 characters; topic_id must be positive when attaching an existing topic")
	}
	if existingTopicID > 0 {
		if err := service.Store.BindTelegramTopic(ctx, taskID, chatID, name, existingTopicID); err != nil {
			return nil, err
		}
		return service.Store.TelegramConversation(ctx, taskID)
	}
	if service.Creator == nil {
		return nil, fmt.Errorf("Telegram topic creation is not enabled")
	}
	reserved, err := service.Store.ReserveTelegramTopic(ctx, taskID, chatID, name)
	if err != nil {
		return nil, err
	}
	if !reserved {
		conversation, err := service.Store.TelegramConversation(ctx, taskID)
		if err != nil {
			return nil, err
		}
		if conversation != nil {
			return conversation, nil
		}
		return nil, fmt.Errorf("topic creation is pending or its outcome is unknown; inspect the forum and attach the existing topic_id instead of creating another")
	}
	topicID, rejected, err := service.Creator.CreateForumTopic(ctx, chatID, name)
	if err != nil {
		if rejected {
			if cleanupErr := service.Store.RejectTelegramTopic(ctx, taskID); cleanupErr != nil {
				return nil, cleanupErr
			}
		}
		return nil, fmt.Errorf("topic creation failed (safe to retry: %t): %w", rejected, err)
	}
	// Finish independently of the HTTP request's lifetime: cancellation after
	// Telegram succeeds must not prevent the known topic from being attached.
	if err := service.Store.BindTelegramTopic(context.WithoutCancel(ctx), taskID, chatID, name, topicID); err != nil {
		return nil, fmt.Errorf("Telegram created topic %d in %s but binding failed; attach that topic_id: %w", topicID, chatID, err)
	}
	return service.Store.TelegramConversation(context.WithoutCancel(ctx), taskID)
}
