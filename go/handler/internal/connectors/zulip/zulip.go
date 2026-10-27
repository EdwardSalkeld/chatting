package zulip

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type CursorStore interface {
	GetZulipCursor(context.Context, string) (*int64, error)
	SetZulipCursor(context.Context, string, int64) error
}

type Connector struct {
	client   *Client
	store    CursorStore
	scope    string
	pending  map[string]int64
	batchEnd int64
}

func New(config Config, store CursorStore) (*Connector, error) {
	client, err := NewClient(config)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("Zulip cursor store is required")
	}
	scope := client.config.Site + "|" + strings.ToLower(client.config.Email)
	return &Connector{client: client, store: store, scope: scope}, nil
}

type message struct {
	ID           int64         `json:"id"`
	Type         string        `json:"type"`
	Content      string        `json:"content"`
	Subject      string        `json:"subject"`
	StreamID     int64         `json:"stream_id"`
	SenderID     int64         `json:"sender_id"`
	SenderEmail  string        `json:"sender_email"`
	SenderName   string        `json:"sender_full_name"`
	Timestamp    int64         `json:"timestamp"`
	RawRecipient jsonRecipient `json:"display_recipient"`
}

// Channel display_recipient is a name; private messages use an array.
type recipient struct {
	ID    int64  `json:"id"`
	Email string `json:"email"`
}
type jsonRecipient struct{ users []recipient }

func (value *jsonRecipient) UnmarshalJSON(raw []byte) error {
	if len(raw) > 0 && raw[0] == '[' {
		return json.Unmarshal(raw, &value.users)
	}
	return nil
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	cursor, err := connector.store.GetZulipCursor(ctx, connector.scope)
	if err != nil {
		return nil, err
	}
	values := url.Values{"apply_markdown": {"false"}, "allow_empty_topic_name": {"true"}, "num_before": {"0"}, "num_after": {"1000"}, "include_anchor": {"false"}}
	if cursor == nil {
		values.Set("anchor", "newest")
		values.Set("num_before", "1")
		values.Set("num_after", "0")
		values.Set("include_anchor", "true")
	} else {
		values.Set("anchor", strconv.FormatInt(*cursor, 10))
	}
	var response struct {
		Messages []message `json:"messages"`
	}
	if err := connector.client.request(ctx, http.MethodGet, "messages", values, &response); err != nil {
		return nil, err
	}
	sort.Slice(response.Messages, func(i, j int) bool { return response.Messages[i].ID < response.Messages[j].ID })
	connector.pending = map[string]int64{}
	connector.batchEnd = 0
	envelopes := []contracts.TaskEnvelope{}
	for _, msg := range response.Messages {
		if msg.ID <= 0 {
			return nil, fmt.Errorf("zulip_invalid_message_id")
		}
		connector.batchEnd = msg.ID
		if cursor == nil || msg.ID <= *cursor {
			continue
		}
		envelope, ok := connector.normalize(msg)
		if ok {
			envelopes = append(envelopes, envelope)
			connector.pending[envelope.ID] = msg.ID
		}
	}
	// On first activation, start at the latest visible message rather than
	// replaying the server's entire history. Never advance past unaccepted work.
	if len(envelopes) == 0 && (cursor == nil || connector.batchEnd > 0) {
		return envelopes, connector.store.SetZulipCursor(ctx, connector.scope, connector.batchEnd)
	}
	return envelopes, nil
}

func (connector *Connector) AckEnvelope(ctx context.Context, id string) error {
	messageID, ok := connector.pending[id]
	if !ok {
		return fmt.Errorf("unknown Zulip acknowledgement")
	}
	if len(connector.pending) == 1 {
		messageID = connector.batchEnd
	}
	if err := connector.store.SetZulipCursor(ctx, connector.scope, messageID); err != nil {
		return err
	}
	delete(connector.pending, id)
	return nil
}

func (connector *Connector) normalize(msg message) (contracts.TaskEnvelope, bool) {
	config := connector.client.config
	if strings.EqualFold(msg.SenderEmail, config.Email) || strings.TrimSpace(msg.Content) == "" {
		return contracts.TaskEnvelope{}, false
	}
	target := Target{Site: config.Site}
	if msg.Type == "stream" {
		allowed := false
		for _, id := range config.AllowedChannelIDs {
			if id == strconv.FormatInt(msg.StreamID, 10) {
				allowed = true
			}
		}
		if !allowed {
			return contracts.TaskEnvelope{}, false
		}
		target.ChannelID = msg.StreamID
		target.Topic = msg.Subject
	} else if msg.Type == "private" || msg.Type == "direct" {
		if !config.AllowPrivateMessages {
			return contracts.TaskEnvelope{}, false
		}
		for _, user := range msg.RawRecipient.users {
			if !strings.EqualFold(user.Email, config.Email) {
				target.UserIDs = append(target.UserIDs, user.ID)
			}
		}
		if len(target.UserIDs) == 0 {
			return contracts.TaskEnvelope{}, false
		}
		sort.Slice(target.UserIDs, func(i, j int) bool { return target.UserIDs[i] < target.UserIDs[j] })
	} else {
		return contracts.TaskEnvelope{}, false
	}
	scopeHash := sha256.Sum256([]byte(config.Site))
	id := fmt.Sprintf("zulip:%x:%d", scopeHash[:8], msg.ID)
	actor := fmt.Sprintf("zulip:%d:%s", msg.SenderID, msg.SenderEmail)
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            id, Source: "im", DedupeKey: id,
		ReceivedAt: contracts.NewTimestamp(time.Unix(msg.Timestamp, 0).UTC()),
		Actor:      &actor, Content: msg.Content,
		Attachments: []contracts.AttachmentRef{},
		ContextRefs: append([]string{}, config.ContextRefs...),
		ReplyChannel: contracts.ReplyChannel{
			Type: "zulip", Target: target.String(),
			Metadata: map[string]any{
				"zulip_site": config.Site, "message_id": msg.ID,
				"channel_id": msg.StreamID, "topic": msg.Subject, "sender": msg.SenderName,
			},
		},
	}, true
}
