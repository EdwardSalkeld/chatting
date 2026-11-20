package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const Source = "im"

const internalNoticeKey = "internal_notice"
const telegramChannelNotEnabledNotice = "telegram_channel_not_enabled"
const telegramGroupNotEnabledNotice = "telegram_group_not_enabled"
const telegramBotDownloadLimitBytes int64 = 20 * 1024 * 1024

type ChatObservation = sqlitestate.TelegramChatObservation

type ObserveChatFunc func(context.Context, ChatObservation) error

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Config struct {
	BotToken           string
	APIBaseURL         string
	PollTimeoutSeconds int
	AllowedChatIDs     []string
	AllowedChannelIDs  []string
	ContextRefs        []string
	AttachmentRootDir  string
	RequestTimeout     time.Duration
	HTTPClient         HTTPClient
	ObserveChat        ObserveChatFunc
	// ObservePoll receives actual API outcomes, including failures swallowed for retry.
	ObservePoll func(error, int)
	Now         func() time.Time
}

type Connector struct {
	config            Config
	client            HTTPClient
	now               func() time.Time
	allowedChatIDs    map[string]bool
	allowedChannelIDs map[string]bool
	nextOffset        *int64
	pollDelay         time.Duration
	nextPollAt        time.Time
	attachmentDelay   time.Duration
}

func New(config Config) (*Connector, error) {
	if strings.TrimSpace(config.BotToken) == "" {
		return nil, errors.New("bot_token is required")
	}
	if strings.TrimSpace(config.APIBaseURL) == "" {
		config.APIBaseURL = "https://api.telegram.org"
	}
	if config.PollTimeoutSeconds <= 0 {
		config.PollTimeoutSeconds = 20
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: config.RequestTimeout}
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Connector{
		config:            config,
		client:            client,
		now:               now,
		allowedChatIDs:    stringSet(config.AllowedChatIDs),
		allowedChannelIDs: stringSet(config.AllowedChannelIDs),
	}, nil
}

// attachmentFetchError is restricted to remote API/download failures, never local state errors.
type attachmentFetchError struct {
	stage  string
	status int
}

func (e *attachmentFetchError) Error() string {
	if e.status != 0 {
		return fmt.Sprintf("telegram_attachment_%s_status=%d", e.stage, e.status)
	}
	return "telegram_attachment_" + e.stage + "_error"
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if connector.now().Before(connector.nextPollAt) {
		return nil, nil
	}
	response, err := connector.getUpdates(ctx)
	if err != nil || !response.OK {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			err = fmt.Errorf("telegram_get_updates_failed code=%d", response.ErrorCode)
		}
		if connector.pollDelay == 0 {
			connector.pollDelay = time.Second
		} else {
			connector.pollDelay = min(connector.pollDelay*2, time.Minute)
		}
		delay := connector.pollDelay
		if response.Parameters.RetryAfter > 0 {
			delay = max(delay, time.Duration(response.Parameters.RetryAfter)*time.Second)
		}
		connector.nextPollAt = connector.now().Add(delay)
		if connector.config.ObservePoll != nil {
			connector.config.ObservePoll(err, response.ErrorCode)
		}
		// Do not log request URLs, tokens, response bodies or upstream descriptions.
		log.Printf("Telegram polling failed: %v; retry in %s", err, delay)
		return nil, nil
	}
	if connector.config.ObservePoll != nil {
		connector.config.ObservePoll(nil, 0)
	}
	connector.pollDelay = 0
	connector.nextPollAt = time.Time{}
	envelopes := make([]contracts.TaskEnvelope, 0, len(response.Result))
	// A failed update must never be acknowledged because a later one was handled.
	sort.SliceStable(response.Result, func(i, j int) bool { return response.Result[i].UpdateID < response.Result[j].UpdateID })
	var highestUpdateID *int64
	for _, update := range response.Result {
		envelope, err := connector.normalizeUpdate(ctx, update)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			var fetchErr *attachmentFetchError
			if !errors.As(err, &fetchErr) {
				return envelopes, err
			}
			// Publish earlier envelopes now; never acknowledge the failed update.
			if highestUpdateID != nil {
				next := *highestUpdateID + 1
				connector.nextOffset = &next
			}
			if connector.attachmentDelay == 0 {
				connector.attachmentDelay = time.Second
			} else {
				connector.attachmentDelay = min(connector.attachmentDelay*2, time.Minute)
			}
			connector.nextPollAt = connector.now().Add(connector.attachmentDelay)
			log.Printf("Telegram attachment fetch failed: %v; retry in %s", fetchErr, connector.attachmentDelay)
			return envelopes, nil
		}
		if highestUpdateID == nil || update.UpdateID > *highestUpdateID {
			value := update.UpdateID
			highestUpdateID = &value
		}
		if envelope != nil {
			envelopes = append(envelopes, *envelope)
		}
	}
	connector.attachmentDelay = 0
	if highestUpdateID != nil {
		next := *highestUpdateID + 1
		connector.nextOffset = &next
	}
	return envelopes, nil
}

func (connector *Connector) getUpdates(ctx context.Context) (getUpdatesResponse, error) {
	endpoint := strings.TrimRight(connector.config.APIBaseURL, "/") + "/bot" + connector.config.BotToken + "/getUpdates"
	query := url.Values{"timeout": []string{strconv.Itoa(connector.config.PollTimeoutSeconds)}}
	if connector.nextOffset != nil {
		query.Set("offset", strconv.FormatInt(*connector.nextOffset, 10))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil)
	if err != nil {
		return getUpdatesResponse{}, errors.New("telegram_request_invalid")
	}
	resp, err := connector.client.Do(req)
	if err != nil {
		return getUpdatesResponse{}, errors.New("telegram_http_error")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var decoded getUpdatesResponse
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&decoded)
		decoded.OK = false
		decoded.ErrorCode = resp.StatusCode
		return decoded, fmt.Errorf("telegram_http_status=%d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return getUpdatesResponse{}, errors.New("telegram_http_error")
	}
	var decoded getUpdatesResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return getUpdatesResponse{}, errors.New("telegram_invalid_json")
	}
	return decoded, nil
}

func (connector *Connector) normalizeUpdate(ctx context.Context, update telegramUpdate) (*contracts.TaskEnvelope, error) {
	if update.Message != nil {
		return connector.normalizeMessage(ctx, update.UpdateID, "message", *update.Message, connector.actorFromUser(update.Message.From))
	}
	if update.ChannelPost != nil {
		return connector.normalizeChannelPost(ctx, update.UpdateID, *update.ChannelPost)
	}
	if update.MyChatMember != nil {
		if err := connector.observeChat(ctx, update.UpdateID, "my_chat_member", update.MyChatMember.Chat, nil); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (connector *Connector) normalizeMessage(ctx context.Context, updateID int64, kind string, message telegramMessage, actor *string) (*contracts.TaskEnvelope, error) {
	if message.MessageID == 0 || message.Chat.ID == 0 {
		return nil, nil
	}
	chatID := strconv.FormatInt(message.Chat.ID, 10)
	messageDate := messageDate(message.Date)
	if err := connector.observeChat(ctx, updateID, kind, message.Chat, messageDate); err != nil {
		return nil, err
	}
	if len(connector.allowedChatIDs) > 0 && !connector.allowedChatIDs[chatID] {
		if message.Chat.Type == "group" || message.Chat.Type == "supergroup" {
			return connector.buildDisallowedGroupNoticeEnvelope(updateID, message, chatID), nil
		}
		return nil, nil
	}
	return connector.buildEnvelope(ctx, updateID, message, chatID, actor)
}

func (connector *Connector) normalizeChannelPost(ctx context.Context, updateID int64, message telegramMessage) (*contracts.TaskEnvelope, error) {
	if message.MessageID == 0 || message.Chat.ID == 0 {
		return nil, nil
	}
	chatID := strconv.FormatInt(message.Chat.ID, 10)
	messageDate := messageDate(message.Date)
	if err := connector.observeChat(ctx, updateID, "channel_post", message.Chat, messageDate); err != nil {
		return nil, err
	}
	if message.Chat.Type != "channel" {
		return nil, nil
	}
	if len(connector.allowedChannelIDs) == 0 || !connector.allowedChannelIDs[chatID] {
		return connector.buildDisallowedChannelNoticeEnvelope(updateID, message, chatID), nil
	}
	return connector.buildEnvelope(ctx, updateID, message, chatID, connector.actorFromChat(message.SenderChat))
}

func (connector *Connector) buildDisallowedChannelNoticeEnvelope(updateID int64, message telegramMessage, chatID string) *contracts.TaskEnvelope {
	return connector.buildDisallowedTelegramNoticeEnvelope(
		updateID,
		message,
		chatID,
		telegramChannelNotEnabledNotice,
		fmt.Sprintf("Not enabled in channel %s. Add this id to telegram_allowed_channel_ids to enable replies here.", chatID),
		"telegram-disallowed-channel:",
	)
}

func (connector *Connector) buildDisallowedGroupNoticeEnvelope(updateID int64, message telegramMessage, chatID string) *contracts.TaskEnvelope {
	return connector.buildDisallowedTelegramNoticeEnvelope(
		updateID,
		message,
		chatID,
		telegramGroupNotEnabledNotice,
		fmt.Sprintf("Not enabled in group %s. Add this id to telegram_allowed_chat_ids to enable replies here.", chatID),
		"telegram-disallowed-group:",
	)
}

func (connector *Connector) buildDisallowedTelegramNoticeEnvelope(
	updateID int64,
	message telegramMessage,
	chatID string,
	noticeType string,
	content string,
	eventPrefix string,
) *contracts.TaskEnvelope {
	eventID := eventPrefix + strconv.FormatInt(updateID, 10)
	actor := "message-handler"
	return &contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            eventID,
		Source:        "internal",
		ReceivedAt:    contracts.NewTimestamp(parseTelegramDate(message.Date, connector.now())),
		Actor:         &actor,
		Content:       content,
		Attachments:   []contracts.AttachmentRef{},
		ContextRefs:   []string{},
		ReplyChannel: contracts.ReplyChannel{
			Type:   "telegram",
			Target: chatID,
			Metadata: map[string]any{
				"message_id":      message.MessageID,
				internalNoticeKey: noticeType,
			},
		},
		DedupeKey: eventID,
	}
}

func (connector *Connector) buildEnvelope(ctx context.Context, updateID int64, message telegramMessage, chatID string, actor *string) (*contracts.TaskEnvelope, error) {
	attachments, err := connector.extractAttachments(ctx, updateID, message)
	if err != nil {
		return nil, err
	}
	content := strings.TrimSpace(message.Text)
	if content == "" {
		content = strings.TrimSpace(message.Caption)
	}
	locationMetadata := locationMetadata(message.Location)
	if locationMetadata != nil {
		locationBlock := formatLocationContent(locationMetadata)
		if content != "" {
			content += "\n\n" + locationBlock
		} else {
			content = locationBlock
		}
	}
	if content == "" {
		if len(attachments) == 0 {
			if message.Document == nil {
				return nil, nil
			}
		} else if message.Document == nil {
			content = "[photo attached]"
		}
	}
	if message.Document != nil {
		documentContext := formatDocumentContent(*message.Document, len(attachments) > 0)
		if content == "" {
			content = documentContext
		} else {
			content += "\n\n" + documentContext
		}
	}
	if message.MessageThreadID != nil {
		content = fmt.Sprintf("[thread_id=%d] %s", *message.MessageThreadID, content)
	}
	eventID := "telegram:" + strconv.FormatInt(updateID, 10)
	metadata := map[string]any{"message_id": message.MessageID}
	if title := strings.TrimSpace(message.Chat.Title); title != "" {
		metadata["chat_title"] = title
	}
	if chatType := strings.TrimSpace(message.Chat.Type); chatType != "" {
		metadata["chat_type"] = chatType
	}
	if message.MessageThreadID != nil {
		metadata["message_thread_id"] = *message.MessageThreadID
	}
	if message.ReplyToMessage != nil && message.ReplyToMessage.MessageID > 0 {
		metadata["reply_to_message_id"] = message.ReplyToMessage.MessageID
	}
	if label := senderLabel(message); label != "" {
		metadata["sender"] = label
	}
	if locationMetadata != nil {
		metadata["location"] = locationMetadata
	}
	return &contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            eventID,
		Source:        Source,
		ReceivedAt:    contracts.NewTimestamp(parseTelegramDate(message.Date, connector.now())),
		Actor:         actor,
		Content:       content,
		Attachments:   attachments,
		ContextRefs:   append([]string{}, connector.config.ContextRefs...),
		ReplyChannel: contracts.ReplyChannel{
			Type:     "telegram",
			Target:   chatID,
			Metadata: metadata,
		},
		DedupeKey: eventID,
	}, nil
}

func (connector *Connector) extractAttachments(ctx context.Context, updateID int64, message telegramMessage) ([]contracts.AttachmentRef, error) {
	if message.Document != nil {
		return connector.extractDocumentAttachment(ctx, updateID, message.MessageID, *message.Document)
	}
	if len(message.Photo) == 0 {
		return []contracts.AttachmentRef{}, nil
	}
	photo := selectBestPhoto(message.Photo)
	if strings.TrimSpace(photo.FileID) == "" {
		return nil, errors.New("telegram_photo_missing_file_id")
	}
	metadata, err := connector.resolveFileMetadata(ctx, photo.FileID)
	if err != nil {
		return nil, err
	}
	suffix := filepath.Ext(metadata.FilePath)
	if suffix == "" {
		suffix = ".jpg"
	}
	uniqueFragment := strings.TrimSpace(photo.FileUniqueID)
	if uniqueFragment == "" {
		uniqueFragment = photo.FileID
	}
	localName := fmt.Sprintf(
		"telegram-%d-%d-%s%s",
		updateID,
		message.MessageID,
		safePathFragment(uniqueFragment),
		suffix,
	)
	rootDir := connector.config.AttachmentRootDir
	if strings.TrimSpace(rootDir) == "" {
		rootDir = filepath.Join(os.TempDir(), "chatting-telegram-attachments")
	}
	destination, err := filepath.Abs(filepath.Join(rootDir, localName))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return nil, err
	}
	fileURL := connector.buildFileDownloadURL(metadata.FilePath)
	raw, err := connector.fetchRaw(ctx, fileURL)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(destination, raw, 0o644); err != nil {
		return nil, err
	}
	// The worker/executor runs as a different OS user than the handler in the
	// split deployment, so it must be able to read a file it never wrote.
	// os.WriteFile's mode is masked by the process umask (the handler runs
	// UMask=0077), which would leave the file 0600 and unreadable by the
	// worker. An explicit Chmod is not subject to the umask, so force the
	// readable mode here.
	if err := os.Chmod(destination, 0o644); err != nil {
		return nil, err
	}
	name := path.Base(metadata.FilePath)
	if name == "." || name == "/" {
		name = filepath.Base(destination)
	}
	return []contracts.AttachmentRef{{
		URI:  (&url.URL{Scheme: "file", Path: filepath.ToSlash(destination)}).String(),
		Name: &name,
	}}, nil
}

func (connector *Connector) extractDocumentAttachment(ctx context.Context, updateID int64, messageID int64, document telegramDocument) ([]contracts.AttachmentRef, error) {
	if strings.TrimSpace(document.FileID) == "" {
		return nil, errors.New("telegram_document_missing_file_id")
	}
	if document.FileSize > telegramBotDownloadLimitBytes {
		return []contracts.AttachmentRef{}, nil
	}
	metadata, err := connector.resolveFileMetadata(ctx, document.FileID)
	if err != nil {
		return nil, err
	}
	displayName := telegramDocumentName(document, metadata.FilePath)
	suffix := filepath.Ext(displayName)
	if suffix == "" {
		suffix = filepath.Ext(metadata.FilePath)
	}
	uniqueFragment := strings.TrimSpace(document.FileUniqueID)
	if uniqueFragment == "" {
		uniqueFragment = document.FileID
	}
	localName := fmt.Sprintf(
		"telegram-%d-%d-%s%s",
		updateID,
		messageID,
		safePathFragment(uniqueFragment),
		suffix,
	)
	rootDir := connector.config.AttachmentRootDir
	if strings.TrimSpace(rootDir) == "" {
		rootDir = filepath.Join(os.TempDir(), "chatting-telegram-attachments")
	}
	destination, err := filepath.Abs(filepath.Join(rootDir, localName))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return nil, err
	}
	raw, err := connector.fetchRaw(ctx, connector.buildFileDownloadURL(metadata.FilePath))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > telegramBotDownloadLimitBytes {
		return nil, errors.New("telegram_document_too_large")
	}
	if err := os.WriteFile(destination, raw, 0o644); err != nil {
		return nil, err
	}
	if err := os.Chmod(destination, 0o644); err != nil {
		return nil, err
	}
	return []contracts.AttachmentRef{{
		URI:  (&url.URL{Scheme: "file", Path: filepath.ToSlash(destination)}).String(),
		Name: &displayName,
	}}, nil
}

func (connector *Connector) resolveFileMetadata(ctx context.Context, fileID string) (telegramFileMetadata, error) {
	endpoint := strings.TrimRight(connector.config.APIBaseURL, "/") + "/bot" + connector.config.BotToken + "/getFile"
	query := url.Values{"file_id": []string{fileID}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil)
	if err != nil {
		return telegramFileMetadata{}, &attachmentFetchError{stage: "metadata"}
	}
	resp, err := connector.client.Do(req)
	if err != nil {
		return telegramFileMetadata{}, &attachmentFetchError{stage: "metadata"}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return telegramFileMetadata{}, &attachmentFetchError{stage: "metadata", status: resp.StatusCode}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return telegramFileMetadata{}, &attachmentFetchError{stage: "metadata"}
	}
	var decoded getFileResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return telegramFileMetadata{}, &attachmentFetchError{stage: "metadata"}
	}
	if !decoded.OK || strings.TrimSpace(decoded.Result.FilePath) == "" {
		return telegramFileMetadata{}, &attachmentFetchError{stage: "metadata"}
	}
	return decoded.Result, nil
}

func (connector *Connector) fetchRaw(ctx context.Context, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, &attachmentFetchError{stage: "download"}
	}
	resp, err := connector.client.Do(req)
	if err != nil {
		return nil, &attachmentFetchError{stage: "download"}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &attachmentFetchError{stage: "download", status: resp.StatusCode}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, telegramBotDownloadLimitBytes+1))
	if err != nil {
		return nil, &attachmentFetchError{stage: "download"}
	}
	if int64(len(raw)) > telegramBotDownloadLimitBytes {
		return nil, errors.New("telegram_document_too_large")
	}
	return raw, nil
}

func (connector *Connector) buildFileDownloadURL(filePath string) string {
	quotedPath := strings.ReplaceAll(url.PathEscape(filePath), "%2F", "/")
	return strings.TrimRight(connector.config.APIBaseURL, "/") + "/file/bot" + connector.config.BotToken + "/" + quotedPath
}

func (connector *Connector) observeChat(ctx context.Context, updateID int64, updateKind string, chat telegramChat, messageDate *time.Time) error {
	if connector.config.ObserveChat == nil || chat.ID == 0 {
		return nil
	}
	return connector.config.ObserveChat(ctx, ChatObservation{
		ChatID:      strconv.FormatInt(chat.ID, 10),
		ChatType:    optionalString(chat.Type),
		Title:       optionalString(chat.Title),
		Username:    optionalString(chat.Username),
		UpdateID:    updateID,
		UpdateKind:  updateKind,
		MessageDate: messageDate,
		RetrievedAt: connector.now(),
	})
}

func (connector *Connector) actorFromUser(user *telegramUser) *string {
	if user == nil || user.ID == 0 {
		return nil
	}
	if strings.TrimSpace(user.Username) != "" {
		value := strconv.FormatInt(user.ID, 10) + ":" + strings.TrimSpace(user.Username)
		return &value
	}
	value := strconv.FormatInt(user.ID, 10)
	return &value
}

// senderLabel builds a human-readable label for the message author, preferring
// the sending user, then the sending chat (channel posts). Returns "" when
// neither is present.
func senderLabel(message telegramMessage) string {
	if label := userLabel(message.From); label != "" {
		return label
	}
	return chatLabel(message.SenderChat)
}

func userLabel(user *telegramUser) string {
	if user == nil {
		return ""
	}
	var label string
	if username := strings.TrimSpace(user.Username); username != "" {
		label = "@" + username
	} else if name := strings.TrimSpace(strings.TrimSpace(user.FirstName) + " " + strings.TrimSpace(user.LastName)); name != "" {
		label = name
	} else if user.ID != 0 {
		label = strconv.FormatInt(user.ID, 10)
	}
	if label != "" && user.IsBot {
		label += " (bot)"
	}
	return label
}

func chatLabel(chat *telegramChat) string {
	if chat == nil {
		return ""
	}
	if username := strings.TrimSpace(chat.Username); username != "" {
		return "@" + username
	}
	if title := strings.TrimSpace(chat.Title); title != "" {
		return title
	}
	if chat.ID != 0 {
		return strconv.FormatInt(chat.ID, 10)
	}
	return ""
}

func (connector *Connector) actorFromChat(chat *telegramChat) *string {
	if chat == nil || chat.ID == 0 {
		return nil
	}
	if strings.TrimSpace(chat.Username) != "" {
		value := strconv.FormatInt(chat.ID, 10) + ":" + strings.TrimSpace(chat.Username)
		return &value
	}
	if strings.TrimSpace(chat.Title) != "" {
		value := strconv.FormatInt(chat.ID, 10) + ":" + strings.TrimSpace(chat.Title)
		return &value
	}
	value := strconv.FormatInt(chat.ID, 10)
	return &value
}

type getUpdatesResponse struct {
	OK         bool             `json:"ok"`
	Result     []telegramUpdate `json:"result"`
	ErrorCode  int              `json:"error_code"`
	Parameters struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

type getFileResponse struct {
	OK     bool                 `json:"ok"`
	Result telegramFileMetadata `json:"result"`
}

type telegramFileMetadata struct {
	FilePath string `json:"file_path"`
}

type telegramUpdate struct {
	UpdateID     int64               `json:"update_id"`
	Message      *telegramMessage    `json:"message"`
	ChannelPost  *telegramMessage    `json:"channel_post"`
	MyChatMember *telegramChatMember `json:"my_chat_member"`
}

type telegramChatMember struct {
	Chat telegramChat `json:"chat"`
}

type telegramMessage struct {
	MessageID       int64             `json:"message_id"`
	MessageThreadID *int64            `json:"message_thread_id"`
	Date            int64             `json:"date"`
	Chat            telegramChat      `json:"chat"`
	From            *telegramUser     `json:"from"`
	SenderChat      *telegramChat     `json:"sender_chat"`
	Text            string            `json:"text"`
	Caption         string            `json:"caption"`
	Location        *telegramLocation `json:"location"`
	Photo           []telegramPhoto   `json:"photo"`
	Document        *telegramDocument `json:"document"`
	ReplyToMessage  *telegramMessage  `json:"reply_to_message"`
}

type telegramPhoto struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileSize     int64  `json:"file_size"`
	Width        int64  `json:"width"`
	Height       int64  `json:"height"`
}

type telegramDocument struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileName     string `json:"file_name"`
	MIMEType     string `json:"mime_type"`
	FileSize     int64  `json:"file_size"`
}

type telegramChat struct {
	ID       int64  `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	Username string `json:"username"`
}

type telegramUser struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	IsBot     bool   `json:"is_bot"`
}

type telegramLocation struct {
	Latitude             float64  `json:"latitude"`
	Longitude            float64  `json:"longitude"`
	HorizontalAccuracy   *float64 `json:"horizontal_accuracy"`
	LivePeriod           *int64   `json:"live_period"`
	Heading              *int64   `json:"heading"`
	ProximityAlertRadius *int64   `json:"proximity_alert_radius"`
}

func parseTelegramDate(value int64, fallback time.Time) time.Time {
	if value <= 0 {
		return fallback.UTC()
	}
	return time.Unix(value, 0).UTC()
}

func messageDate(value int64) *time.Time {
	if value <= 0 {
		return nil
	}
	parsed := time.Unix(value, 0).UTC()
	return &parsed
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func stringSet(values []string) map[string]bool {
	result := map[string]bool{}
	for _, value := range values {
		result[strings.TrimSpace(value)] = true
	}
	return result
}

func locationMetadata(location *telegramLocation) map[string]any {
	if location == nil {
		return nil
	}
	latitude := round6(location.Latitude)
	longitude := round6(location.Longitude)
	result := map[string]any{
		"latitude":  latitude,
		"longitude": longitude,
		"map_url":   fmt.Sprintf("https://maps.google.com/?q=%.6f,%.6f", location.Latitude, location.Longitude),
	}
	if location.HorizontalAccuracy != nil {
		result["horizontal_accuracy"] = *location.HorizontalAccuracy
	}
	if location.LivePeriod != nil {
		result["live_period"] = *location.LivePeriod
	}
	if location.Heading != nil {
		result["heading"] = *location.Heading
	}
	if location.ProximityAlertRadius != nil {
		result["proximity_alert_radius"] = *location.ProximityAlertRadius
	}
	return result
}

func formatLocationContent(metadata map[string]any) string {
	lines := []string{
		"[location shared]",
		fmt.Sprintf("latitude: %v", metadata["latitude"]),
		fmt.Sprintf("longitude: %v", metadata["longitude"]),
	}
	for _, key := range []string{"horizontal_accuracy", "live_period", "heading", "proximity_alert_radius"} {
		if value, ok := metadata[key]; ok {
			lines = append(lines, fmt.Sprintf("%s: %v", key, value))
		}
	}
	lines = append(lines, fmt.Sprintf("map: %s", metadata["map_url"]))
	return strings.Join(lines, "\n")
}

func round6(value float64) float64 {
	return math.Round(value*1000000) / 1000000
}

func selectBestPhoto(photos []telegramPhoto) telegramPhoto {
	var best telegramPhoto
	bestKey := [3]int64{-1, -1, -1}
	for _, photo := range photos {
		key := [3]int64{photo.FileSize, photo.Width, photo.Height}
		if key[0] > bestKey[0] ||
			(key[0] == bestKey[0] && key[1] > bestKey[1]) ||
			(key[0] == bestKey[0] && key[1] == bestKey[1] && key[2] > bestKey[2]) {
			best = photo
			bestKey = key
		}
	}
	return best
}

func telegramDocumentName(document telegramDocument, filePath string) string {
	name := strings.TrimSpace(document.FileName)
	if name == "" {
		name = path.Base(filePath)
	}
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	if name == "." || name == "/" || name == "" {
		return "document"
	}
	return name
}

func formatDocumentContent(document telegramDocument, downloaded bool) string {
	name := telegramDocumentName(document, "")
	if downloaded {
		return "[document attached: " + name + "]"
	}
	if document.FileSize > telegramBotDownloadLimitBytes {
		return "[document not downloaded: " + name + " exceeds Telegram's 20 MB bot download limit]"
	}
	return "[document not downloaded: " + name + "]"
}

func safePathFragment(value string) string {
	var builder strings.Builder
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-' ||
			character == '_' {
			builder.WriteRune(character)
		} else {
			builder.WriteByte('_')
		}
	}
	if builder.Len() == 0 {
		return "attachment"
	}
	return builder.String()
}
