package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const Source = "im"

type ChatObservation struct {
	ChatID      string
	ChatType    *string
	Title       *string
	Username    *string
	UpdateID    int64
	UpdateKind  string
	MessageDate *time.Time
	RetrievedAt time.Time
}

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
	PromptContext      contracts.PromptContext
	AttachmentRootDir  string
	RequestTimeout     time.Duration
	HTTPClient         HTTPClient
	ObserveChat        ObserveChatFunc
	Now                func() time.Time
}

type Connector struct {
	config            Config
	client            HTTPClient
	now               func() time.Time
	prompt            *contracts.PromptContext
	allowedChatIDs    map[string]bool
	allowedChannelIDs map[string]bool
	nextOffset        *int64
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
	var prompt *contracts.PromptContext
	if config.PromptContext.HasContent() {
		copy := config.PromptContext
		prompt = &copy
	}
	return &Connector{
		config:            config,
		client:            client,
		now:               now,
		prompt:            prompt,
		allowedChatIDs:    stringSet(config.AllowedChatIDs),
		allowedChannelIDs: stringSet(config.AllowedChannelIDs),
	}, nil
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := connector.getUpdates(ctx)
	if err != nil {
		return nil, err
	}
	if !response.OK {
		return nil, errors.New("telegram_get_updates_failed")
	}
	envelopes := make([]contracts.TaskEnvelope, 0, len(response.Result))
	var highestUpdateID *int64
	for _, update := range response.Result {
		if highestUpdateID == nil || update.UpdateID > *highestUpdateID {
			value := update.UpdateID
			highestUpdateID = &value
		}
		envelope, err := connector.normalizeUpdate(ctx, update)
		if err != nil {
			return envelopes, err
		}
		if envelope != nil {
			envelopes = append(envelopes, *envelope)
		}
	}
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
		return getUpdatesResponse{}, err
	}
	resp, err := connector.client.Do(req)
	if err != nil {
		return getUpdatesResponse{}, errors.New("telegram_http_error")
	}
	defer resp.Body.Close()
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
		return nil, nil
	}
	return connector.buildEnvelope(updateID, message, chatID, actor)
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
	if message.Chat.Type != "channel" || len(connector.allowedChannelIDs) == 0 || !connector.allowedChannelIDs[chatID] {
		return nil, nil
	}
	return connector.buildEnvelope(updateID, message, chatID, connector.actorFromChat(message.SenderChat))
}

func (connector *Connector) buildEnvelope(updateID int64, message telegramMessage, chatID string, actor *string) (*contracts.TaskEnvelope, error) {
	attachments, err := connector.extractAttachments(updateID, message)
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
			return nil, nil
		}
		content = "[photo attached]"
	}
	if message.MessageThreadID != nil {
		content = fmt.Sprintf("[thread_id=%d] %s", *message.MessageThreadID, content)
	}
	eventID := "telegram:" + strconv.FormatInt(updateID, 10)
	metadata := map[string]any{"message_id": message.MessageID}
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
		PromptContext: connector.prompt,
		ReplyChannel: contracts.ReplyChannel{
			Type:     "telegram",
			Target:   chatID,
			Metadata: metadata,
		},
		DedupeKey: eventID,
	}, nil
}

func (connector *Connector) extractAttachments(updateID int64, message telegramMessage) ([]contracts.AttachmentRef, error) {
	if len(message.Photo) == 0 {
		return []contracts.AttachmentRef{}, nil
	}
	photo := selectBestPhoto(message.Photo)
	if strings.TrimSpace(photo.FileID) == "" {
		return nil, errors.New("telegram_photo_missing_file_id")
	}
	metadata, err := connector.resolveFileMetadata(photo.FileID)
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
	raw, err := connector.fetchRaw(fileURL)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(destination, raw, 0o600); err != nil {
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

func (connector *Connector) resolveFileMetadata(fileID string) (telegramFileMetadata, error) {
	endpoint := strings.TrimRight(connector.config.APIBaseURL, "/") + "/bot" + connector.config.BotToken + "/getFile"
	query := url.Values{"file_id": []string{fileID}}
	req, err := http.NewRequest(http.MethodGet, endpoint+"?"+query.Encode(), nil)
	if err != nil {
		return telegramFileMetadata{}, err
	}
	resp, err := connector.client.Do(req)
	if err != nil {
		return telegramFileMetadata{}, errors.New("telegram_http_error")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return telegramFileMetadata{}, errors.New("telegram_http_error")
	}
	var decoded getFileResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return telegramFileMetadata{}, errors.New("telegram_invalid_json")
	}
	if !decoded.OK || strings.TrimSpace(decoded.Result.FilePath) == "" {
		return telegramFileMetadata{}, errors.New("telegram_invalid_response_shape")
	}
	return decoded.Result, nil
}

func (connector *Connector) fetchRaw(rawURL string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := connector.client.Do(req)
	if err != nil {
		return nil, errors.New("telegram_http_error")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("telegram_http_error")
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
	OK     bool             `json:"ok"`
	Result []telegramUpdate `json:"result"`
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
}

type telegramPhoto struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileSize     int64  `json:"file_size"`
	Width        int64  `json:"width"`
	Height       int64  `json:"height"`
}

type telegramChat struct {
	ID       int64  `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	Username string `json:"username"`
}

type telegramUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
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
