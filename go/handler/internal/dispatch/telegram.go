package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const telegramParseMode = "HTML"

type TelegramConfig struct {
	BotToken   string
	APIBaseURL string
	Timeout    time.Duration
	HTTPClient *http.Client
	// AttachmentAllowedDirs constrains which local files may be sent as
	// attachments. The path in an outbound attachment is caller-controlled
	// (the executor via app.main_reply), so without this a caller could make
	// the handler read and exfiltrate an arbitrary file. Empty means no file
	// attachment is permitted (fail closed).
	AttachmentAllowedDirs []string
}

type TelegramMessageSender struct {
	botToken              string
	apiBaseURL            string
	client                *http.Client
	attachmentAllowedDirs []string
}

func NewTelegramMessageSender(config TelegramConfig) (*TelegramMessageSender, error) {
	if strings.TrimSpace(config.BotToken) == "" {
		return nil, errors.New("bot_token is required")
	}
	if strings.TrimSpace(config.APIBaseURL) == "" {
		return nil, errors.New("api_base_url is required")
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if timeout < 0 {
		return nil, errors.New("timeout must be positive")
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	allowedDirs := make([]string, 0, len(config.AttachmentAllowedDirs))
	for _, dir := range config.AttachmentAllowedDirs {
		trimmed := strings.TrimSpace(dir)
		if trimmed == "" {
			continue
		}
		allowedDirs = append(allowedDirs, filepath.Clean(trimmed))
	}
	return &TelegramMessageSender{
		botToken:              config.BotToken,
		apiBaseURL:            strings.TrimRight(config.APIBaseURL, "/"),
		client:                client,
		attachmentAllowedDirs: allowedDirs,
	}, nil
}

func (sender *TelegramMessageSender) Send(ctx context.Context, target string, message contracts.OutboundMessage) error {
	_, err := sender.SendWithResult(ctx, target, message)
	return err
}

// SendWithResult delivers a Telegram message and retains the Telegram-assigned
// message id. The ordinary Send method remains for dispatcher/test compatibility;
// callers that own a history ledger can opt into the richer result.
func (sender *TelegramMessageSender) SendWithResult(ctx context.Context, target string, message contracts.OutboundMessage) (TelegramSendResult, error) {
	if strings.TrimSpace(target) == "" {
		return TelegramSendResult{}, errors.New("target is required")
	}
	var topicID *int64
	if raw, present := message.Metadata["message_thread_id"]; present && raw != nil {
		value, ok := metadataPositiveInt(message.Metadata, "message_thread_id")
		if !ok {
			return TelegramSendResult{}, errors.New("telegram_topic_id_invalid")
		}
		topicID = &value
	}
	if message.Attachment == nil {
		if message.Body == nil || strings.TrimSpace(*message.Body) == "" {
			return TelegramSendResult{}, errors.New("body is required")
		}
		return sender.sendText(ctx, target, *message.Body, topicID)
	}

	file, fileName, err := openTelegramAttachment(message.Attachment.URI, sender.attachmentAllowedDirs)
	if err != nil {
		return TelegramSendResult{}, err
	}
	defer file.Close()
	method := telegramAPIMethodForAttachment(fileName)
	fileField := "document"
	if method == "sendPhoto" {
		fileField = "photo"
	}
	payload := map[string]string{"chat_id": target}
	if topicID != nil {
		payload["message_thread_id"] = fmt.Sprint(*topicID)
	}
	if message.Body != nil {
		payload["caption"] = renderTelegramMarkdown(*message.Body)
		payload["parse_mode"] = telegramParseMode
	}
	response, err := sender.postMultipart(ctx, method, payload, fileField, fileName, file)
	if err == nil && response.OK {
		return telegramSendResult(response)
	}
	if err == nil && message.Body != nil && isTelegramParseModeError(response) {
		fallback := map[string]string{"chat_id": target, "caption": *message.Body}
		if topicID != nil {
			fallback["message_thread_id"] = fmt.Sprint(*topicID)
		}
		response, err = sender.postMultipart(ctx, method, fallback, fileField, fileName, file)
		if err == nil && response.OK {
			return telegramSendResult(response)
		}
	}
	return TelegramSendResult{}, errors.New(describeTelegramResponseError("telegram_attachment_send_failed", response))
}

func (sender *TelegramMessageSender) React(ctx context.Context, target string, messageID int64, emoji string) error {
	if strings.TrimSpace(target) == "" {
		return errors.New("target is required")
	}
	if messageID <= 0 {
		return errors.New("message_id must be positive")
	}
	if strings.TrimSpace(emoji) == "" {
		return errors.New("emoji is required")
	}
	payload := map[string]any{
		"chat_id":    target,
		"message_id": messageID,
		"reaction": []map[string]string{
			{"type": "emoji", "emoji": strings.TrimSpace(emoji)},
		},
	}
	response, err := sender.postJSON(ctx, "setMessageReaction", payload)
	if err != nil || !response.OK {
		return errors.New(describeTelegramResponseError("telegram_reaction_failed", response))
	}
	return nil
}

func (sender *TelegramMessageSender) CreateForumTopic(ctx context.Context, chatID, name string) (int64, bool, error) {
	response, err := sender.postJSON(ctx, "createForumTopic", map[string]string{"chat_id": chatID, "name": name})
	if err != nil {
		return 0, false, fmt.Errorf("Telegram creation response unavailable")
	}
	if !response.OK {
		return 0, response.ErrorCode >= 400 && response.ErrorCode < 500, errors.New(describeTelegramResponseError("telegram_topic_create_failed", response))
	}
	var result struct {
		TopicID int64 `json:"message_thread_id"`
	}
	if json.Unmarshal(response.Result, &result) != nil || result.TopicID <= 0 {
		return 0, false, fmt.Errorf("Telegram creation response has no topic ID")
	}
	return result.TopicID, false, nil
}

func (sender *TelegramMessageSender) sendText(ctx context.Context, target string, body string, topicID *int64) (TelegramSendResult, error) {
	normalizedBody := strings.ReplaceAll(body, "\r\n", "\n")
	payload := map[string]any{
		"chat_id":    target,
		"text":       renderTelegramMarkdown(normalizedBody),
		"parse_mode": telegramParseMode,
	}
	if topicID != nil {
		payload["message_thread_id"] = *topicID
	}
	response, err := sender.postJSON(ctx, "sendMessage", payload)
	if err == nil && response.OK {
		return telegramSendResult(response)
	}
	if err == nil && isTelegramParseModeError(response) {
		fallback := map[string]any{"chat_id": target, "text": normalizedBody}
		if topicID != nil {
			fallback["message_thread_id"] = *topicID
		}
		response, err = sender.postJSON(ctx, "sendMessage", fallback)
		if err == nil && response.OK {
			return telegramSendResult(response)
		}
	}
	return TelegramSendResult{}, errors.New(describeTelegramResponseError("telegram_send_failed", response))
}

type TelegramSendResult struct {
	MessageID int64
	TopicID   *int64
}

type telegramAPIResponse struct {
	OK          bool            `json:"ok"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Result      json.RawMessage `json:"result,omitempty"`
}

type telegramAPIMessage struct {
	MessageID int64  `json:"message_id"`
	TopicID   *int64 `json:"message_thread_id"`
}

func telegramSendResult(response telegramAPIResponse) (TelegramSendResult, error) {
	var result telegramAPIMessage
	if len(response.Result) == 0 || json.Unmarshal(response.Result, &result) != nil || result.MessageID <= 0 {
		return TelegramSendResult{}, errors.New("telegram_response_missing_message_id")
	}
	return TelegramSendResult{MessageID: result.MessageID, TopicID: result.TopicID}, nil
}

func (sender *TelegramMessageSender) postJSON(ctx context.Context, method string, payload any) (telegramAPIResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return telegramAPIResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sender.methodURL(method), bytes.NewReader(body))
	if err != nil {
		return telegramAPIResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	return sender.do(request)
}

func (sender *TelegramMessageSender) postMultipart(ctx context.Context, method string, payload map[string]string, fileField string, fileName string, file io.ReadSeeker) (telegramAPIResponse, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range payload {
		if err := writer.WriteField(key, value); err != nil {
			return telegramAPIResponse{}, err
		}
	}
	// The caption fallback sends the same file a second time.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return telegramAPIResponse{}, err
	}
	mimeType := mime.TypeByExtension(filepath.Ext(fileName))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	part, err := writer.CreatePart(map[string][]string{
		"Content-Disposition": {fmt.Sprintf(`form-data; name="%s"; filename="%s"`, fileField, fileName)},
		"Content-Type":        {mimeType},
	})
	if err != nil {
		return telegramAPIResponse{}, err
	}
	if _, err := io.Copy(part, file); err != nil {
		return telegramAPIResponse{}, err
	}
	if err := writer.Close(); err != nil {
		return telegramAPIResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sender.methodURL(method), &body)
	if err != nil {
		return telegramAPIResponse{}, err
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return sender.do(request)
}

func (sender *TelegramMessageSender) do(request *http.Request) (telegramAPIResponse, error) {
	response, err := sender.client.Do(request)
	if err != nil {
		return telegramAPIResponse{}, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return telegramAPIResponse{}, err
	}
	var parsed telegramAPIResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return telegramAPIResponse{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return parsed, nil
	}
	return parsed, nil
}

func (sender *TelegramMessageSender) methodURL(method string) string {
	return sender.apiBaseURL + "/bot" + sender.botToken + "/" + method
}

// openTelegramAttachment opens the file an attachment URI names and returns it
// with its base name, provided it sits inside one of allowedDirs.
func openTelegramAttachment(rawURI string, allowedDirs []string) (*os.File, string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return nil, "", errors.New("telegram_attachment_unsupported_uri")
	}
	var path string
	switch parsed.Scheme {
	case "":
		path = rawURI
	case "file":
		unescaped, err := url.PathUnescape(parsed.Path)
		if err != nil {
			return nil, "", errors.New("telegram_attachment_unsupported_uri")
		}
		path = unescaped
		if parsed.Host != "" {
			path = "//" + parsed.Host + path
		}
	default:
		return nil, "", errors.New("telegram_attachment_unsupported_uri")
	}
	if !filepath.IsAbs(path) {
		return nil, "", errors.New("telegram_attachment_path_not_absolute")
	}
	// The path is caller-controlled, so confine it to an allowlisted directory
	// before touching the filesystem — otherwise a caller could have the handler
	// read and send an arbitrary file. Cleaning first collapses any ".." so the
	// containment check can't be walked out of.
	cleaned := filepath.Clean(path)
	reason := "telegram_attachment_path_not_allowed"
	for _, dir := range allowedDirs {
		rel, ok := relativeToAllowedDir(cleaned, dir)
		if !ok {
			continue
		}
		file, err := openWithinDir(dir, rel)
		if err == nil {
			return file, filepath.Base(cleaned), nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			reason = "telegram_attachment_missing"
		}
	}
	return nil, "", errors.New(reason)
}

// openWithinDir opens rel beneath dir. A lexical check alone would let a
// symlink inside dir expose a file elsewhere; os.Root refuses any link that
// leads out of dir, and holds that through the open itself, so swapping a
// path component after the check gains nothing. Directories report as missing.
func openWithinDir(dir string, rel string) (*os.File, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.Open(rel)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || info.IsDir() {
		file.Close()
		return nil, fs.ErrNotExist
	}
	return file, nil
}

// relativeToAllowedDir returns cleaned (already filepath.Clean'd and absolute)
// relative to dir, and whether it sits inside dir.
func relativeToAllowedDir(cleaned string, dir string) (string, bool) {
	rel, err := filepath.Rel(dir, cleaned)
	if err != nil {
		return "", false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	if filepath.IsAbs(rel) {
		return "", false
	}
	return rel, true
}

func telegramAPIMethodForAttachment(filePath string) string {
	mimeType := mime.TypeByExtension(filepath.Ext(filePath))
	if strings.HasPrefix(mimeType, "image/") {
		return "sendPhoto"
	}
	return "sendDocument"
}

func isTelegramParseModeError(response telegramAPIResponse) bool {
	return strings.Contains(strings.ToLower(response.Description), "parse entities")
}

// describeTelegramResponseError appends the Telegram API description (for example
// "Bad Request: REACTION_INVALID") to the reason code so dispatch failures are
// diagnosable from the logged/emailed reason rather than an opaque code.
func describeTelegramResponseError(prefix string, response telegramAPIResponse) string {
	if description := strings.TrimSpace(response.Description); description != "" {
		return prefix + " description=" + description
	}
	return prefix
}
