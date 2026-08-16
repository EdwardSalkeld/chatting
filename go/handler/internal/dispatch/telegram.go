package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type TelegramConfig struct {
	BotToken   string
	APIBaseURL string
	ParseMode  *string
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
	parseMode             *string
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
	parseMode := config.ParseMode
	if parseMode == nil {
		defaultParseMode := "Markdown"
		parseMode = &defaultParseMode
	}
	if parseMode != nil && *parseMode != "Markdown" && *parseMode != "MarkdownV2" && *parseMode != "HTML" {
		return nil, errors.New("parse_mode must be one of Markdown, MarkdownV2, HTML, or nil")
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
		parseMode:             parseMode,
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
	if message.Attachment == nil {
		if message.Body == nil || strings.TrimSpace(*message.Body) == "" {
			return TelegramSendResult{}, errors.New("body is required")
		}
		return sender.sendText(ctx, target, *message.Body)
	}

	filePath, err := resolveTelegramAttachmentPath(message.Attachment.URI, sender.attachmentAllowedDirs)
	if err != nil {
		return TelegramSendResult{}, err
	}
	method := telegramAPIMethodForAttachment(filePath)
	fileField := "document"
	if method == "sendPhoto" {
		fileField = "photo"
	}
	payload := map[string]string{"chat_id": target}
	if message.Body != nil {
		payload["caption"] = normalizeTelegramTextForParseMode(*message.Body, sender.parseMode)
		if sender.parseMode != nil {
			payload["parse_mode"] = *sender.parseMode
		}
	}
	response, err := sender.postMultipart(ctx, method, payload, fileField, filePath)
	if err == nil && response.OK {
		return telegramSendResult(response)
	}
	if err == nil && sender.parseMode != nil && message.Body != nil && isTelegramParseModeError(response) {
		fallback := map[string]string{"chat_id": target, "caption": *message.Body}
		response, err = sender.postMultipart(ctx, method, fallback, fileField, filePath)
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

func (sender *TelegramMessageSender) sendText(ctx context.Context, target string, body string) (TelegramSendResult, error) {
	normalizedBody := normalizeTelegramOutboundText(body)
	payload := map[string]any{
		"chat_id": target,
		"text":    normalizeTelegramTextForParseMode(normalizedBody, sender.parseMode),
	}
	if sender.parseMode != nil {
		payload["parse_mode"] = *sender.parseMode
	}
	response, err := sender.postJSON(ctx, "sendMessage", payload)
	if err == nil && response.OK {
		return telegramSendResult(response)
	}
	if err == nil && sender.parseMode != nil && isTelegramParseModeError(response) {
		fallback := map[string]any{"chat_id": target, "text": normalizedBody}
		response, err = sender.postJSON(ctx, "sendMessage", fallback)
		if err == nil && response.OK {
			return telegramSendResult(response)
		}
	}
	return TelegramSendResult{}, errors.New(describeTelegramResponseError("telegram_send_failed", response))
}

type TelegramSendResult struct {
	MessageID int64
}

type telegramAPIResponse struct {
	OK          bool            `json:"ok"`
	Description string          `json:"description"`
	Result      json.RawMessage `json:"result,omitempty"`
}

type telegramAPIMessage struct {
	MessageID int64 `json:"message_id"`
}

func telegramSendResult(response telegramAPIResponse) (TelegramSendResult, error) {
	var result telegramAPIMessage
	if len(response.Result) == 0 || json.Unmarshal(response.Result, &result) != nil || result.MessageID <= 0 {
		return TelegramSendResult{}, errors.New("telegram_response_missing_message_id")
	}
	return TelegramSendResult{MessageID: result.MessageID}, nil
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

func (sender *TelegramMessageSender) postMultipart(ctx context.Context, method string, payload map[string]string, fileField string, filePath string) (telegramAPIResponse, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range payload {
		if err := writer.WriteField(key, value); err != nil {
			return telegramAPIResponse{}, err
		}
	}
	file, err := os.Open(filePath)
	if err != nil {
		return telegramAPIResponse{}, err
	}
	defer file.Close()
	mimeType := mime.TypeByExtension(filepath.Ext(filePath))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	part, err := writer.CreatePart(map[string][]string{
		"Content-Disposition": {fmt.Sprintf(`form-data; name="%s"; filename="%s"`, fileField, filepath.Base(filePath))},
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

func resolveTelegramAttachmentPath(rawURI string, allowedDirs []string) (string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return "", errors.New("telegram_attachment_unsupported_uri")
	}
	var path string
	switch parsed.Scheme {
	case "":
		path = rawURI
	case "file":
		unescaped, err := url.PathUnescape(parsed.Path)
		if err != nil {
			return "", errors.New("telegram_attachment_unsupported_uri")
		}
		path = unescaped
		if parsed.Host != "" {
			path = "//" + parsed.Host + path
		}
	default:
		return "", errors.New("telegram_attachment_unsupported_uri")
	}
	if !filepath.IsAbs(path) {
		return "", errors.New("telegram_attachment_path_not_absolute")
	}
	// The path is caller-controlled, so confine it to an allowlisted directory
	// before touching the filesystem — otherwise a caller could have the handler
	// read and send an arbitrary file. Cleaning first collapses any ".." so the
	// containment check can't be walked out of.
	cleaned := filepath.Clean(path)
	if !pathWithinAllowedDir(cleaned, allowedDirs) {
		return "", errors.New("telegram_attachment_path_not_allowed")
	}
	info, err := os.Stat(cleaned)
	if err != nil || info.IsDir() {
		return "", errors.New("telegram_attachment_missing")
	}
	return cleaned, nil
}

// pathWithinAllowedDir reports whether cleaned (already filepath.Clean'd and
// absolute) sits inside one of allowedDirs. Empty allowedDirs denies everything
// (fail closed).
func pathWithinAllowedDir(cleaned string, allowedDirs []string) bool {
	for _, dir := range allowedDirs {
		rel, err := filepath.Rel(dir, cleaned)
		if err != nil {
			continue
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		if filepath.IsAbs(rel) {
			continue
		}
		return true
	}
	return false
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

func normalizeTelegramTextForParseMode(text string, parseMode *string) string {
	if parseMode == nil {
		return text
	}
	switch *parseMode {
	case "HTML":
		return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(text)
	case "MarkdownV2":
		return escapeWithBackslashPrefix(text, "\\_*[]()~`>#+-=|{}.!")
	case "Markdown":
		return escapeWithBackslashPrefix(text, "\\_*`[]()")
	default:
		return text
	}
}

func escapeWithBackslashPrefix(text string, specials string) string {
	var builder strings.Builder
	for _, character := range text {
		if strings.ContainsRune(specials, character) {
			builder.WriteRune('\\')
		}
		builder.WriteRune(character)
	}
	return builder.String()
}

func normalizeTelegramOutboundText(text string) string {
	normalized := strings.ReplaceAll(text, "\r\n", "\n")
	patterns := []struct {
		expression  *regexp.Regexp
		replacement string
	}{
		{regexp.MustCompile(`(?:\\)+r(?:\\)+n`), "\n"},
		{regexp.MustCompile(`(?:\\)+n`), "\n"},
		{regexp.MustCompile(`(?:\\)+r`), "\n"},
		{regexp.MustCompile(`(?:\\)+([\\_*` + "`" + `\[\]\(\)~>#\+\-=|{}.!])`), "$1"},
	}
	for range 3 {
		previous := normalized
		for _, pattern := range patterns {
			normalized = pattern.expression.ReplaceAllString(normalized, pattern.replacement)
		}
		normalized = strings.ReplaceAll(normalized, `\t`, "\t")
		if normalized == previous {
			break
		}
	}
	return normalized
}
