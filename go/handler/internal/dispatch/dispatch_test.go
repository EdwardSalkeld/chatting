package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestDispatcherSendsEmailMessage(t *testing.T) {
	body := "Done."
	sender := &recordingEmailSender{}
	message := contracts.OutboundMessage{
		Channel: "email",
		Target:  "alice@example.com",
		Body:    &body,
	}

	dispatched, err := (Dispatcher{EmailSender: sender}).Dispatch(context.Background(), message, contracts.TaskEnvelope{})
	if err != nil {
		t.Fatal(err)
	}
	if dispatched == nil || dispatched.Channel != "email" {
		t.Fatalf("dispatched = %#v", dispatched)
	}
	if len(sender.messages) != 1 {
		t.Fatalf("sent count = %d", len(sender.messages))
	}
	if sender.messages[0].target != "alice@example.com" || sender.messages[0].body != "Done." || sender.messages[0].subject != nil {
		t.Fatalf("sent = %#v", sender.messages[0])
	}
}

func TestDispatcherDropsEmailWhenSenderNotConfigured(t *testing.T) {
	body := "Done."
	message := contracts.OutboundMessage{
		Channel: "email",
		Target:  "alice@example.com",
		Body:    &body,
	}

	dispatched, err := (Dispatcher{}).Dispatch(context.Background(), message, contracts.TaskEnvelope{})
	if err != nil {
		t.Fatal(err)
	}
	if dispatched != nil {
		t.Fatalf("dispatched = %#v", dispatched)
	}
}

func TestDispatcherFormatsEmailReplySubjectAndQuotedOriginal(t *testing.T) {
	body := "Here is the answer."
	sender := &recordingEmailSender{}
	message := contracts.OutboundMessage{
		Channel: "email",
		Target:  "alice@example.com",
		Body:    &body,
	}

	_, err := (Dispatcher{EmailSender: sender}).Dispatch(context.Background(), message, emailEnvelope("Quarterly update", "Can you summarize the key points?"))
	if err != nil {
		t.Fatal(err)
	}

	sent := sender.messages[0]
	if sent.subject == nil || *sent.subject != "Re: Quarterly update" {
		t.Fatalf("subject = %#v", sent.subject)
	}
	if !strings.Contains(sent.body, "Here is the answer.") ||
		!strings.Contains(sent.body, "Original message:") ||
		!strings.Contains(sent.body, "> Can you summarize the key points?") {
		t.Fatalf("body = %q", sent.body)
	}
}

func TestDispatcherStripsLeadingSubjectLineFromEmailBody(t *testing.T) {
	body := "Subject: Re: Ice-cream\n\nGreat choice. Let's go classic."
	sender := &recordingEmailSender{}
	message := contracts.OutboundMessage{
		Channel: "email",
		Target:  "alice@example.com",
		Body:    &body,
	}

	_, err := (Dispatcher{EmailSender: sender}).Dispatch(context.Background(), message, emailEnvelope("Ice-cream", "Yes please"))
	if err != nil {
		t.Fatal(err)
	}

	sent := sender.messages[0]
	if sent.subject == nil || *sent.subject != "Re: Ice-cream" {
		t.Fatalf("subject = %#v", sent.subject)
	}
	if strings.HasPrefix(strings.TrimSpace(sent.body), "Subject:") {
		t.Fatalf("body still starts with subject line: %q", sent.body)
	}
	if !strings.Contains(sent.body, "Great choice. Let's go classic.") {
		t.Fatalf("body = %q", sent.body)
	}
}

func TestDispatcherResolvesFinalToEnvelopeReplyChannel(t *testing.T) {
	body := "Final answer."
	sender := &recordingEmailSender{}
	message := contracts.OutboundMessage{
		Channel: "final",
		Target:  "unused",
		Body:    &body,
	}

	dispatched, err := (Dispatcher{EmailSender: sender}).Dispatch(context.Background(), message, emailEnvelope("Question", "Body"))
	if err != nil {
		t.Fatal(err)
	}
	if dispatched == nil || dispatched.Channel != "email" || dispatched.Target != "alice@example.com" {
		t.Fatalf("dispatched = %#v", dispatched)
	}
	if sender.messages[0].target != "alice@example.com" {
		t.Fatalf("target = %q", sender.messages[0].target)
	}
}

func TestDispatcherReturnsEmailFailureReason(t *testing.T) {
	body := "Done."
	message := contracts.OutboundMessage{
		Channel: "email",
		Target:  "alice@example.com",
		Body:    &body,
	}

	_, err := (Dispatcher{EmailSender: &recordingEmailSender{err: errors.New("boom")}}).Dispatch(context.Background(), message, contracts.TaskEnvelope{})
	if err == nil {
		t.Fatal("expected error")
	}
	var dispatchErr MessageDispatchError
	if !errors.As(err, &dispatchErr) || dispatchErr.ReasonCode != "email_dispatch_failed" {
		t.Fatalf("error = %#v", err)
	}
}

func TestDispatcherSendsTelegramMessage(t *testing.T) {
	body := "Done via telegram."
	sender := &recordingTelegramSender{}
	message := contracts.OutboundMessage{
		Channel: "telegram",
		Target:  "12345",
		Body:    &body,
	}

	dispatched, err := (Dispatcher{TelegramSender: sender}).Dispatch(context.Background(), message, contracts.TaskEnvelope{})
	if err != nil {
		t.Fatal(err)
	}
	if dispatched == nil || dispatched.Channel != "telegram" || dispatched.Target != "12345" {
		t.Fatalf("dispatched = %#v", dispatched)
	}
	if len(sender.messages) != 1 || sender.messages[0].target != "12345" {
		t.Fatalf("messages = %#v", sender.messages)
	}
}

func TestDispatcherSendsTelegramReactionFromEnvelopeMetadata(t *testing.T) {
	body := "👍"
	sender := &recordingTelegramSender{}
	message := contracts.OutboundMessage{
		Channel: "telegram_reaction",
		Target:  "12345",
		Body:    &body,
	}
	envelope := contracts.TaskEnvelope{
		ReplyChannel: contracts.ReplyChannel{
			Type:     "telegram",
			Target:   "12345",
			Metadata: map[string]any{"message_id": float64(42)},
		},
	}

	_, err := (Dispatcher{TelegramSender: sender}).Dispatch(context.Background(), message, envelope)
	if err != nil {
		t.Fatal(err)
	}
	if len(sender.reactions) != 1 || sender.reactions[0].messageID != 42 || sender.reactions[0].emoji != "👍" {
		t.Fatalf("reactions = %#v", sender.reactions)
	}
}

func TestDispatcherReturnsTelegramAttachmentFailureReason(t *testing.T) {
	body := "photo"
	message := contracts.OutboundMessage{
		Channel: "telegram",
		Target:  "12345",
		Body:    &body,
		Attachment: &contracts.AttachmentRef{
			URI: "/tmp/missing.png",
		},
	}

	_, err := (Dispatcher{TelegramSender: &recordingTelegramSender{err: errors.New("telegram_attachment_missing")}}).Dispatch(context.Background(), message, contracts.TaskEnvelope{})
	var dispatchErr MessageDispatchError
	if !errors.As(err, &dispatchErr) || dispatchErr.ReasonCode != "telegram_attachment_missing" {
		t.Fatalf("error = %#v", err)
	}
}

func TestTelegramMessageSenderSendsTextAndFallsBackWithoutParseMode(t *testing.T) {
	var calls []telegramHTTPCall
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call := recordTelegramJSONCall(t, request)
		calls = append(calls, call)
		if len(calls) == 1 {
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": false, "description": "Bad Request: can't parse entities"})
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL)
	body := `Line 1.\nNeeds *escaping*.`

	err := sender.Send(context.Background(), "12345", contracts.OutboundMessage{Channel: "telegram", Target: "12345", Body: &body})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 {
		t.Fatalf("calls = %#v", calls)
	}
	if calls[0].path != "/bottoken/sendMessage" || calls[0].payload["parse_mode"] != "Markdown" {
		t.Fatalf("first call = %#v", calls[0])
	}
	if calls[0].payload["text"] != "Line 1.\nNeeds \\*escaping\\*." {
		t.Fatalf("first text = %#v", calls[0].payload["text"])
	}
	if _, ok := calls[1].payload["parse_mode"]; ok {
		t.Fatalf("fallback still has parse_mode: %#v", calls[1].payload)
	}
	if calls[1].payload["text"] != "Line 1.\nNeeds *escaping*." {
		t.Fatalf("fallback text = %#v", calls[1].payload["text"])
	}
}

func TestTelegramMessageSenderSendsReaction(t *testing.T) {
	var call telegramHTTPCall
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call = recordTelegramJSONCall(t, request)
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL)

	if err := sender.React(context.Background(), "12345", 99, "🙂"); err != nil {
		t.Fatal(err)
	}
	if call.path != "/bottoken/setMessageReaction" || call.payload["message_id"] != float64(99) {
		t.Fatalf("call = %#v", call)
	}
	reactions, ok := call.payload["reaction"].([]any)
	if !ok || len(reactions) != 1 {
		t.Fatalf("reaction = %#v", call.payload["reaction"])
	}
}

func TestTelegramMessageSenderSendsPhotoAttachment(t *testing.T) {
	tempDir := t.TempDir()
	photoPath := filepath.Join(tempDir, "plant.png")
	if err := os.WriteFile(photoPath, []byte("png bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	var multipartCall telegramMultipartCall
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		multipartCall = recordTelegramMultipartCall(t, request)
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL)
	body := "what is this?"

	if err := sender.Send(context.Background(), "12345", contracts.OutboundMessage{
		Channel: "telegram",
		Target:  "12345",
		Body:    &body,
		Attachment: &contracts.AttachmentRef{
			URI: photoPath,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if multipartCall.path != "/bottoken/sendPhoto" || multipartCall.fields["chat_id"] != "12345" || multipartCall.fileField != "photo" {
		t.Fatalf("multipart call = %#v", multipartCall)
	}
}

func TestTelegramMessageSenderSendsDocumentAttachmentFromFileURI(t *testing.T) {
	tempDir := t.TempDir()
	documentPath := filepath.Join(tempDir, "report.txt")
	if err := os.WriteFile(documentPath, []byte("report"), 0o600); err != nil {
		t.Fatal(err)
	}
	var multipartCall telegramMultipartCall
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		multipartCall = recordTelegramMultipartCall(t, request)
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL)

	if err := sender.Send(context.Background(), "12345", contracts.OutboundMessage{
		Channel: "telegram",
		Target:  "12345",
		Attachment: &contracts.AttachmentRef{
			URI: "file://" + documentPath,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if multipartCall.path != "/bottoken/sendDocument" || multipartCall.fileField != "document" {
		t.Fatalf("multipart call = %#v", multipartCall)
	}
}

func TestTelegramMessageSenderRejectsUnsupportedAttachmentURI(t *testing.T) {
	sender := newTestTelegramSender(t, "http://127.0.0.1")
	err := sender.Send(context.Background(), "12345", contracts.OutboundMessage{
		Channel: "telegram",
		Target:  "12345",
		Attachment: &contracts.AttachmentRef{
			URI: "https://example.com/image.png",
		},
	})
	if err == nil || err.Error() != "telegram_attachment_unsupported_uri" {
		t.Fatalf("error = %v", err)
	}
}

type sentEmail struct {
	target  string
	body    string
	subject *string
}

type recordingEmailSender struct {
	messages []sentEmail
	err      error
}

func (sender *recordingEmailSender) Send(_ context.Context, target string, body string, subject *string) error {
	if sender.err != nil {
		return sender.err
	}
	sender.messages = append(sender.messages, sentEmail{target: target, body: body, subject: subject})
	return nil
}

type sentTelegram struct {
	target  string
	message contracts.OutboundMessage
}

type sentReaction struct {
	target    string
	messageID int64
	emoji     string
}

type recordingTelegramSender struct {
	messages  []sentTelegram
	reactions []sentReaction
	err       error
}

func (sender *recordingTelegramSender) Send(_ context.Context, target string, message contracts.OutboundMessage) error {
	if sender.err != nil {
		return sender.err
	}
	sender.messages = append(sender.messages, sentTelegram{target: target, message: message})
	return nil
}

func (sender *recordingTelegramSender) React(_ context.Context, target string, messageID int64, emoji string) error {
	if sender.err != nil {
		return sender.err
	}
	sender.reactions = append(sender.reactions, sentReaction{target: target, messageID: messageID, emoji: emoji})
	return nil
}

type telegramHTTPCall struct {
	path    string
	payload map[string]any
}

type telegramMultipartCall struct {
	path      string
	fields    map[string]string
	fileField string
	fileName  string
}

func newTestTelegramSender(t *testing.T, apiBaseURL string) *TelegramMessageSender {
	t.Helper()
	sender, err := NewTelegramMessageSender(TelegramConfig{BotToken: "token", APIBaseURL: apiBaseURL})
	if err != nil {
		t.Fatal(err)
	}
	return sender
}

func recordTelegramJSONCall(t *testing.T, request *http.Request) telegramHTTPCall {
	t.Helper()
	if request.Method != http.MethodPost {
		t.Fatalf("method = %s", request.Method)
	}
	var payload map[string]any
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	return telegramHTTPCall{path: request.URL.Path, payload: payload}
}

func recordTelegramMultipartCall(t *testing.T, request *http.Request) telegramMultipartCall {
	t.Helper()
	reader, err := request.MultipartReader()
	if err != nil {
		t.Fatal(err)
	}
	call := telegramMultipartCall{path: request.URL.Path, fields: map[string]string{}}
	for {
		part, err := reader.NextPart()
		if errors.Is(err, multipart.ErrMessageTooLarge) {
			t.Fatal(err)
		}
		if err != nil {
			break
		}
		if part.FileName() == "" {
			raw, err := io.ReadAll(part)
			if err != nil {
				t.Fatal(err)
			}
			call.fields[part.FormName()] = string(raw)
			continue
		}
		call.fileField = part.FormName()
		call.fileName = part.FileName()
	}
	return call
}

func emailEnvelope(subject string, body string) contracts.TaskEnvelope {
	actor := "alice@example.com"
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "email:1",
		Source:        "email",
		ReceivedAt:    contracts.NewTimestamp(mustTime("2026-03-06T11:00:00Z")),
		Actor:         &actor,
		Content:       "Subject: " + subject + "\n\n" + body,
		ReplyChannel: contracts.ReplyChannel{
			Type:   "email",
			Target: "alice@example.com",
		},
		DedupeKey: "email:1",
	}
}

func mustTime(raw string) time.Time {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return parsed
}
