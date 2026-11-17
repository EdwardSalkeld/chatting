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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestDispatcherRejectsChannelWithoutConfiguredSender(t *testing.T) {
	body := "Done."
	for _, test := range []struct {
		name     string
		message  contracts.OutboundMessage
		envelope contracts.TaskEnvelope
	}{
		{name: "email", message: contracts.OutboundMessage{Channel: "email", Target: "alice@example.com", Body: &body}},
		{name: "telegram", message: contracts.OutboundMessage{Channel: "telegram", Target: "123", Body: &body}},
		{name: "telegram_reaction", message: contracts.OutboundMessage{Channel: "telegram_reaction", Target: "123", Body: &body, Metadata: map[string]any{"message_id": 7}}},
		{name: "github", message: contracts.OutboundMessage{Channel: "github", Target: "owner/repo#1", Body: &body}},
		{name: "final resolving to email", message: contracts.OutboundMessage{Channel: "final", Body: &body}, envelope: emailEnvelope("Question", "Body")},
	} {
		t.Run(test.name, func(t *testing.T) {
			dispatched, err := (Dispatcher{}).Dispatch(context.Background(), test.message, test.envelope)
			if !errors.Is(err, ErrChannelNotConfigured) {
				t.Fatalf("err = %v, want ErrChannelNotConfigured", err)
			}
			if dispatched != nil {
				t.Fatalf("dispatched = %#v", dispatched)
			}
		})
	}
}

func TestDispatcherLogAndDropNeedNoSender(t *testing.T) {
	body := "noted"
	for _, channel := range []string{"log", "drop"} {
		dispatched, err := (Dispatcher{}).Dispatch(context.Background(), contracts.OutboundMessage{Channel: channel, Body: &body}, contracts.TaskEnvelope{})
		if err != nil {
			t.Fatalf("%s: err = %v", channel, err)
		}
		if dispatched == nil || dispatched.Channel != channel {
			t.Fatalf("%s: dispatched = %#v", channel, dispatched)
		}
	}
}

func TestUnconfiguredChannels(t *testing.T) {
	channels := []string{"email", "telegram", "telegram_reaction", "github", "log"}
	if got, want := (Dispatcher{}).UnconfiguredChannels(channels), []string{"email", "telegram", "telegram_reaction", "github"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("UnconfiguredChannels = %#v, want %#v", got, want)
	}
	configured := Dispatcher{EmailSender: &recordingEmailSender{}, TelegramSender: &recordingTelegramSender{}}
	if got, want := configured.UnconfiguredChannels(channels), []string{"github"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("UnconfiguredChannels = %#v, want %#v", got, want)
	}
}

func TestDispatcherRendersEmailReplies(t *testing.T) {
	for _, body := range []string{"Here is the answer.", "Subject: Re: Question\n\nHere is the answer."} {
		sender := &recordingEmailSender{}
		message := contracts.OutboundMessage{Channel: "email", Target: "alice@example.com", Body: &body}
		_, err := (Dispatcher{EmailSender: sender}).Dispatch(context.Background(), message, emailEnvelope("Question", "Original question"))
		if err != nil {
			t.Fatal(err)
		}
		if len(sender.messages) != 1 {
			t.Fatalf("messages = %#v", sender.messages)
		}
		sent := sender.messages[0]
		if sent.subject == nil || *sent.subject != "Re: Question" || strings.HasPrefix(strings.TrimSpace(sent.body), "Subject:") {
			t.Fatalf("sent = %#v", sent)
		}
		for _, text := range []string{"Here is the answer.", "Original message:", "> Original question"} {
			if !strings.Contains(sent.body, text) {
				t.Fatalf("body = %q missing %q", sent.body, text)
			}
		}
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

func TestDispatcherPreservesFailureReasons(t *testing.T) {
	body := "Done."
	for _, tc := range []struct {
		channel, reason string
		dispatcher      Dispatcher
	}{
		{"email", "email_dispatch_failed", Dispatcher{EmailSender: &recordingEmailSender{err: errors.New("boom")}}},
		{"telegram", "telegram_attachment_missing", Dispatcher{TelegramSender: &recordingTelegramSender{err: errors.New("telegram_attachment_missing")}}},
		{"github", "github_dispatch_failed", Dispatcher{GitHubSender: &recordingGitHubSender{err: errors.New("boom")}}},
	} {
		t.Run(tc.channel, func(t *testing.T) {
			message := contracts.OutboundMessage{Channel: tc.channel, Target: "target", Body: &body}
			if tc.channel == "telegram" {
				message.Attachment = &contracts.AttachmentRef{URI: "/tmp/missing.png"}
			}
			_, err := tc.dispatcher.Dispatch(context.Background(), message, contracts.TaskEnvelope{})
			var dispatchErr MessageDispatchError
			if !errors.As(err, &dispatchErr) || dispatchErr.ReasonCode != tc.reason {
				t.Fatalf("error = %#v", err)
			}
		})
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

func TestTelegramMessageSenderSendsReaction(t *testing.T) {
	var call telegramHTTPCall
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call = recordTelegramJSONCall(t, request)
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
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

func TestTelegramMessageSenderReactionFailurePreservesDescription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok":          false,
			"error_code":  400,
			"description": "Bad Request: REACTION_INVALID",
		})
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL)

	err := sender.React(context.Background(), "12345", 99, "🙂")
	if err == nil {
		t.Fatal("expected reaction error")
	}
	if got, want := err.Error(), "telegram_reaction_failed description=Bad Request: REACTION_INVALID"; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestTelegramMessageSenderRejectsAttachmentOutsideAllowedDirs(t *testing.T) {
	tempDir := t.TempDir()
	allowedDir := filepath.Join(tempDir, "allowed")
	if err := os.MkdirAll(allowedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// A real file, but outside the allowlist — must be refused before any read.
	outsidePath := filepath.Join(tempDir, "secret.txt")
	if err := os.WriteFile(outsidePath, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	sender := newTestTelegramSender(t, "http://127.0.0.1", allowedDir)

	err := sender.Send(context.Background(), "12345", contracts.OutboundMessage{
		Channel: "telegram",
		Target:  "12345",
		Attachment: &contracts.AttachmentRef{
			URI: "file://" + outsidePath,
		},
	})
	if err == nil || err.Error() != "telegram_attachment_path_not_allowed" {
		t.Fatalf("err = %v, want telegram_attachment_path_not_allowed", err)
	}
	// Traversal out of an allowed dir must also be refused.
	traversal := filepath.Join(allowedDir, "..", "secret.txt")
	err = sender.Send(context.Background(), "12345", contracts.OutboundMessage{
		Channel:    "telegram",
		Target:     "12345",
		Attachment: &contracts.AttachmentRef{URI: "file://" + traversal},
	})
	if err == nil || err.Error() != "telegram_attachment_path_not_allowed" {
		t.Fatalf("traversal err = %v, want telegram_attachment_path_not_allowed", err)
	}
}

func TestTelegramMessageSenderRejectsSymlinksLeadingOutOfAllowedDirs(t *testing.T) {
	tempDir := t.TempDir()
	allowedDir := filepath.Join(tempDir, "allowed")
	outsideDir := filepath.Join(tempDir, "outside")
	for _, dir := range []string{allowedDir, outsideDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	secretPath := filepath.Join(outsideDir, "secret.txt")
	if err := os.WriteFile(secretPath, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	links := map[string]string{
		"file-link.txt":     secretPath,
		"relative-link.txt": filepath.Join("..", "outside", "secret.txt"),
		"dir-link":          outsideDir,
		"dangling-link.txt": filepath.Join(outsideDir, "absent.txt"),
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(allowedDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		t.Errorf("unexpected Telegram call: %s", request.URL.Path)
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL, allowedDir)

	for _, name := range []string{"file-link.txt", "relative-link.txt", filepath.Join("dir-link", "secret.txt"), "dangling-link.txt"} {
		err := sender.Send(context.Background(), "12345", contracts.OutboundMessage{
			Channel:    "telegram",
			Target:     "12345",
			Attachment: &contracts.AttachmentRef{URI: "file://" + filepath.Join(allowedDir, name)},
		})
		if err == nil || err.Error() != "telegram_attachment_path_not_allowed" {
			t.Errorf("%s: err = %v, want telegram_attachment_path_not_allowed", name, err)
		}
	}
}

func TestTelegramMessageSenderFollowsSymlinksThatStayInsideAllowedDirs(t *testing.T) {
	tempDir := t.TempDir()
	realDir := filepath.Join(tempDir, "real")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(realDir, "report.txt"), []byte("report"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("report.txt", filepath.Join(realDir, "latest.txt")); err != nil {
		t.Fatal(err)
	}
	// The allowed directory may itself be reached through a link.
	allowedDir := filepath.Join(tempDir, "allowed")
	if err := os.Symlink(realDir, allowedDir); err != nil {
		t.Fatal(err)
	}
	var multipartCall telegramMultipartCall
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		multipartCall = recordTelegramMultipartCall(t, request)
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 505}})
	}))
	defer server.Close()
	sender := newTestTelegramSender(t, server.URL, allowedDir)

	if err := sender.Send(context.Background(), "12345", contracts.OutboundMessage{
		Channel:    "telegram",
		Target:     "12345",
		Attachment: &contracts.AttachmentRef{URI: "file://" + filepath.Join(allowedDir, "latest.txt")},
	}); err != nil {
		t.Fatal(err)
	}
	if multipartCall.fileName != "latest.txt" || multipartCall.fileBody != "report" {
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

type sentReaction struct {
	target    string
	messageID int64
	emoji     string
}

type recordingTelegramSender struct {
	reactions []sentReaction
	err       error
}

func (sender *recordingTelegramSender) Send(context.Context, string, contracts.OutboundMessage) error {
	return sender.err
}

func (sender *recordingTelegramSender) React(_ context.Context, target string, messageID int64, emoji string) error {
	if sender.err != nil {
		return sender.err
	}
	sender.reactions = append(sender.reactions, sentReaction{target: target, messageID: messageID, emoji: emoji})
	return nil
}

type recordingGitHubSender struct{ err error }

func (sender *recordingGitHubSender) Send(context.Context, string, string) error {
	return sender.err
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
	fileBody  string
}

func newTestTelegramSender(t *testing.T, apiBaseURL string, allowedDirs ...string) *TelegramMessageSender {
	t.Helper()
	sender, err := NewTelegramMessageSender(TelegramConfig{
		BotToken:              "token",
		APIBaseURL:            apiBaseURL,
		AttachmentAllowedDirs: allowedDirs,
	})
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
		raw, err := io.ReadAll(part)
		if err != nil {
			t.Fatal(err)
		}
		call.fileBody = string(raw)
	}
	return call
}

func emailEnvelope(subject string, body string) contracts.TaskEnvelope {
	actor := "alice@example.com"
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            "email:1",
		Source:        "email",
		ReceivedAt:    contracts.NewTimestamp(time.Date(2026, 3, 6, 11, 0, 0, 0, time.UTC)),
		Actor:         &actor,
		Content:       "Subject: " + subject + "\n\n" + body,
		ReplyChannel: contracts.ReplyChannel{
			Type:   "email",
			Target: "alice@example.com",
		},
		DedupeKey: "email:1",
	}
}
