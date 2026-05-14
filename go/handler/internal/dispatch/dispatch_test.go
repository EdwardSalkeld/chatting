package dispatch

import (
	"context"
	"errors"
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
