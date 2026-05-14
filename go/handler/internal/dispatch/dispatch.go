package dispatch

import (
	"context"
	"errors"
	"strings"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type EmailSender interface {
	Send(ctx context.Context, target string, body string, subject *string) error
}

type Dispatcher struct {
	EmailSender EmailSender
}

type MessageDispatchError struct {
	ReasonCode string
}

func (err MessageDispatchError) Error() string {
	return err.ReasonCode
}

func (dispatcher Dispatcher) Dispatch(ctx context.Context, message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*contracts.OutboundMessage, error) {
	channel, target := resolveDispatchChannelAndTarget(message, envelope)
	normalized := message
	normalized.Channel = channel
	normalized.Target = target

	switch channel {
	case "log", "drop":
		return &normalized, nil
	case "email":
		if dispatcher.EmailSender == nil {
			return nil, nil
		}
		subject, body, err := formatEmailReply(message, envelope)
		if err != nil {
			return nil, err
		}
		if err := dispatcher.EmailSender.Send(ctx, target, body, subject); err != nil {
			return nil, MessageDispatchError{ReasonCode: "email_dispatch_failed"}
		}
		return &normalized, nil
	default:
		if heartbeat.IsLogPong(normalized, envelope) {
			return &normalized, nil
		}
		return nil, errors.New("dispatch is not implemented in the Go handler yet")
	}
}

func resolveDispatchChannelAndTarget(message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (string, string) {
	if message.Channel != "final" {
		return message.Channel, message.Target
	}
	return envelope.ReplyChannel.Type, envelope.ReplyChannel.Target
}

func formatEmailReply(message contracts.OutboundMessage, envelope contracts.TaskEnvelope) (*string, string, error) {
	if message.Body == nil {
		return nil, "", errors.New("email body is required")
	}
	cleanedBody := stripLeadingSubjectLine(*message.Body)
	if envelope.Source != "email" {
		return nil, cleanedBody, nil
	}

	originalSubject, originalBody := parseEmailEnvelopeContent(envelope.Content)
	replySubject := toReplySubject(originalSubject)
	if originalBody == "" {
		return &replySubject, cleanedBody, nil
	}

	lines := strings.Split(originalBody, "\n")
	quoted := make([]string, 0, len(lines))
	for _, line := range lines {
		quoted = append(quoted, "> "+line)
	}
	quotedOriginal := strings.Join(quoted, "\n")
	if quotedOriginal == "" {
		return &replySubject, cleanedBody, nil
	}
	replyBody := strings.TrimRight(cleanedBody, " \t\r\n") + "\n\nOriginal message:\n" + quotedOriginal + "\n"
	return &replySubject, replyBody, nil
}

func parseEmailEnvelopeContent(content string) (string, string) {
	if !strings.HasPrefix(content, "Subject: ") {
		return "(no subject)", strings.TrimSpace(content)
	}
	subject, body, ok := strings.Cut(content, "\n\n")
	if !ok {
		body = ""
	}
	subjectValue := strings.TrimSpace(strings.TrimPrefix(subject, "Subject: "))
	if subjectValue == "" {
		subjectValue = "(no subject)"
	}
	return subjectValue, strings.TrimSpace(body)
}

func toReplySubject(subject string) string {
	if strings.HasPrefix(strings.ToLower(subject), "re:") {
		return subject
	}
	return "Re: " + subject
}

func stripLeadingSubjectLine(body string) string {
	stripped := strings.TrimLeft(body, " \t\r\n")
	if !strings.HasPrefix(strings.ToLower(stripped), "subject:") {
		return body
	}
	firstLine, remainder, _ := strings.Cut(stripped, "\n")
	subjectCandidate := strings.TrimSpace(strings.TrimPrefix(firstLine, "Subject:"))
	if subjectCandidate == "" {
		return body
	}
	cleaned := strings.TrimLeft(remainder, " \t\r\n")
	if cleaned == "" {
		return body
	}
	return cleaned
}
