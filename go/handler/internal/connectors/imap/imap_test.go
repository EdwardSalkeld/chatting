package imap

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestPollNormalizesMessagesToEnvelopes(t *testing.T) {
	raw := buildRawEmail("alice@example.com", "Please summarize", "Summarize this inbox thread.", "Sat, 28 Feb 2026 10:15:00 +0000")
	fake := &fakeClient{messages: map[string][]byte{"101": []byte(raw)}}
	connector, err := New(Config{
		Host:            "imap.example.com",
		Port:            993,
		Username:        "bot@example.com",
		Password:        "secret",
		ContextRefs:     []string{"repo:/home/edward/chatting"},
		SearchCriterion: "UNSEEN",
		ClientFactory: func(ctx context.Context, config ClientConfig) (Client, error) {
			return fake, nil
		},
		Now: func() time.Time {
			return time.Date(2026, 2, 28, 10, 30, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(envelopes) != 1 {
		t.Fatalf("len(envelopes) = %d", len(envelopes))
	}
	envelope := envelopes[0]
	if envelope.Source != "email" {
		t.Fatalf("Source = %q", envelope.Source)
	}
	if envelope.Actor == nil || *envelope.Actor != "alice@example.com" {
		t.Fatalf("Actor = %#v", envelope.Actor)
	}
	if envelope.ReplyChannel.Target != "alice@example.com" {
		t.Fatalf("ReplyChannel.Target = %q", envelope.ReplyChannel.Target)
	}
	if envelope.DedupeKey != "email:101" {
		t.Fatalf("DedupeKey = %q", envelope.DedupeKey)
	}
	if envelope.Content != "Subject: Please summarize\n\nSummarize this inbox thread." {
		t.Fatalf("Content = %q", envelope.Content)
	}
	if !envelope.ReceivedAt.Equal(time.Date(2026, 2, 28, 10, 15, 0, 0, time.UTC)) {
		t.Fatalf("ReceivedAt = %s", envelope.ReceivedAt.Format(time.RFC3339))
	}
	if len(envelope.ContextRefs) != 1 || envelope.ContextRefs[0] != "repo:/home/edward/chatting" {
		t.Fatalf("ContextRefs = %#v", envelope.ContextRefs)
	}
	if !fake.loggedOut {
		t.Fatal("client was not logged out")
	}
}

func TestPollAttachesPromptContext(t *testing.T) {
	raw := buildRawEmail("alice@example.com", "Please summarize", "Body", "Sat, 28 Feb 2026 10:15:00 +0000")
	fake := &fakeClient{messages: map[string][]byte{"101": []byte(raw)}}
	connector, err := New(Config{
		Host:     "imap.example.com",
		Port:     993,
		Username: "bot@example.com",
		Password: "secret",
		PromptContext: contracts.PromptContext{
			GlobalInstructions:       []string{"Keep replies concise."},
			ReplyChannelInstructions: []string{"Use a clear email subject line."},
		},
		ClientFactory: func(ctx context.Context, config ClientConfig) (Client, error) {
			return fake, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	got := envelopes[0].PromptContext.AssembledInstructions()
	want := []string{"Keep replies concise.", "Use a clear email subject line."}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("instructions = %#v", got)
	}
}

func TestPollFallsBackToNowForInvalidDateHeader(t *testing.T) {
	raw := buildRawEmail("alice@example.com", "No Date", "Body", "not-a-date")
	fake := &fakeClient{messages: map[string][]byte{"201": []byte(raw)}}
	fallback := time.Date(2026, 2, 28, 11, 0, 0, 0, time.UTC)
	connector, err := New(Config{
		Host:     "imap.example.com",
		Port:     993,
		Username: "bot@example.com",
		Password: "secret",
		ClientFactory: func(ctx context.Context, config ClientConfig) (Client, error) {
			return fake, nil
		},
		Now: func() time.Time { return fallback },
	})
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !envelopes[0].ReceivedAt.Equal(fallback) {
		t.Fatalf("ReceivedAt = %s", envelopes[0].ReceivedAt.Format(time.RFC3339))
	}
}

func TestPollUsesUsernameAsReplyTargetWhenFromAddressIsMissing(t *testing.T) {
	raw := "To: bot@example.com\r\nSubject: No Sender\r\nDate: Sat, 28 Feb 2026 10:15:00 +0000\r\n\r\nBody"
	fake := &fakeClient{messages: map[string][]byte{"301": []byte(raw)}}
	connector, err := New(Config{
		Host:     "imap.example.com",
		Port:     993,
		Username: "bot@example.com",
		Password: "secret",
		ClientFactory: func(ctx context.Context, config ClientConfig) (Client, error) {
			return fake, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	envelopes, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if envelopes[0].Actor != nil {
		t.Fatalf("Actor = %#v", envelopes[0].Actor)
	}
	if envelopes[0].ReplyChannel.Target != "bot@example.com" {
		t.Fatalf("ReplyChannel.Target = %q", envelopes[0].ReplyChannel.Target)
	}
}

type fakeClient struct {
	messages  map[string][]byte
	loggedOut bool
}

func (client *fakeClient) Login(username string, password string) error {
	return nil
}

func (client *fakeClient) Select(mailbox string) error {
	return nil
}

func (client *fakeClient) Search(criterion string) ([]string, error) {
	uids := make([]string, 0, len(client.messages))
	for uid := range client.messages {
		uids = append(uids, uid)
	}
	return uids, nil
}

func (client *fakeClient) FetchRFC822(uid string) ([]byte, error) {
	return client.messages[uid], nil
}

func (client *fakeClient) Logout() error {
	client.loggedOut = true
	return nil
}

func buildRawEmail(sender string, subject string, body string, date string) string {
	return "From: " + sender + "\r\n" +
		"To: bot@example.com\r\n" +
		"Subject: " + subject + "\r\n" +
		"Date: " + date + "\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"\r\n" +
		body
}
