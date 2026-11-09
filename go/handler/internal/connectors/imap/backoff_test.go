package imap

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

type failingClient struct {
	fakeClient
	stage string
}

func (c *failingClient) Login(string, string) error {
	if c.stage == "login" {
		return errors.New("private login reply password")
	}
	return nil
}
func (c *failingClient) Select(string) error {
	if c.stage == "select" {
		return errors.New("private mailbox")
	}
	return nil
}
func (c *failingClient) Search(string) ([]string, error) {
	if c.stage == "search" {
		return nil, errors.New("private search")
	}
	return []string{"1", "2"}, nil
}
func (c *failingClient) FetchRFC822(uid string) ([]byte, error) {
	if c.stage == "fetch" && uid == "2" {
		return nil, errors.New("private email")
	}
	return []byte(buildRawEmail("private@example.test", "private subject", "private body", "")), nil
}

func TestPollBackoffStagesRecoveryAndPrivacy(t *testing.T) {
	for _, stage := range []string{"connect", "login", "select", "search", "fetch"} {
		t.Run(stage, func(t *testing.T) {
			now := time.Now()
			dials := 0
			client := &failingClient{stage: stage}
			var output bytes.Buffer
			old := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(old)
			c, err := New(Config{Host: "private.host", Port: 993, Username: "private user", Password: "private password", Now: func() time.Time { return now }, ClientFactory: func(context.Context, ClientConfig) (Client, error) {
				dials++
				if client.stage == "connect" {
					return nil, errors.New("private endpoint")
				}
				return client, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			for _, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
				envelopes, err := c.Poll(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if stage == "fetch" && len(envelopes) != 1 {
					t.Fatal("lost already-fetched message")
				}
				if c.pollDelay != delay {
					t.Fatalf("delay=%s want=%s", c.pollDelay, delay)
				}
				if stage != "connect" && !client.loggedOut {
					t.Fatal("session not closed")
				}
				before := dials
				if _, err := c.Poll(context.Background()); err != nil {
					t.Fatal(err)
				}
				if dials != before {
					t.Fatal("polled during backoff")
				}
				now = now.Add(delay)
			}
			client.stage = ""
			envelopes, err := c.Poll(context.Background())
			if err != nil || len(envelopes) != 2 {
				t.Fatalf("recovery=%d %v", len(envelopes), err)
			}
			if c.pollDelay != 0 || !c.nextPollAt.IsZero() {
				t.Fatal("backoff not reset")
			}
			if !strings.Contains(output.String(), "stage="+stage) || !strings.Contains(output.String(), "IMAP polling recovered") {
				t.Fatal(output.String())
			}
			if strings.Contains(output.String(), "private") {
				t.Fatal("private details leaked")
			}
		})
	}
}

func TestCancellationStillPropagatesDuringPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c, _ := New(Config{Host: "host", Port: 993, Username: "user", Password: "password", ClientFactory: func(context.Context, ClientConfig) (Client, error) { cancel(); return nil, context.Canceled }})
	if _, err := c.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if c.pollDelay != 0 {
		t.Fatal("cancellation scheduled retry")
	}
	if _, err := c.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestServerStatusClassificationDiscardsResponseText(t *testing.T) {
	for _, test := range []struct{ line, status string }{
		{"A0001 NO private password", "no"},
		{"A0001 BAD private email", "bad"},
		{"* BYE private username", "bye"},
		{"private-secret", "unknown"},
	} {
		err := safeResponseError(test.line)
		if err.Error() != "imap_response_"+test.status {
			t.Fatal(err)
		}
	}
}
