package jmap

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Opt-in check against a real account. Set JMAP_LIVE=1 and put a read-only
// token in ~/.config/chatting/fastmail-jmap-token. It never writes.
func TestLiveReadOnly(t *testing.T) {
	if os.Getenv("JMAP_LIVE") != "1" {
		t.Skip("set JMAP_LIVE=1 to run against a real account")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(home, ".config", "chatting", "fastmail-jmap-token"))
	if err != nil {
		t.Skip("no token file")
	}
	client := &Client{HTTP: &http.Client{Timeout: 30 * time.Second}, Token: strings.TrimSpace(string(raw))}
	ctx := context.Background()

	session, err := client.Session(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("account=%s maxCalls=%d apiHost=%s", session.AccountID, session.MaxCallsInRequest,
		strings.SplitN(strings.TrimPrefix(session.APIURL, "https://"), "/", 2)[0])

	boxes, err := client.Mailboxes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var inbox string
	duplicates := map[string]int{}
	for _, box := range boxes {
		duplicates[box.Name]++
		if box.Role == "inbox" {
			inbox = box.ID
		}
	}
	if inbox == "" {
		t.Fatal("no mailbox with role=inbox")
	}
	for name, count := range duplicates {
		if count > 1 {
			t.Logf("DUPLICATE FOLDER NAME: %q appears %d times", name, count)
		}
	}

	state, err := client.EmailState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("email state=%s", state)

	ids, err := client.InMailbox(ctx, inbox, 3)
	if err != nil {
		t.Fatal(err)
	}
	emails, err := client.Emails(ctx, ids, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for _, email := range emails {
		body, degraded := email.Body()
		t.Logf("id=%s thread=%s sender=%q attach=%v degraded=%v bodyRunes=%d msgid=%q",
			email.ID, email.ThreadID, email.Sender(), email.HasAttachment, degraded,
			len([]rune(body)), email.HeaderMessageID())
	}

	changes, err := client.Changes(ctx, state, 50)
	if err != nil {
		t.Fatalf("changes from current state: %v", err)
	}
	t.Logf("changes since now: created=%d updated=%d destroyed=%d more=%v",
		len(changes.Created), len(changes.Updated), len(changes.Destroyed), changes.HasMoreChanges)
}
