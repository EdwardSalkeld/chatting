package personalinbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Resolver turns configured folder names into mailbox ids. Names are not unique
// in a real account — a Fastmail account can hold two mailboxes both called
// "Archive" — so an ambiguous name is an error rather than a coin toss.
type Resolver struct {
	Mail Mailbox

	mu     sync.Mutex
	byPath map[string][]string
	inbox  string
}

func (resolver *Resolver) load(ctx context.Context) error {
	if resolver.byPath != nil {
		return nil
	}
	boxes, err := resolver.Mail.Mailboxes(ctx)
	if err != nil {
		return err
	}
	names := map[string]string{}
	parents := map[string]string{}
	for _, box := range boxes {
		names[box.ID] = box.Name
		parents[box.ID] = box.ParentID
	}
	byPath := map[string][]string{}
	for _, box := range boxes {
		segments := []string{box.Name}
		for parent := box.ParentID; parent != ""; parent = parents[parent] {
			name, ok := names[parent]
			if !ok {
				break
			}
			segments = append([]string{name}, segments...)
		}
		path := strings.Join(segments, "/")
		byPath[path] = append(byPath[path], box.ID)
		// A nested mailbox is also addressable by its leaf name when that name
		// is unambiguous, so short config entries keep working.
		if len(segments) > 1 {
			byPath[box.Name] = append(byPath[box.Name], box.ID)
		}
		if box.Role == "inbox" {
			resolver.inbox = box.ID
		}
	}
	if resolver.inbox == "" {
		return errors.New("account has no mailbox with role inbox")
	}
	resolver.byPath = byPath
	return nil
}

// Forget drops the cache so the next lookup re-reads the mailbox list. Called
// when a name fails to resolve, in case the operator has just created it.
func (resolver *Resolver) Forget() {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	resolver.byPath = nil
	resolver.inbox = ""
}

func (resolver *Resolver) Inbox(ctx context.Context) (string, error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if err := resolver.load(ctx); err != nil {
		return "", err
	}
	return resolver.inbox, nil
}

func (resolver *Resolver) ID(ctx context.Context, name string) (string, error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if err := resolver.load(ctx); err != nil {
		return "", err
	}
	matches := resolver.byPath[strings.Trim(strings.TrimSpace(name), "/")]
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", fmt.Errorf("mailbox %q does not exist; create it before filing into it", name)
	default:
		return "", fmt.Errorf("mailbox %q is ambiguous: %d mailboxes share that name, use its full path", name, len(matches))
	}
}

// CheckConfigured resolves every configured folder once, so a typo fails at
// startup rather than silently holding mail as pending forever.
func (resolver *Resolver) CheckConfigured(ctx context.Context, config Config) error {
	for _, category := range Categories {
		folder := config.Folder(category)
		if folder == "" {
			continue
		}
		if _, err := resolver.ID(ctx, folder); err != nil {
			return fmt.Errorf("personal_inbox folder for %s: %w", category, err)
		}
	}
	return nil
}
