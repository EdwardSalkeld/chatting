// Package jmap is a minimal JMAP mail client: session discovery and the handful
// of methods the personal inbox needs. It speaks JSON over HTTPS, so unlike the
// IMAP client there is no wire format to parse and no command injection surface.
package jmap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/mailtext"
)

const (
	CoreCapability = "urn:ietf:params:jmap:core"
	MailCapability = "urn:ietf:params:jmap:mail"
	SeenKeyword    = "$seen"
)

// ErrCannotCalculateChanges means the server can no longer answer from the
// stored state and the caller must re-establish a baseline.
var ErrCannotCalculateChanges = errors.New("jmap: server cannot calculate changes from this state")

type Client struct {
	HTTP       *http.Client
	Token      string
	SessionURL string

	mu      sync.Mutex
	session *Session
}

type Session struct {
	APIURL    string `json:"-"`
	AccountID string `json:"-"`
	// MaxCallsInRequest bounds how many method calls may be batched together.
	MaxCallsInRequest int `json:"-"`
}

type sessionPayload struct {
	APIURL          string `json:"apiUrl"`
	PrimaryAccounts map[string]string
	Capabilities    map[string]json.RawMessage
}

func (payload *sessionPayload) UnmarshalJSON(raw []byte) error {
	var shape struct {
		APIURL          string                     `json:"apiUrl"`
		PrimaryAccounts map[string]string          `json:"primaryAccounts"`
		Capabilities    map[string]json.RawMessage `json:"capabilities"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		return err
	}
	payload.APIURL = shape.APIURL
	payload.PrimaryAccounts = shape.PrimaryAccounts
	payload.Capabilities = shape.Capabilities
	return nil
}

type EmailAddress struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type BodyPart struct {
	PartID string `json:"partId"`
	Type   string `json:"type"`
}

type BodyValue struct {
	Value             string `json:"value"`
	IsTruncated       bool   `json:"isTruncated"`
	IsEncodingProblem bool   `json:"isEncodingProblem"`
}

type Mailbox struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Role     string `json:"role"`
	ParentID string `json:"parentId"`
}

type Email struct {
	ID            string               `json:"id"`
	ThreadID      string               `json:"threadId"`
	MailboxIDs    map[string]bool      `json:"mailboxIds"`
	Keywords      map[string]bool      `json:"keywords"`
	MessageID     []string             `json:"messageId"`
	Subject       string               `json:"subject"`
	From          []EmailAddress       `json:"from"`
	ReceivedAt    string               `json:"receivedAt"`
	Size          int64                `json:"size"`
	HasAttachment bool                 `json:"hasAttachment"`
	Preview       string               `json:"preview"`
	TextBody      []BodyPart           `json:"textBody"`
	HTMLBody      []BodyPart           `json:"htmlBody"`
	BodyValues    map[string]BodyValue `json:"bodyValues"`
}

type Changes struct {
	OldState       string   `json:"oldState"`
	NewState       string   `json:"newState"`
	HasMoreChanges bool     `json:"hasMoreChanges"`
	Created        []string `json:"created"`
	Updated        []string `json:"updated"`
	Destroyed      []string `json:"destroyed"`
}

// Sender is the display name of the first From address, falling back to its
// address. The digest lists mail by who sent it, so this is what it shows.
func (email Email) Sender() string {
	if len(email.From) == 0 {
		return ""
	}
	if name := strings.TrimSpace(email.From[0].Name); name != "" {
		return name
	}
	return strings.TrimSpace(email.From[0].Email)
}

func (email Email) FromAddress() string {
	if len(email.From) == 0 {
		return ""
	}
	return strings.TrimSpace(email.From[0].Email)
}

func (email Email) HeaderMessageID() string {
	if len(email.MessageID) == 0 {
		return ""
	}
	return email.MessageID[0]
}

// Body is a message rendered for reading: markup stripped, and the two ways it
// can fall short kept apart. Truncated means the part was longer than we asked
// for, which is our limit rather than a fault in the mail. Degraded means the
// server could not give us readable content at all.
type Body struct {
	Text      string
	HTML      bool
	Truncated bool
	Degraded  bool
}

// BodyText prefers the plain text part and falls back to HTML, converting it.
// Parts are not always labelled accurately, so content decides, not the type.
func (email Email) BodyText() Body {
	for _, parts := range [][]BodyPart{email.TextBody, email.HTMLBody} {
		for _, part := range parts {
			value, ok := email.BodyValues[part.PartID]
			if !ok || strings.TrimSpace(value.Value) == "" {
				continue
			}
			text, markup := value.Value, mailtext.LooksLikeHTML(value.Value)
			if markup {
				text = mailtext.HTMLToText(text)
			}
			// An image-only part converts to nothing; try the next one.
			if strings.TrimSpace(text) == "" {
				continue
			}
			return Body{Text: strings.TrimSpace(text), HTML: markup,
				Truncated: value.IsTruncated, Degraded: value.IsEncodingProblem}
		}
	}
	return Body{Degraded: true}
}

func (client *Client) sessionURL() string {
	if strings.TrimSpace(client.SessionURL) != "" {
		return client.SessionURL
	}
	return "https://api.fastmail.com/jmap/session"
}

func (client *Client) do(ctx context.Context, method, url string, body []byte) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+client.Token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.HTTP.Do(request)
	if err != nil {
		// Never surface the error verbatim: it can carry the request URL and,
		// through redirects, header material.
		return nil, errors.New("jmap request failed")
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 64*1024*1024))
	if err != nil {
		return nil, errors.New("jmap response unreadable")
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jmap rejected request: status %d", response.StatusCode)
	}
	return payload, nil
}

// Session discovers the account id and regional API URL. Fastmail serves each
// account from its own region, so the API URL must come from here and never be
// hardcoded.
func (client *Client) Session(ctx context.Context) (Session, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.session != nil {
		return *client.session, nil
	}
	raw, err := client.do(ctx, http.MethodGet, client.sessionURL(), nil)
	if err != nil {
		return Session{}, err
	}
	var payload sessionPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return Session{}, errors.New("jmap session malformed")
	}
	account := payload.PrimaryAccounts[MailCapability]
	if strings.TrimSpace(payload.APIURL) == "" || strings.TrimSpace(account) == "" {
		return Session{}, errors.New("jmap session missing mail account")
	}
	session := Session{APIURL: payload.APIURL, AccountID: account, MaxCallsInRequest: 16}
	if core, ok := payload.Capabilities[CoreCapability]; ok {
		var limits struct {
			MaxCallsInRequest int `json:"maxCallsInRequest"`
		}
		if json.Unmarshal(core, &limits) == nil && limits.MaxCallsInRequest > 0 {
			session.MaxCallsInRequest = limits.MaxCallsInRequest
		}
	}
	client.session = &session
	return session, nil
}

type invocation struct {
	Method string
	Args   map[string]any
}

func (client *Client) invoke(ctx context.Context, calls []invocation) ([]json.RawMessage, error) {
	session, err := client.Session(ctx)
	if err != nil {
		return nil, err
	}
	methodCalls := make([][3]any, 0, len(calls))
	for index, call := range calls {
		methodCalls = append(methodCalls, [3]any{call.Method, call.Args, fmt.Sprintf("c%d", index)})
	}
	body, err := json.Marshal(map[string]any{
		"using":       []string{CoreCapability, MailCapability},
		"methodCalls": methodCalls,
	})
	if err != nil {
		return nil, err
	}
	raw, err := client.do(ctx, http.MethodPost, session.APIURL, body)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		MethodResponses []json.RawMessage `json:"methodResponses"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, errors.New("jmap response malformed")
	}
	if len(envelope.MethodResponses) != len(calls) {
		return nil, errors.New("jmap returned an unexpected number of responses")
	}
	results := make([]json.RawMessage, 0, len(calls))
	for index, response := range envelope.MethodResponses {
		var triple []json.RawMessage
		if err := json.Unmarshal(response, &triple); err != nil || len(triple) != 3 {
			return nil, errors.New("jmap method response malformed")
		}
		var name string
		if err := json.Unmarshal(triple[0], &name); err != nil {
			return nil, errors.New("jmap method name malformed")
		}
		if name == "error" {
			var failure struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(triple[1], &failure)
			if failure.Type == "cannotCalculateChanges" {
				return nil, ErrCannotCalculateChanges
			}
			return nil, fmt.Errorf("jmap %s failed: %s", calls[index].Method, failure.Type)
		}
		results = append(results, triple[1])
	}
	return results, nil
}

func (client *Client) AccountID(ctx context.Context) (string, error) {
	session, err := client.Session(ctx)
	return session.AccountID, err
}

func (client *Client) Mailboxes(ctx context.Context) ([]Mailbox, error) {
	account, err := client.AccountID(ctx)
	if err != nil {
		return nil, err
	}
	results, err := client.invoke(ctx, []invocation{{
		Method: "Mailbox/get",
		Args:   map[string]any{"accountId": account, "ids": nil},
	}})
	if err != nil {
		return nil, err
	}
	var payload struct {
		List []Mailbox `json:"list"`
	}
	if err := json.Unmarshal(results[0], &payload); err != nil {
		return nil, errors.New("jmap mailbox list malformed")
	}
	return payload.List, nil
}

// EmailState is the current server state, used as the baseline a later Changes
// call reports against.
func (client *Client) EmailState(ctx context.Context) (string, error) {
	account, err := client.AccountID(ctx)
	if err != nil {
		return "", err
	}
	results, err := client.invoke(ctx, []invocation{{
		Method: "Email/get",
		Args:   map[string]any{"accountId": account, "ids": []string{}, "properties": []string{"id"}},
	}})
	if err != nil {
		return "", err
	}
	var payload struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(results[0], &payload); err != nil || payload.State == "" {
		return "", errors.New("jmap state missing")
	}
	return payload.State, nil
}

func (client *Client) Changes(ctx context.Context, since string, max int) (Changes, error) {
	account, err := client.AccountID(ctx)
	if err != nil {
		return Changes{}, err
	}
	results, err := client.invoke(ctx, []invocation{{
		Method: "Email/changes",
		Args:   map[string]any{"accountId": account, "sinceState": since, "maxChanges": max},
	}})
	if err != nil {
		return Changes{}, err
	}
	var changes Changes
	if err := json.Unmarshal(results[0], &changes); err != nil {
		return Changes{}, errors.New("jmap changes malformed")
	}
	return changes, nil
}

// InMailbox lists message ids currently in one mailbox, oldest first.
func (client *Client) InMailbox(ctx context.Context, mailboxID string, limit int) ([]string, error) {
	account, err := client.AccountID(ctx)
	if err != nil {
		return nil, err
	}
	results, err := client.invoke(ctx, []invocation{{
		Method: "Email/query",
		Args: map[string]any{
			"accountId": account,
			"filter":    map[string]any{"inMailbox": mailboxID},
			"sort":      []map[string]any{{"property": "receivedAt", "isAscending": true}},
			"limit":     limit,
		},
	}})
	if err != nil {
		return nil, err
	}
	var payload struct {
		IDs []string `json:"ids"`
	}
	if err := json.Unmarshal(results[0], &payload); err != nil {
		return nil, errors.New("jmap query malformed")
	}
	return payload.IDs, nil
}

func (client *Client) Emails(ctx context.Context, ids []string, maxBodyBytes int) ([]Email, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	account, err := client.AccountID(ctx)
	if err != nil {
		return nil, err
	}
	results, err := client.invoke(ctx, []invocation{{
		Method: "Email/get",
		Args: map[string]any{
			"accountId": account,
			"ids":       ids,
			"properties": []string{"id", "threadId", "mailboxIds", "keywords", "messageId", "subject",
				"from", "receivedAt", "size", "hasAttachment", "preview", "textBody", "htmlBody",
				// Required explicitly: the fetch flags below populate bodyValues
				// but do not add it to an explicit property list.
				"bodyValues"},
			"fetchTextBodyValues": true,
			"fetchHTMLBodyValues": true,
			"maxBodyValueBytes":   maxBodyBytes,
		},
	}})
	if err != nil {
		return nil, err
	}
	var payload struct {
		List []Email `json:"list"`
	}
	if err := json.Unmarshal(results[0], &payload); err != nil {
		return nil, errors.New("jmap email list malformed")
	}
	return payload.List, nil
}

// File moves one message to a single mailbox and optionally marks it read, in
// one atomic set. IMAP needed two commands that could half-succeed; this cannot.
func (client *Client) File(ctx context.Context, id, mailboxID string, markRead bool) error {
	account, err := client.AccountID(ctx)
	if err != nil {
		return err
	}
	patch := map[string]any{"mailboxIds": map[string]bool{mailboxID: true}}
	if markRead {
		patch["keywords/"+SeenKeyword] = true
	}
	results, err := client.invoke(ctx, []invocation{{
		Method: "Email/set",
		Args:   map[string]any{"accountId": account, "update": map[string]any{id: patch}},
	}})
	if err != nil {
		return err
	}
	var payload struct {
		Updated    map[string]json.RawMessage `json:"updated"`
		NotUpdated map[string]struct {
			Type string `json:"type"`
		} `json:"notUpdated"`
	}
	if err := json.Unmarshal(results[0], &payload); err != nil {
		return errors.New("jmap set response malformed")
	}
	if failure, ok := payload.NotUpdated[id]; ok {
		return fmt.Errorf("jmap could not file message: %s", failure.Type)
	}
	if _, ok := payload.Updated[id]; !ok {
		return errors.New("jmap set reported neither success nor failure")
	}
	return nil
}
