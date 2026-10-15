package personalinbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/jmap"
)

// How much of a body is handed to the classifier, and how many new messages one
// cycle takes on. Both bound a single pass, not the backlog: whatever is left
// arrives on the next cycle.
const (
	MaxBodyBytes   = 60000
	ChangesPerPass = 25
	BackfillLimit  = 500
)

type Broker interface {
	EnsureQueue(context.Context, string) error
	PublishJSON(context.Context, string, map[string]any) (string, error)
	PickupJSON(context.Context, string, int, int) (*bbmb.PickedMessage, error)
	Ack(context.Context, string, string) error
}

// Mailbox is the JMAP surface this workflow needs. Nothing here can send mail,
// delete it, or reach any mailbox the token does not already cover.
type Mailbox interface {
	AccountID(context.Context) (string, error)
	Mailboxes(context.Context) ([]jmap.Mailbox, error)
	EmailState(context.Context) (string, error)
	Changes(context.Context, string, int) (jmap.Changes, error)
	InMailbox(context.Context, string, int) ([]string, error)
	Emails(context.Context, []string, int) ([]jmap.Email, error)
	File(context.Context, string, string, bool) error
}

// Mail is the classifier's view of a message: text only, no attachments, no
// headers beyond the ones shown here.
type Mail struct {
	From       string `json:"from"`
	Subject    string `json:"subject"`
	Date       string `json:"date"`
	Body       string `json:"body"`
	Incomplete bool   `json:"incomplete"`
}

type Classification struct {
	Category   string  `json:"category"`
	Summary    string  `json:"summary"`
	Action     string  `json:"action"`
	Deadline   string  `json:"deadline"`
	Confidence float64 `json:"confidence"`
}

func (value Classification) Validate() error {
	if value.Confidence < 0 || value.Confidence > 1 {
		return errors.New("invalid confidence")
	}
	if value.Category != "news" && value.Category != "order" && value.Category != "action" && value.Category != "review" {
		return errors.New("invalid category")
	}
	if strings.TrimSpace(value.Summary) == "" || len([]rune(value.Summary)) > 600 || len([]rune(value.Action)) > 600 || len([]rune(value.Deadline)) > 100 {
		return errors.New("invalid classification text")
	}
	return nil
}

type Performer interface {
	Perform(context.Context, Effect) (string, error)
}

type Service struct {
	Config    Config
	Store     *Store
	Broker    Broker
	Mail      Mailbox
	Folders   *Resolver
	Performer Performer
	Now       func() time.Time

	mu      sync.Mutex
	account string
}

func limitRunes(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

// mail converts a JMAP message into the classifier's view. Attachments and
// degraded bodies are surfaced rather than quietly filed: the server tells us
// outright, so there is no heuristic here.
func mail(email jmap.Email) Mail {
	body, degraded := email.Body()
	return Mail{
		From:       limitRunes(email.Sender(), 300),
		Subject:    limitRunes(email.Subject, 300),
		Date:       limitRunes(email.ReceivedAt, 100),
		Body:       limitRunes(body, MaxBodyBytes),
		Incomplete: degraded || email.HasAttachment,
	}
}

func strictJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func (service *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		if err := service.Cycle(ctx); err != nil && ctx.Err() == nil {
			// Do not log mail content, API errors containing secrets, or crash
			// the ordinary handler when this independent workflow is unhealthy.
			log.Printf("personal_inbox_cycle_failed: %v", err)
			_ = service.Store.putMeta(ctx, "last_error", err.Error())
		} else if ctx.Err() == nil {
			_ = service.Store.putMeta(ctx, "last_success", service.Now().UTC().Format(time.RFC3339))
			_ = service.Store.putMeta(ctx, "last_error", "")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (service *Service) Cycle(ctx context.Context) error {
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, err := service.Store.db.ExecContext(ctx, `UPDATE inbox_effects SET data=json_set(data,'$.status','uncertain','$.note','interrupted before durable receipt') WHERE json_extract(data,'$.status')='inflight'`); err != nil {
		return err
	}
	for _, queue := range []string{TasksQueue, ResultsQueue, DeadQueue} {
		if err := service.Broker.EnsureQueue(ctx, queue); err != nil {
			return errors.New("personal inbox queue unavailable")
		}
	}
	if service.account == "" {
		account, err := service.Mail.AccountID(ctx)
		if err != nil {
			return errors.New("personal inbox account unavailable")
		}
		service.account = account
	}
	// Recovery and notifications continue even if the mail server is down, so
	// each step records its own health: one failing step must not make the
	// whole workflow look dead to whoever is watching the status endpoint.
	steps := []struct {
		name string
		run  func(context.Context) error
	}{
		{"results", service.results}, {"ingest", service.ingest}, {"publish", service.publish},
		{"digest", service.digest}, {"perform", service.perform},
	}
	stamp := service.Now().UTC().Format(time.RFC3339)
	var failures []error
	for _, step := range steps {
		err := step.run(ctx)
		if err != nil {
			failures = append(failures, err)
			_ = service.Store.putMeta(ctx, "step_error:"+step.name, err.Error())
			continue
		}
		_ = service.Store.putMeta(ctx, "step_success:"+step.name, stamp)
		_ = service.Store.putMeta(ctx, "step_error:"+step.name, "")
	}
	return errors.Join(failures...)
}

func (service *Service) store(ctx context.Context, inbox string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	emails, err := service.Mail.Emails(ctx, ids, MaxBodyBytes)
	if err != nil {
		return errors.New("personal inbox fetch failed")
	}
	for _, email := range emails {
		// Mail that arrived and was filed elsewhere before we looked is outside
		// this workflow's view, exactly as it was under IMAP.
		if !email.MailboxIDs[inbox] {
			continue
		}
		message := Message{ID: email.ID, ThreadID: email.ThreadID, Mail: mail(email), Created: service.Now().Unix()}
		if err := service.Store.insertMessage(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) ingest(ctx context.Context) error {
	inbox, err := service.Folders.Inbox(ctx)
	if err != nil {
		return errors.New("personal inbox mailbox unavailable")
	}
	state, err := service.Store.getMeta(ctx, "email_state")
	if err != nil {
		return err
	}
	if state == "" {
		return service.baseline(ctx, inbox)
	}
	changes, err := service.Mail.Changes(ctx, state, ChangesPerPass)
	if errors.Is(err, jmap.ErrCannotCalculateChanges) {
		// The server can no longer answer from this state. Re-baseline instead
		// of replaying the whole mailbox as if it had just arrived.
		log.Printf("personal_inbox_state_expired")
		_ = service.Store.putMeta(ctx, "state_reset_at", service.Now().UTC().Format(time.RFC3339))
		return service.baselineAt(ctx)
	}
	if err != nil {
		return errors.New("personal inbox change feed unavailable")
	}
	// Every created id in this batch is stored before the state advances, so an
	// interruption replays them; inserts ignore ids already held.
	if err := service.store(ctx, inbox, changes.Created); err != nil {
		return err
	}
	if changes.NewState == "" {
		return errors.New("personal inbox change feed returned no new state")
	}
	return service.Store.putMeta(ctx, "email_state", changes.NewState)
}

// baselineAt records where the mailbox is now and ingests nothing before it.
func (service *Service) baselineAt(ctx context.Context) error {
	state, err := service.Mail.EmailState(ctx)
	if err != nil {
		return errors.New("personal inbox state unavailable")
	}
	return service.Store.putMeta(ctx, "email_state", state)
}

func (service *Service) baseline(ctx context.Context, inbox string) error {
	if !service.Config.Backfill {
		return service.baselineAt(ctx)
	}
	// Read the state first: anything arriving during the backfill then shows up
	// as a change rather than being skipped.
	state, err := service.Mail.EmailState(ctx)
	if err != nil {
		return errors.New("personal inbox state unavailable")
	}
	ids, err := service.Mail.InMailbox(ctx, inbox, BackfillLimit)
	if err != nil {
		return errors.New("personal inbox backfill query failed")
	}
	for start := 0; start < len(ids); start += ChangesPerPass {
		end := min(start+ChangesPerPass, len(ids))
		if err := service.store(ctx, inbox, ids[start:end]); err != nil {
			return err
		}
	}
	return service.Store.putMeta(ctx, "email_state", state)
}

func (service *Service) publish(ctx context.Context) error {
	messages, err := service.Store.messages(ctx, "json_extract(data,'$.classification') IS NULL AND json_extract(data,'$.published') <= ?", service.Now().Unix()-600)
	if err != nil {
		return err
	}
	for _, message := range messages {
		payload := map[string]any{"kind": "personal_inbox.classify.v1", "message_id": message.ID,
			"work_item_id": "personal-inbox:" + service.Config.Account, "mail": message.Mail}
		if _, err := service.Broker.PublishJSON(ctx, TasksQueue, payload); err != nil {
			return errors.New("personal inbox task publish failed")
		}
		message.Published = service.Now().Unix()
		if err := service.Store.saveMessage(ctx, message, nil); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) results(ctx context.Context) error {
	for i := 0; i < 100; i++ {
		picked, err := service.Broker.PickupJSON(ctx, ResultsQueue, 30, 0)
		if err != nil {
			return errors.New("personal inbox result queue failed")
		}
		if picked == nil {
			return nil
		}
		var result struct {
			Kind           string         `json:"kind"`
			MessageID      string         `json:"message_id"`
			Classification Classification `json:"classification"`
		}
		raw, _ := json.Marshal(picked.Payload)
		err = strictJSON(raw, &result)
		if err == nil {
			err = result.Classification.Validate()
		}
		if err != nil || result.Kind != "personal_inbox.result.v1" {
			if _, err := service.Broker.PublishJSON(ctx, DeadQueue, map[string]any{"guid": picked.GUID, "reason": "invalid_result", "payload": picked.Payload}); err != nil {
				return err
			}
		} else {
			messages, err := service.Store.messages(ctx, "id=?", result.MessageID)
			if err != nil {
				return err
			}
			if len(messages) == 0 {
				if _, err := service.Broker.PublishJSON(ctx, DeadQueue, map[string]any{"guid": picked.GUID, "reason": "unknown_message", "message_id": result.MessageID}); err != nil {
					return err
				}
			}
			if len(messages) > 0 && messages[0].Classification == nil {
				message := messages[0]
				classification := result.Classification
				// Action always wins, including an action extracted by a model
				// that mistakenly labelled an order/news email as routine.
				if strings.TrimSpace(classification.Action) != "" || strings.TrimSpace(classification.Deadline) != "" {
					classification.Category = "action"
				}
				if message.Mail.Incomplete && classification.Category != "action" {
					classification.Category = "review"
				}
				if classification.Confidence < 0.90 {
					classification.Category = "review"
				}
				message.Classification = &classification
				if err := service.Store.saveMessage(ctx, message, service.plan(message)); err != nil {
					return err
				}
			}
		}
		if err := service.Broker.Ack(ctx, ResultsQueue, picked.GUID); err != nil {
			return err
		}
	}
	return nil
}

// link points at the message in the mail client. The folder segment is
// decorative — Fastmail resolves by thread and message id — so a link keeps
// working after the message is filed, or moved again by hand.
func (service *Service) link(message Message, folder string) string {
	if service.Config.LinkTemplate == "" || message.ThreadID == "" || service.account == "" {
		return ""
	}
	if folder == "" {
		folder = "Inbox"
	}
	return strings.NewReplacer(
		"{folder}", url.PathEscape(folder),
		"{thread}", url.PathEscape(message.ThreadID),
		"{email}", url.PathEscape(message.ID),
		"{account}", url.PathEscape(strings.TrimPrefix(service.account, "u")),
		"{account_id}", url.PathEscape(service.account),
	).Replace(service.Config.LinkTemplate)
}

func (service *Service) plan(message Message) []Effect {
	classification := message.Classification
	folder := service.Config.Folder(classification.Category)
	body := classification.Category + ": " + message.Mail.Subject + "\nFrom: " + message.Mail.From + "\n" + classification.Summary
	if classification.Action != "" {
		body += "\nAction: " + classification.Action
	}
	if classification.Deadline != "" {
		body += "\nDeadline: " + classification.Deadline
	}
	if len([]rune(body)) > 1800 {
		body = limitRunes(body, 1800)
	}
	if link := service.link(message, folder); link != "" {
		body += "\n" + link
	} else {
		body += "\nMail reference: " + message.ID
	}
	effects := []Effect{}
	add := func(kind, target string) {
		status := "pending"
		if service.Config.DryRun {
			status = "preview"
		}
		effects = append(effects, Effect{ID: message.ID + ":" + kind, MessageID: message.ID, Kind: kind,
			Category: classification.Category, Target: target, Body: body, Status: status})
	}
	switch classification.Category {
	case "order":
		add("telegram", service.Config.OrdersTarget)
	case "action":
		add("telegram", service.Config.ActionTarget)
		add("linear", service.Config.LinearTeam)
	case "review":
		add("telegram", service.Config.ActionTarget)
	}
	// News is relayed by the digest rather than one alert per newsletter.
	if folder != "" {
		add("file", folder)
	}
	return effects
}

func (service *Service) digest(ctx context.Context) error {
	messages, err := service.Store.messages(ctx, "json_extract(data,'$.classification.category')='news' AND COALESCE(json_extract(data,'$.digest_id'),'')='' AND json_extract(data,'$.created')<=?", service.Now().Unix()-int64(service.Config.DigestSeconds))
	if err != nil {
		return err
	}
	// Bounded messages keep every Telegram digest comfortably below its limit.
	for len(messages) > 0 {
		size := min(2, len(messages))
		if err := service.Store.planDigest(ctx, messages[:size], service.Config.DigestTarget, service.Config.DryRun); err != nil {
			return err
		}
		messages = messages[size:]
	}
	return nil
}

func (service *Service) perform(ctx context.Context) error {
	effects, err := service.Store.effects(ctx, "json_extract(data,'$.status')='pending'")
	if err != nil {
		return err
	}
	var failures []error
	for _, effect := range effects {
		if service.Config.DryRun {
			continue
		} // Pausing live effects is immediate.
		// Stop a previously planned filing when the operator clears the folder.
		if effect.Kind == "file" && service.Config.Folder(effect.Category) != effect.Target {
			continue
		}
		effect.Status = "inflight"
		if err := service.Store.saveEffect(ctx, effect); err != nil {
			return err
		}
		receipt, err := service.Performer.Perform(ctx, effect)
		if err != nil {
			effect.Status = "uncertain"
			effect.Note = "external outcome unknown; inspect destination before resolving"
			log.Printf("personal_inbox_effect_uncertain id=%s kind=%s", effect.ID, effect.Kind)
			failures = append(failures, errors.New("personal inbox effect outcome uncertain"))
		} else {
			effect.Status = "done"
			effect.Receipt = receipt
		}
		// Failure here leaves inflight durable. Startup converts it to uncertain.
		if err := service.Store.saveEffect(ctx, effect); err != nil {
			return err
		}
	}
	return errors.Join(failures...)
}
