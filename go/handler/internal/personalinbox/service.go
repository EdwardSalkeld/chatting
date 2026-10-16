package personalinbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// Fetched generously because the raw part is usually HTML: it is stripped
	// to text afterwards, so capping the fetch would cut the markup rather
	// than the words. MaxBodyBytes then caps the text that is kept.
	MaxFetchBytes  = 400000
	MaxBodyBytes   = 60000
	ChangesPerPass = 25
	BackfillLimit  = 500
	// One digest covers at most this many messages, and each contributes this
	// much of its body to the single batch call that writes the round-up.
	DigestMaxItems  = 60
	DigestBodyChars = 2000
	// A message republished this many times without an answer is not slow, it
	// is stuck, and the cycle says so rather than retrying in silence.
	StuckAttempts = 2
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
	Trace      *Trace  `json:"trace,omitempty"`
}

// Trace records what the classifier saw and which rule settled the category,
// so "why is this in review" is answerable from the record instead of being
// inferred from the mail afterwards.
type Trace struct {
	Reason         string  `json:"reason,omitempty"`
	JevCategory    string  `json:"jev_category,omitempty"`
	JevConfidence  float64 `json:"jev_confidence,omitempty"`
	RequiresAction bool    `json:"requires_action,omitempty"`
	HasDeadline    bool    `json:"has_deadline,omitempty"`
	Suspicious     bool    `json:"suspicious,omitempty"`
	TriageFailed   bool    `json:"triage_failed,omitempty"`
	Summarised     bool    `json:"summarised,omitempty"`
}

func (value Classification) Validate() error {
	if value.Confidence < 0 || value.Confidence > 1 {
		return errors.New("invalid confidence")
	}
	if value.Category != "news" && value.Category != "order" && value.Category != "action" && value.Category != "review" {
		return errors.New("invalid category")
	}
	// Routine mail carries no summary: the digest writes one line per message
	// from a single batch call rather than buying one summary per newsletter.
	if value.Category != "news" && strings.TrimSpace(value.Summary) == "" {
		return errors.New("classification needs a summary")
	}
	if len([]rune(value.Summary)) > 600 || len([]rune(value.Action)) > 600 || len([]rune(value.Deadline)) > 100 {
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

	mu        sync.Mutex
	accountMu sync.RWMutex
	account   string
}

func (service *Service) accountID() string {
	service.accountMu.RLock()
	defer service.accountMu.RUnlock()
	return service.account
}

func (service *Service) setAccountID(value string) {
	service.accountMu.Lock()
	defer service.accountMu.Unlock()
	service.account = value
}

func limitRunes(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

// mail converts a JMAP message into the classifier's view. Only an attachment
// or a body the server could not render makes it incomplete; being longer than
// we asked for does not, since that is our limit rather than a fault in the
// mail. The body facts come back too, for the record rather than the model.
func mail(email jmap.Email) (Mail, jmap.Body) {
	body := email.BodyText()
	return Mail{
		From:       limitRunes(email.Sender(), 300),
		Subject:    limitRunes(email.Subject, 300),
		Date:       limitRunes(email.ReceivedAt, 100),
		Body:       limitRunes(body.Text, MaxBodyBytes),
		Incomplete: body.Degraded || email.HasAttachment,
	}, body
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
	if service.accountID() == "" {
		account, err := service.Mail.AccountID(ctx)
		if err != nil {
			return errors.New("personal inbox account unavailable")
		}
		service.setAccountID(account)
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
	emails, err := service.Mail.Emails(ctx, ids, MaxFetchBytes)
	if err != nil {
		return errors.New("personal inbox fetch failed")
	}
	for _, email := range emails {
		// Mail that arrived and was filed elsewhere before we looked is outside
		// this workflow's view, exactly as it was under IMAP.
		if !email.MailboxIDs[inbox] {
			continue
		}
		view, body := mail(email)
		message := Message{ID: email.ID, ThreadID: email.ThreadID, Mail: view,
			BodyHTML: body.HTML, BodyTruncated: body.Truncated,
			BodyChars: len([]rune(view.Body)), Created: service.Now().Unix()}
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
		message.Attempts++
		if err := service.Store.saveMessage(ctx, message, nil); err != nil {
			return err
		}
	}
	// Read from stored state rather than from what this pass happened to
	// republish, so the signal stays up between windows instead of blinking
	// green until the next one comes round.
	_, _, stuck, err := service.Store.awaiting(ctx, service.Now().Unix())
	if err != nil {
		return err
	}
	if stuck > 0 {
		return fmt.Errorf("%d personal inbox messages have no classification after %d attempts", stuck, StuckAttempts)
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
		raw, _ := json.Marshal(picked.Payload)
		var probe struct {
			Kind string `json:"kind"`
		}
		_ = json.Unmarshal(raw, &probe)
		var invalid bool
		switch probe.Kind {
		case "personal_inbox.result.v1":
			invalid, err = service.classificationResult(ctx, raw)
		case "personal_inbox.digest_result.v1":
			invalid, err = service.digestResult(ctx, raw)
		default:
			invalid = true
		}
		if err != nil {
			return err
		}
		if invalid {
			if _, err := service.Broker.PublishJSON(ctx, DeadQueue, map[string]any{"guid": picked.GUID, "reason": "invalid_result", "payload": picked.Payload}); err != nil {
				return err
			}
			if err := service.Store.countDeadLetter(ctx); err != nil {
				return err
			}
		}
		if err := service.Broker.Ack(ctx, ResultsQueue, picked.GUID); err != nil {
			return err
		}
	}
	return nil
}

// The first result returns true when the payload is rubbish and belongs in the
// dead queue; an error is a storage failure worth retrying the whole cycle for.
func (service *Service) classificationResult(ctx context.Context, raw []byte) (bool, error) {
	var result struct {
		Kind           string         `json:"kind"`
		MessageID      string         `json:"message_id"`
		Classification Classification `json:"classification"`
	}
	if err := strictJSON(raw, &result); err != nil {
		return true, nil
	}
	if err := result.Classification.Validate(); err != nil {
		return true, nil
	}
	messages, err := service.Store.messages(ctx, "id=?", result.MessageID)
	if err != nil {
		return false, err
	}
	if len(messages) == 0 {
		return true, nil
	}
	if messages[0].Classification != nil {
		return false, nil
	}
	message := messages[0]
	classification := result.Classification
	if classification.Trace == nil {
		classification.Trace = &Trace{}
	}
	// One place settles the category, and records which rule did it.
	switch {
	case classification.Trace.TriageFailed:
		classification.Category, classification.Trace.Reason = "review", "triage_unavailable"
	case classification.Trace.Suspicious:
		classification.Category, classification.Trace.Reason = "review", "injection_suspected"
	case strings.TrimSpace(classification.Action) != "" || strings.TrimSpace(classification.Deadline) != "":
		// An action wins even when a model labelled the mail routine.
		classification.Category, classification.Trace.Reason = "action", "action_extracted"
	case message.Mail.Incomplete:
		classification.Category, classification.Trace.Reason = "review", "incomplete_body_or_attachment"
	case classification.Confidence < service.Config.Threshold():
		classification.Category, classification.Trace.Reason = "review", "confidence_below_threshold"
	default:
		classification.Trace.Reason = "accepted"
	}
	// Only routine mail is allowed to arrive without a summary, so anything
	// promoted out of news must have bought one.
	if classification.Category != "news" && strings.TrimSpace(classification.Summary) == "" {
		return true, nil
	}
	message.Classification = &classification
	return false, service.Store.saveMessage(ctx, message, service.plan(message))
}

func (service *Service) digestResult(ctx context.Context, raw []byte) (bool, error) {
	var result struct {
		Kind     string       `json:"kind"`
		DigestID string       `json:"digest_id"`
		Items    []DigestLine `json:"items"`
	}
	if err := strictJSON(raw, &result); err != nil {
		return true, nil
	}
	digests, err := service.Store.digests(ctx, "id=?", result.DigestID)
	if err != nil {
		return false, err
	}
	if len(digests) == 0 {
		return true, nil
	}
	digest := digests[0]
	if digest.Lines != nil {
		return false, nil
	}
	// The round-up must account for exactly the membership frozen when it was
	// opened: a model cannot drop a message, invent one, or list one twice.
	outstanding := map[string]bool{}
	for _, id := range digest.Messages {
		outstanding[id] = true
	}
	if len(result.Items) != len(outstanding) {
		return true, nil
	}
	for _, item := range result.Items {
		if !outstanding[item.Ref] {
			return true, nil
		}
		delete(outstanding, item.Ref)
		if strings.TrimSpace(item.Line) == "" || len([]rune(item.Line)) > 200 || len([]rune(item.Sender)) > 120 {
			return true, nil
		}
	}
	digest.Lines = result.Items
	return false, service.Store.saveDigest(ctx, digest)
}

// link points at the message in the mail client. The folder segment is
// decorative — Fastmail resolves by thread and message id — so a link keeps
// working after the message is filed, or moved again by hand.
func (service *Service) link(message Message, folder string) string {
	account := service.accountID()
	if service.Config.LinkTemplate == "" || message.ThreadID == "" || account == "" {
		return ""
	}
	if folder == "" {
		folder = "Inbox"
	}
	return strings.NewReplacer(
		"{folder}", url.PathEscape(folder),
		"{thread}", url.PathEscape(message.ThreadID),
		"{email}", url.PathEscape(message.ID),
		"{account}", url.PathEscape(strings.TrimPrefix(account, "u")),
		"{account_id}", url.PathEscape(account),
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
		if service.Config.CreatesTickets() {
			add("linear", service.Config.LinearTeam)
		}
	}
	// Review is deliberately not alerted. It means the workflow could not
	// classify the mail, and such mail is left in the inbox, so a message
	// saying so repeats what opening the inbox already shows.
	// News is relayed by the digest rather than one alert per newsletter.
	if folder != "" {
		add("file", folder)
	}
	return effects
}

// latestOccurrence is the most recent daily boundary at or before now.
func latestOccurrence(now time.Time, hour, minute int, location *time.Location) time.Time {
	local := now.In(location)
	today := time.Date(local.Year(), local.Month(), local.Day(), hour, minute, 0, 0, location)
	if today.After(local) {
		return today.AddDate(0, 0, -1)
	}
	return today
}

func (service *Service) digest(ctx context.Context) error {
	if err := service.openDigest(ctx); err != nil {
		return err
	}
	if err := service.publishDigests(ctx); err != nil {
		return err
	}
	return service.deliverDigests(ctx)
}

// openDigest freezes one day's routine mail into a round-up when its send time
// passes. A day with no routine mail still records the occurrence, so the next
// newsletter waits for tomorrow instead of firing a digest the moment it lands.
func (service *Service) openDigest(ctx context.Context) error {
	location, err := service.Config.Location()
	if err != nil {
		return err
	}
	hour, minute, err := service.Config.DigestTime()
	if err != nil {
		return err
	}
	id := "digest:" + latestOccurrence(service.Now(), hour, minute, location).Format("2006-01-02T15:04")
	seen, err := service.Store.getMeta(ctx, "digest_occurrence")
	if err != nil {
		return err
	}
	if seen >= id {
		return nil
	}
	messages, err := service.Store.messages(ctx, "json_extract(data,'$.classification.category')='news' AND COALESCE(json_extract(data,'$.digest_id'),'')=''")
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return service.Store.putMeta(ctx, "digest_occurrence", id)
	}
	if len(messages) > DigestMaxItems {
		messages = messages[:DigestMaxItems]
	}
	digest := Digest{ID: id, Created: service.Now().Unix()}
	for _, message := range messages {
		digest.Messages = append(digest.Messages, message.ID)
	}
	return service.Store.createDigest(ctx, digest, messages)
}

func (service *Service) publishDigests(ctx context.Context) error {
	digests, err := service.Store.digests(ctx, "json_extract(data,'$.lines') IS NULL AND json_extract(data,'$.published') <= ?", service.Now().Unix()-600)
	if err != nil {
		return err
	}
	for _, digest := range digests {
		items := []map[string]any{}
		for _, id := range digest.Messages {
			messages, err := service.Store.messages(ctx, "id=?", id)
			if err != nil {
				return err
			}
			if len(messages) == 0 {
				continue
			}
			// Marketing subjects say nothing ("A little thank you from us"), so
			// the line has to come from the body. The lead is enough: offers and
			// announcements put the substance at the top.
			items = append(items, map[string]any{"ref": id, "from": messages[0].Mail.From,
				"subject": messages[0].Mail.Subject, "body": limitRunes(messages[0].Mail.Body, DigestBodyChars)})
		}
		if len(items) == 0 {
			digest.Lines = []DigestLine{}
			if err := service.Store.saveDigest(ctx, digest); err != nil {
				return err
			}
			continue
		}
		payload := map[string]any{"kind": "personal_inbox.digest.v1", "digest_id": digest.ID,
			"work_item_id": "personal-inbox:" + service.Config.Account, "items": items}
		if _, err := service.Broker.PublishJSON(ctx, TasksQueue, payload); err != nil {
			return errors.New("personal inbox digest publish failed")
		}
		digest.Published = service.Now().Unix()
		if err := service.Store.saveDigest(ctx, digest); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) digestBody(digest Digest) string {
	date := strings.TrimPrefix(digest.ID, "digest:")
	if parsed, err := time.Parse("2006-01-02T15:04", date); err == nil {
		date = parsed.Format("2 Jan")
	}
	body := fmt.Sprintf("Inbox digest — %d since yesterday, %s\n", len(digest.Lines), date)
	shown := 0
	for _, line := range digest.Lines {
		entry := "\n• " + line.Sender + " — " + line.Line
		if len([]rune(body))+len([]rune(entry)) > 3800 {
			break
		}
		body += entry
		shown++
	}
	if remaining := len(digest.Lines) - shown; remaining > 0 {
		body += fmt.Sprintf("\n\n+%d more", remaining)
	}
	return body
}

func (service *Service) deliverDigests(ctx context.Context) error {
	digests, err := service.Store.digests(ctx, "json_extract(data,'$.lines') IS NOT NULL AND json_extract(data,'$.sent')=0")
	if err != nil {
		return err
	}
	for _, digest := range digests {
		digest.Sent = true
		if len(digest.Lines) == 0 {
			if err := service.Store.saveDigest(ctx, digest); err != nil {
				return err
			}
			continue
		}
		status := "pending"
		if service.Config.DryRun {
			status = "preview"
		}
		effect := Effect{ID: digest.ID + ":telegram", Kind: "telegram", Target: service.Config.DigestTarget,
			Body: service.digestBody(digest), Status: status}
		if err := service.Store.completeDigest(ctx, digest, effect); err != nil {
			return err
		}
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
