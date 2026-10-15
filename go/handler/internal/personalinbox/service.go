package personalinbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/bbmb"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/imap"
)

type Broker interface {
	EnsureQueue(context.Context, string) error
	PublishJSON(context.Context, string, map[string]any) (string, error)
	PickupJSON(context.Context, string, int, int) (*bbmb.PickedMessage, error)
	Ack(context.Context, string, string) error
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
type Dial func(context.Context) (imap.UIDClient, error)

type Service struct {
	Config    Config
	Store     *Store
	Broker    Broker
	Dial      Dial
	Performer Performer
	Now       func() time.Time
	mu        sync.Mutex
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
	// Recovery and notifications continue even if IMAP is temporarily down.
	var failures []error
	for _, step := range []func(context.Context) error{service.results, service.ingest, service.publish, service.digest, service.perform} {
		if err := step(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (service *Service) ingest(ctx context.Context) error {
	client, err := service.Dial(ctx)
	if err != nil {
		return errors.New("personal inbox IMAP unavailable")
	}
	defer func() { _ = client.Logout() }()
	validity, next, err := client.SelectUID(service.Config.Mailbox)
	if err != nil {
		return errors.New("personal inbox mailbox selection failed")
	}
	stored, err := service.Store.getMeta(ctx, "identity")
	if err != nil {
		return err
	}
	identityBytes, _ := json.Marshal([]any{service.Config.Account, service.Config.Host, service.Config.Port, service.Config.Username, service.Config.Mailbox, validity})
	identity := string(identityBytes)
	if stored == "" {
		cursor := uint32(0)
		if !service.Config.Backfill {
			cursor = next - 1
		}
		// Baseline and identity must commit together, including first startup.
		tx, err := service.Store.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		for key, value := range map[string]string{"identity": identity, "cursor": strconv.FormatUint(uint64(cursor), 10)} {
			if _, err := tx.ExecContext(ctx, "INSERT INTO inbox_meta VALUES (?,?)", key, value); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	} else if stored != identity {
		return errors.New("personal inbox identity/UIDVALIDITY changed: ingestion paused; operator must reconcile")
	}
	value, err := service.Store.getMeta(ctx, "cursor")
	if err != nil {
		return err
	}
	cursor, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return err
	}
	if cursor == uint64(^uint32(0)) {
		return nil
	}
	uids, err := client.SearchUID(uint32(cursor + 1))
	if err != nil {
		return errors.New("personal inbox UID search failed")
	}
	sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
	count := 0
	for _, uid := range uids {
		if uint64(uid) <= cursor {
			continue
		}
		if count >= 25 {
			break
		}
		raw, err := client.PeekUID(uid)
		oversized := errors.Is(err, imap.ErrMessageTooLarge)
		if err != nil && !oversized {
			return fmt.Errorf("personal inbox fetch failed at UID %d; cursor retained", uid)
		}
		// A vanished message still receives a durable review record.
		hash := sha256.Sum256([]byte(identity + ":" + strconv.FormatUint(uint64(uid), 10)))
		message := Message{ID: hex.EncodeToString(hash[:]), UID: uid, Mail: imap.ParseText(raw), Created: service.Now().Unix()}
		if oversized {
			message.Mail = imap.TextMessage{Subject: "Large email requires review", Body: "Message exceeded the safe fetch limit; inspect original inbox.", Incomplete: true}
		}
		if err := service.Store.saveMessage(ctx, message, nil, strconv.FormatUint(uint64(uid), 10)); err != nil {
			return err
		}
		cursor = uint64(uid)
		count++
		if oversized {
			break
		} // The oversized literal closed the IMAP session.
	}
	return nil
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
		if err := service.Store.saveMessage(ctx, message, nil, ""); err != nil {
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
				if err := service.Store.saveMessage(ctx, message, service.plan(message), ""); err != nil {
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

func (service *Service) plan(message Message) []Effect {
	classification := message.Classification
	body := classification.Category + ": " + message.Mail.Subject + "\nFrom: " + message.Mail.From + "\n" + classification.Summary
	if classification.Action != "" {
		body += "\nAction: " + classification.Action
	}
	if classification.Deadline != "" {
		body += "\nDeadline: " + classification.Deadline
	}
	body += "\nMail reference: " + message.ID
	if len([]rune(body)) > 1900 {
		body = string([]rune(body)[:1800]) + "\nMail reference: " + message.ID
	}
	effects := []Effect{}
	add := func(kind, target string) {
		status := "pending"
		if service.Config.DryRun {
			status = "preview"
		}
		effects = append(effects, Effect{ID: message.ID + ":" + kind, MessageID: message.ID, Kind: kind, Target: target, Body: body, UID: message.UID, Status: status})
	}
	switch classification.Category {
	case "news":
		if service.Config.MarkNewsRead {
			add("read", service.Config.Mailbox)
		}
		if service.Config.MoveNews {
			add("move", service.Config.NewsFolder)
		}
	case "order":
		add("telegram", service.Config.OrdersTarget)
	case "action":
		add("telegram", service.Config.ActionTarget)
		add("linear", service.Config.LinearTeam)
	case "review":
		add("telegram", service.Config.ActionTarget)
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
	for _, effect := range effects {
		if service.Config.DryRun {
			continue
		} // Pausing live effects is immediate.
		// Read must precede move. Never change a UID in the wrong mailbox. A move
		// planned while mark_news_read was off has no read to wait for; ordering
		// it against a sibling that will never exist blocks it forever.
		if effect.Kind == "move" && service.Config.MarkNewsRead {
			var status string
			err := service.Store.db.QueryRowContext(ctx, "SELECT json_extract(data,'$.status') FROM inbox_effects WHERE id=?", effect.MessageID+":read").Scan(&status)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil && status != "done" {
				continue
			}
		}
		// Stop previously planned changes too when the operator disables them.
		if (effect.Kind == "move" && !service.Config.MoveNews) || (effect.Kind == "read" && !service.Config.MarkNewsRead) {
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
		} else {
			effect.Status = "done"
			effect.Receipt = receipt
		}
		// Failure here leaves inflight durable. Startup converts it to uncertain.
		if err := service.Store.saveEffect(ctx, effect); err != nil {
			return err
		}
	}
	return nil
}
