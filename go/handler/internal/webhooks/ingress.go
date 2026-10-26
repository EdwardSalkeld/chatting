package webhooks

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

// AtomicReceiver commits filtering state and the durable pending notification
// together. A receiver without this capability remains logging-only.
type AtomicReceiver interface {
	WebhookTransaction(context.Context, func(contracts.WebhookTransaction) error) error
}

type HeldStore interface {
	HoldGitHub(context.Context, string, string, string, []byte, string, time.Time) error
	ClearHeldGitHub(context.Context, string) error
}

func ReceiveGitHub(ctx context.Context, receiver AtomicReceiver, filter *Filter, body []byte, event, delivery string, now time.Time) (Decision, *contracts.TaskQueueMessage, error) {
	var d Decision
	var notification *contracts.TaskQueueMessage
	var buildErr error
	err := receiver.WebhookTransaction(ctx, func(tx contracts.WebhookTransaction) error {
		var observed *contracts.TaskEnvelope
		var observedURL string
		var observedAttempt int64
		// Observing an outcome is independent of whether a PR needs a notice.
		if event == "workflow_run" {
			var p filterPayload
			if json.Unmarshal(body, &p) == nil && p.Action == "completed" && p.Run != nil && p.Run.ID > 0 && p.Run.RunAttempt > 0 && p.Run.WorkflowID > 0 && p.Run.HeadSHA != "" && p.Run.RunNumber > 0 && p.Run.Conclusion != "" {
				runURL := fmt.Sprintf("https://github.com/%s/actions/runs/%d", p.Repository.FullName, p.Run.ID)
				task, err := GitHubNotification(body, event, delivery, now, runURL)
				if err == nil {
					observed, observedURL, observedAttempt = &task.Envelope, runURL, p.Run.RunAttempt
				}
			}
		}
		local := &Filter{Store: tx, Login: filter.Login, SelfLogins: filter.SelfLogins, ReleaseRepositories: filter.ReleaseRepositories}
		var err error
		d, err = local.Evaluate(ctx, body, event, delivery)
		if err != nil {
			return err
		}
		if d.Disposition == "keep" {
			task, err := GitHubNotification(body, event, delivery, now, d.WorkflowTarget)
			if err != nil {
				buildErr = err
				return err
			}
			err = tx.StageGitHub(ctx, &task.Envelope, filter.DeliveryEnabled)
			if err != nil {
				return err
			}
			task.TaskID = "task:" + task.Envelope.ID
			notification = &task
		}
		// Advance observations after filtering has read the previous outcome.
		// Holds/drops still contribute to shared ordering and late registration.
		if observed != nil {
			return tx.ObserveGitHubRun(ctx, filter.Login, observedURL, observedAttempt, *observed)
		}
		return nil
	})
	if buildErr != nil {
		d = decision("needs_context", buildErr.Error())
		err = nil
	}
	if errors.Is(err, contracts.ErrGitHubNeedsContext) {
		d = decision("needs_context", err.Error())
		err = nil
	}
	if held, ok := receiver.(HeldStore); ok && err == nil {
		key := fmt.Sprintf("%x", sha256.Sum256(append([]byte(event+"\x00"), body...)))
		if d.Disposition == "needs_context" {
			err = held.HoldGitHub(ctx, key, event, delivery, body, d.Reason, now)
		} else {
			err = held.ClearHeldGitHub(ctx, key)
		}
	}
	return d, notification, err
}

type PendingStore interface {
	PendingGitHub(context.Context) ([]contracts.TaskEnvelope, error)
	AckGitHub(context.Context, string) error
}

// Connector reuses the normal ingress assignment, ledger and broker path.
// Publication failures leave notifications pending for retry after restart.
type Connector struct{ Store PendingStore }

func (c Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	return c.Store.PendingGitHub(ctx)
}

func (c Connector) AckEnvelope(ctx context.Context, id string) error {
	return c.Store.AckGitHub(ctx, id)
}
