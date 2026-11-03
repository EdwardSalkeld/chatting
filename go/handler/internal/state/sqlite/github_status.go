package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func (store *Store) GitHubWebhookStatus(ctx context.Context, limit, offset int) (contracts.GitHubWebhookStatus, error) {
	status := contracts.GitHubWebhookStatus{Limit: limit, Offset: offset,
		Held: []contracts.GitHubHeldEventSummary{}, Workflows: []contracts.GitHubWorkflowOwner{}, HeldByReason: map[string]int{}}
	if limit < 1 || limit > 100 || offset < 0 || offset > 1000000 {
		return status, errors.New("invalid webhook status pagination")
	}
	// One read transaction keeps totals, reasons and the page consistent.
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return status, err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM github_webhook_pending`).Scan(&status.PendingCount); err != nil {
		return status, err
	}
	reasons, err := tx.QueryContext(ctx, `SELECT reason,count(*) FROM github_webhook_held GROUP BY reason`)
	if err != nil {
		return status, err
	}
	for reasons.Next() {
		var reason string
		var count int
		if err := reasons.Scan(&reason, &count); err != nil {
			reasons.Close()
			return status, err
		}
		status.HeldByReason[reason] = count
		status.HeldCount += count
	}
	err = reasons.Err()
	reasons.Close()
	if err != nil {
		return status, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT event_key,event,delivery,reason,received_at,body FROM github_webhook_held ORDER BY received_at,event_key LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return status, err
	}
	for rows.Next() {
		var item contracts.GitHubHeldEventSummary
		var body []byte
		if err := rows.Scan(&item.EventKey, &item.Event, &item.Delivery, &item.Reason, &item.ReceivedAt, &body); err != nil {
			rows.Close()
			return status, err
		}
		item.BodyBytes = len(body)
		var metadata struct {
			Repository struct {
				FullName string `json:"full_name"`
			} `json:"repository"`
			Issue struct {
				URL string `json:"html_url"`
			} `json:"issue"`
			PR struct {
				URL string `json:"html_url"`
			} `json:"pull_request"`
			Run struct {
				URL string `json:"html_url"`
			} `json:"workflow_run"`
		}
		// Malformed event shapes can be held too; keep their reason and identity
		// inspectable even when metadata cannot be extracted.
		if json.Unmarshal(body, &metadata) == nil {
			item.Repository, item.RunURL = metadata.Repository.FullName, metadata.Run.URL
			item.SubjectURL = metadata.PR.URL
			if item.SubjectURL == "" {
				item.SubjectURL = metadata.Issue.URL
			}
		}
		status.Held = append(status.Held, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return status, err
	}
	owners, err := tx.QueryContext(ctx, `SELECT artifact_key,work_item_id FROM work_item_artifacts WHERE kind='github_workflow' ORDER BY artifact_key`)
	if err != nil {
		return status, err
	}
	for owners.Next() {
		var owner contracts.GitHubWorkflowOwner
		if err := owners.Scan(&owner.URL, &owner.WorkItemID); err != nil {
			owners.Close()
			return status, err
		}
		status.Workflows = append(status.Workflows, owner)
	}
	err = owners.Err()
	owners.Close()
	if err != nil {
		return status, err
	}
	return status, tx.Commit()
}
