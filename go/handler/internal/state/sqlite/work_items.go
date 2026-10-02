package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

// AssignTask makes the ingress handler authoritative for lane identity.
func (store *Store) AssignTask(ctx context.Context, task contracts.TaskQueueMessage) (contracts.TaskQueueMessage, error) {
	decision := store.router.Decide(task)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return task, err
	}
	defer rollbackUnlessCommitted(tx)
	var itemID, workspaceID string
	var existingReason string
	err = tx.QueryRowContext(ctx, `SELECT work_item_id, workspace_id, route_reason FROM task_assignments WHERE task_id = ?`, task.TaskID).Scan(&itemID, &workspaceID, &existingReason)
	if err == nil {
		task.WorkItemID, task.WorkspaceID = itemID, workspaceID
		if err = tx.Commit(); err != nil {
			return task, err
		}
		return store.applyPreferredReply(ctx, task, existingReason)
	}
	if err != sql.ErrNoRows {
		return task, err
	}
	reason := decision.Reason
	if len(decision.PRKeys) > 0 {
		owners := map[string]bool{}
		for _, key := range decision.PRKeys {
			var owner string
			err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_artifacts WHERE kind = 'github_pr' AND artifact_key = ?`, key).Scan(&owner)
			if err == nil {
				owners[owner] = true
			} else if err != sql.ErrNoRows {
				return task, err
			}
		}
		if len(owners) == 1 {
			for owner := range owners {
				itemID = owner
			}
			reason = "github_pr"
		}
	}
	if itemID == "" {
		err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_routes WHERE route_kind = ? AND route_key = ?`, decision.Kind, decision.Key).Scan(&itemID)
		if err != nil && err != sql.ErrNoRows {
			return task, err
		}
	}
	if itemID == "" {
		itemID, err = newWorkID("item_")
		if err != nil {
			return task, err
		}
		workspaceID, err = newWorkID("ws_")
		if err != nil {
			return task, err
		}
		reply, err := json.Marshal(task.Envelope.ReplyChannel)
		if err != nil {
			return task, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO work_items VALUES (?, ?, ?, ?)`, itemID, workspaceID, string(reply), formatTimestamp(time.Now()))
		if err != nil {
			return task, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO work_item_routes VALUES (?, ?, ?)`, decision.Kind, decision.Key, itemID)
		if err != nil {
			return task, err
		}
	} else {
		err = tx.QueryRowContext(ctx, `SELECT workspace_id FROM work_items WHERE work_item_id = ?`, itemID).Scan(&workspaceID)
		if err != nil {
			return task, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO task_assignments VALUES (?, ?, ?, ?)`, task.TaskID, itemID, workspaceID, reason)
	if err != nil {
		return task, err
	}
	if err = tx.Commit(); err != nil {
		return task, err
	}
	task.WorkItemID, task.WorkspaceID = itemID, workspaceID
	return store.applyPreferredReply(ctx, task, reason)
}

func (store *Store) applyPreferredReply(ctx context.Context, task contracts.TaskQueueMessage, reason string) (contracts.TaskQueueMessage, error) {
	if reason == "github_pr" {
		var encoded string
		if err := store.db.QueryRowContext(ctx,
			`SELECT preferred_reply_json FROM work_items WHERE work_item_id = ?`, task.WorkItemID,
		).Scan(&encoded); err != nil {
			return task, err
		}
		var reply contracts.ReplyChannel
		if err := json.Unmarshal([]byte(encoded), &reply); err != nil {
			return task, err
		}
		if reply.Type == "telegram" {
			task.Envelope.ReplyChannel = reply
		}
	}
	return task, nil
}

func newWorkID(prefix string) (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate work ID: %w", err)
	}
	return prefix + hex.EncodeToString(buffer), nil
}
