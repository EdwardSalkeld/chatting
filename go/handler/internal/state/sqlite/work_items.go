package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/routing"
)

// AssignTask makes the ingress handler authoritative for lane identity.
func (store *Store) AssignTask(ctx context.Context, task contracts.TaskQueueMessage) (contracts.TaskQueueMessage, error) {
	decision := store.router.Decide(task)
	if decision.Kind == "github_unresolved" {
		return task, fmt.Errorf("github ownership needs_context")
	}
	if len(decision.PRKeys) == 0 && store.resolveRuns != nil {
		for _, run := range decision.RunRefs {
			keys, err := store.resolveRuns(ctx, run)
			if err != nil {
				log.Printf("run_pr_lookup_failed task_id=%s repo=%s run_id=%d err=%v", task.TaskID, run.Repo, run.ID, err)
				continue
			}
			decision.PRKeys = append(decision.PRKeys, keys...)
		}
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return task, err
	}
	defer rollbackUnlessCommitted(tx)
	var itemID string
	var existingReason string
	err = tx.QueryRowContext(ctx, `SELECT work_item_id, route_reason FROM task_assignments WHERE task_id = ?`, task.TaskID).Scan(&itemID, &existingReason)
	if err == nil {
		task.WorkItemID = itemID
		if err = tx.Commit(); err != nil {
			return task, err
		}
		return store.applyPreferredReply(ctx, task, existingReason)
	}
	if err != sql.ErrNoRows {
		return task, err
	}
	reason := decision.Reason
	if decision.Kind == "github_repository" {
		if err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_artifacts WHERE kind = 'github_repository' AND artifact_key = ?`, decision.Key).Scan(&itemID); err != nil {
			return task, fmt.Errorf("github repository ownership needs_context: %w", err)
		}
	}
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
		} else if len(owners) > 1 && task.Envelope.Source == "webhook" {
			return task, fmt.Errorf("github ownership needs_context: conflicting owners")
		}
	}
	if itemID == "" {
		err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_routes WHERE route_kind = ? AND route_key = ?`, decision.Kind, decision.Key).Scan(&itemID)
		if err != nil && err != sql.ErrNoRows {
			return task, err
		}
	}
	if itemID == "" {
		itemID = randomID("item_", 16)
		reply, err := json.Marshal(task.Envelope.ReplyChannel)
		if err != nil {
			return task, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO work_items VALUES (?, ?, ?)`, itemID, string(reply), formatTimestamp(time.Now()))
		if err != nil {
			return task, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO work_item_routes VALUES (?, ?, ?)`, decision.Kind, decision.Key, itemID)
		if err != nil {
			return task, err
		}
	}
	if decision.Kind == "github_subject" {
		// Issues and PRs use the same ownership registry (historically github_pr).
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO work_item_artifacts VALUES ('github_pr', ?, ?)`, decision.Key, itemID); err != nil {
			return task, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO task_assignments VALUES (?, ?, ?)`, task.TaskID, itemID, reason)
	if err != nil {
		return task, err
	}
	if err = tx.Commit(); err != nil {
		return task, err
	}
	task.WorkItemID = itemID
	return store.applyPreferredReply(ctx, task, reason)
}

// RegisterPR links a PR to the lane assigned to the originating task.
func (store *Store) RegisterPR(ctx context.Context, taskID, prURL string) error {
	key, err := routing.NormalizePR(prURL)
	if err != nil {
		return err
	}
	return store.registerSubject(ctx, taskID, key)
}

// RegisterSubject associates a related issue or PR with the originating item.
func (store *Store) RegisterSubject(ctx context.Context, taskID, subjectURL string) error {
	key, err := routing.NormalizeSubject(subjectURL)
	if err != nil {
		return err
	}
	return store.registerSubject(ctx, taskID, key)
}

func (store *Store) registerSubject(ctx context.Context, taskID, key string) error {
	return store.registerArtifact(ctx, taskID, "github_pr", key)
}

func (store *Store) RegisterRepository(ctx context.Context, taskID, repo string) error {
	key, err := routing.NormalizeRepository(repo)
	if err != nil {
		return err
	}
	return store.registerArtifact(ctx, taskID, "github_repository", key)
}

func (store *Store) registerArtifact(ctx context.Context, taskID, kind, key string) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessCommitted(tx)
	var itemID string
	if err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM task_assignments WHERE task_id = ?`, taskID).Scan(&itemID); err != nil {
		return err
	}
	var owner string
	err = tx.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_artifacts WHERE kind = ? AND artifact_key = ?`, kind, key).Scan(&owner)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if owner != "" && owner != itemID {
		return fmt.Errorf("GitHub subject already belongs to another work item")
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO work_item_artifacts VALUES (?, ?, ?)`, kind, key, itemID); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *Store) applyPreferredReply(ctx context.Context, task contracts.TaskQueueMessage, reason string) (contracts.TaskQueueMessage, error) {
	if reason == "github_pr" || reason == "github_subject" || reason == "github_repository" {
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
		task.Envelope.ReplyChannel = reply
	}
	return task, nil
}
