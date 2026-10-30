package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/routing"
)

func (s webhookTx) ObserveGitHubRun(ctx context.Context, login, value string, attempt int64, envelope contracts.TaskEnvelope) error {
	key, err := routing.NormalizeRun(value)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = s.tx.ExecContext(ctx, `INSERT INTO github_webhook_observed_runs VALUES (?, ?, ?)
		ON CONFLICT(run_url) DO UPDATE SET attempt=excluded.attempt, envelope_json=excluded.envelope_json
		WHERE excluded.attempt > github_webhook_observed_runs.attempt`, key, attempt, string(encoded))
	if err != nil {
		return err
	}
	m := envelope.ReplyChannel.Metadata
	state := map[string]any{"id": m["run_id"], "workflow_id": m["workflow_id"], "run_number": m["run_number"], "run_attempt": m["run_attempt"], "head_sha": m["head_sha"], "event": m["run_event"], "head_branch": m["head_branch"], "conclusion": m["conclusion"]}
	encoded, err = json.Marshal(state)
	if err != nil {
		return err
	}
	var fields struct {
		WorkflowID int64  `json:"workflow_id"`
		HeadSHA    string `json:"head_sha"`
		Event      string `json:"event"`
		HeadBranch string `json:"head_branch"`
	}
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return err
	}
	workflowKey := contracts.GitHubWorkflowHistoryKey(login, m["repository"].(string), fields.WorkflowID, fields.HeadSHA, fields.Event, fields.HeadBranch)
	_, err = s.tx.ExecContext(ctx, `INSERT INTO github_webhook_workflows VALUES (?,?)
		ON CONFLICT(workflow_key) DO UPDATE SET state=excluded.state
		WHERE json_extract(excluded.state,'$.run_number') > json_extract(github_webhook_workflows.state,'$.run_number')
		OR (json_extract(excluded.state,'$.run_number') = json_extract(github_webhook_workflows.state,'$.run_number')
		AND json_extract(excluded.state,'$.run_attempt') > json_extract(github_webhook_workflows.state,'$.run_attempt'))`, workflowKey, string(encoded))
	return err
}

// Receipt dedupe is separate from delivery of a run attempt to its owner.
func prepareRunCompletion(envelope *contracts.TaskEnvelope, runURL, owner string) error {
	canonical, err := routing.NormalizeRun(runURL)
	if err != nil {
		return err
	}
	identity, _ := json.Marshal([]any{canonical, envelope.ReplyChannel.Metadata["run_attempt"], owner})
	envelope.ID = fmt.Sprintf("github-run-completion:sha256:%x", sha256.Sum256(identity))
	envelope.DedupeKey = envelope.ID
	return nil
}

func stageRunCompletion(ctx context.Context, tx *sql.Tx, envelope *contracts.TaskEnvelope) error {
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO idempotency_keys VALUES ('github_run_completion', ?, ?)`, envelope.ID, formatTimestamp(time.Now()))
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO github_webhook_pending VALUES (?, ?, ?)`, envelope.ID, string(encoded), formatTimestamp(time.Now()))
	return err
}

func reconcileRunCompletion(ctx context.Context, tx *sql.Tx, runURL, owner string) error {
	var encoded string
	err := tx.QueryRowContext(ctx, `SELECT envelope_json FROM github_webhook_observed_runs WHERE run_url=?`, runURL).Scan(&encoded)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	var envelope contracts.TaskEnvelope
	if err := json.Unmarshal([]byte(encoded), &envelope); err != nil {
		return err
	}
	eventKey := strings.TrimPrefix(envelope.ID, "github-webhook:sha256:")
	if err := prepareRunCompletion(&envelope, runURL, owner); err != nil {
		return err
	}
	if err := stageRunCompletion(ctx, tx, &envelope); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM github_webhook_held WHERE event_key=?`, eventKey)
	return err
}
