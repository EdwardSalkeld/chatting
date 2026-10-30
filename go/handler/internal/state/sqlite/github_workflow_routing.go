package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type workflowQueries interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func watchedGitHubPRs(ctx context.Context, q workflowQueries, login, repo, sha string) ([]int, error) {
	if sha == "" {
		return nil, nil
	}
	prefix := strings.ToLower(login+"/"+repo) + "#"
	rows, err := q.QueryContext(ctx, `SELECT watch_key FROM github_webhook_watches
		WHERE substr(watch_key, 1, length(?)) = ? AND json_extract(state, '$.head_sha') = ? ORDER BY watch_key`, prefix, prefix, sha)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []int
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		number, err := strconv.Atoi(strings.TrimPrefix(key, prefix))
		if err != nil || number <= 0 {
			return nil, fmt.Errorf("invalid stored PR watch")
		}
		result = append(result, number)
	}
	return result, rows.Err()
}

func workflowDestination(ctx context.Context, q workflowQueries, repo string, run, workflow int64) (string, error) {
	for _, candidate := range []struct {
		kind, path string
		id         int64
	}{
		{"github_run", "runs", run}, {"github_workflow", "workflows", workflow},
	} {
		if candidate.id <= 0 {
			continue
		}
		key := fmt.Sprintf("https://github.com/%s/actions/%s/%d", strings.ToLower(repo), candidate.path, candidate.id)
		var owner string
		err := q.QueryRowContext(ctx, `SELECT work_item_id FROM work_item_artifacts WHERE kind = ? AND artifact_key = ?`, candidate.kind, key).Scan(&owner)
		if err == nil {
			return key, nil
		}
		if err != sql.ErrNoRows {
			return "", err
		}
	}
	return "", nil
}

func (store *Store) WatchedGitHubPRs(ctx context.Context, login, repo, sha string) ([]int, error) {
	return watchedGitHubPRs(ctx, store.db, login, repo, sha)
}
func (s webhookTx) WatchedGitHubPRs(ctx context.Context, login, repo, sha string) ([]int, error) {
	return watchedGitHubPRs(ctx, s.tx, login, repo, sha)
}
func (store *Store) GitHubWorkflowDestination(ctx context.Context, repo string, run, workflow int64) (string, error) {
	return workflowDestination(ctx, store.db, repo, run, workflow)
}
func (s webhookTx) GitHubWorkflowDestination(ctx context.Context, repo string, run, workflow int64) (string, error) {
	return workflowDestination(ctx, s.tx, repo, run, workflow)
}
