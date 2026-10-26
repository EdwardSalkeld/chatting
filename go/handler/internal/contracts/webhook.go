package contracts

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrGitHubNeedsContext = errors.New("github ownership needs_context")

// WebhookFilterState is the storage boundary for atomic receipt of an event.
type WebhookFilterState interface {
	WebhookState(context.Context, string, string) (string, error)
	PutWebhookState(context.Context, string, string, string) error
	Seen(context.Context, string, string) (bool, error)
	MarkSeen(context.Context, string, string) error
	WatchedGitHubPRs(context.Context, string, string, string) ([]int, error)
	GitHubWorkflowDestination(context.Context, string, int64, int64) (string, error)
}

type WebhookTransaction interface {
	WebhookFilterState
	ObserveGitHubRun(context.Context, string, string, int64, TaskEnvelope) error
	StageGitHub(context.Context, *TaskEnvelope, bool) error
}

func GitHubWorkflowHistoryKey(login, repo string, id int64, sha, event, branch string) string {
	scope := sha
	if event != "" && event != "pull_request" && event != "pull_request_target" {
		scope = "branch/" + branch
	}
	return fmt.Sprintf("%s/%s/%d/%s", strings.ToLower(login), strings.ToLower(repo), id, scope)
}
