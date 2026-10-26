package contracts

import (
	"context"
	"errors"
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
	StageGitHub(context.Context, TaskEnvelope, bool) error
}
