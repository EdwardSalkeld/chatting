package contracts

// GitHubWebhookStatus exposes queue and ownership metadata, never request bodies.
type GitHubWebhookStatus struct {
	HeldCount    int                      `json:"held_count"`
	PendingCount int                      `json:"pending_count"`
	HeldByReason map[string]int           `json:"held_by_reason"`
	Held         []GitHubHeldEventSummary `json:"held"`
	Workflows    []GitHubWorkflowOwner    `json:"workflows"`
	Limit        int                      `json:"limit"`
	Offset       int                      `json:"offset"`
}

type GitHubHeldEventSummary struct {
	EventKey   string `json:"event_key"`
	Event      string `json:"event"`
	Delivery   string `json:"delivery"`
	Reason     string `json:"reason"`
	ReceivedAt string `json:"received_at"`
	BodyBytes  int    `json:"body_bytes"`
	Repository string `json:"repository,omitempty"`
	SubjectURL string `json:"subject_url,omitempty"`
	RunURL     string `json:"run_url,omitempty"`
}

type GitHubWorkflowOwner struct {
	URL        string `json:"url"`
	WorkItemID string `json:"work_item_id"`
}
