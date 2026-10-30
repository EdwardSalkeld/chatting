package webhooks

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type FilterStore interface {
	contracts.WebhookFilterState
}

type Decision struct {
	Disposition string `json:"disposition"`
	Reason      string `json:"reason"`
	Watched     bool   `json:"watched"`
	NewWatch    bool   `json:"new_watch,omitempty"`
	// Routing evidence only: never serialized into filter diagnostics or used
	// to replace the original signed body for task identity/deduplication.
	WorkflowTarget string `json:"-"`
}

// Filter serializes decisions and state writes within the handler process.
// There is deliberately no broker or dispatcher dependency.
type Filter struct {
	mu                  sync.Mutex
	Store               FilterStore
	Login               string
	SelfLogins          []string
	ReleaseRepositories []string
	DeliveryEnabled     bool
	AssignTask          func(context.Context, contracts.TaskQueueMessage) (contracts.TaskQueueMessage, error)
}

type user struct {
	Login string `json:"login"`
}
type subject struct {
	Number      int             `json:"number"`
	User        user            `json:"user"`
	PullRequest json.RawMessage `json:"pull_request"`
	Title       string          `json:"title"`
	Body        string          `json:"body"`
	Draft       bool            `json:"draft"`
	Head        struct {
		SHA string `json:"sha"`
	} `json:"head"`
}
type textObject struct {
	Body string `json:"body"`
}
type workflow struct {
	Event        string    `json:"event"`
	HeadBranch   string    `json:"head_branch"`
	ID           int64     `json:"id"`
	WorkflowID   int64     `json:"workflow_id"`
	RunNumber    int64     `json:"run_number"`
	RunAttempt   int64     `json:"run_attempt"`
	HeadSHA      string    `json:"head_sha"`
	Conclusion   string    `json:"conclusion"`
	PullRequests []subject `json:"pull_requests"`
}
type filterPayload struct {
	Action     string `json:"action"`
	Sender     user   `json:"sender"`
	Assignee   user   `json:"assignee"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Issue   *subject    `json:"issue"`
	PR      *subject    `json:"pull_request"`
	Comment *textObject `json:"comment"`
	Review  *textObject `json:"review"`
	Changes map[string]struct {
		From string `json:"from"`
	} `json:"changes"`
	Run     *workflow `json:"workflow_run"`
	Release *struct {
		Draft      bool `json:"draft"`
		Prerelease bool `json:"prerelease"`
	} `json:"release"`
}
type watchState struct {
	HeadSHA string `json:"head_sha,omitempty"`
}

func decision(disposition, reason string) Decision {
	return Decision{Disposition: disposition, Reason: reason}
}

func (f *Filter) Evaluate(ctx context.Context, body []byte, event, delivery string) (Decision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Store == nil || strings.TrimSpace(f.Login) == "" {
		return decision("needs_context", "filter_not_configured"), nil
	}
	var p filterPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return decision("needs_context", "invalid_event_shape"), nil
	}
	// Hash the event/body as well as recording the delivery ID. Headers are not
	// signed; changing a delivery header must not bypass duplicate suppression.
	key := fmt.Sprintf("%x", sha256.Sum256(append([]byte(event+"\x00"), body...)))
	source := "github_webhook:" + strings.ToLower(f.Login)
	keys := []string{"body:" + key}
	if delivery != "" {
		keys = append(keys, "delivery:"+delivery)
	}
	for _, key := range keys {
		seen, err := f.Store.Seen(ctx, source, key)
		if err != nil {
			return Decision{}, err
		}
		if seen {
			return decision("drop", "duplicate"), nil
		}
	}
	d, err := f.evaluate(ctx, p, event)
	if err != nil {
		return Decision{}, err
	}
	// Missing context can be retried after configuration/context is available.
	if d.Disposition != "needs_context" {
		for _, key := range keys {
			if err := f.Store.MarkSeen(ctx, source, key); err != nil {
				return Decision{}, err
			}
		}
	}
	return d, err
}

func (f *Filter) watchKey(repo string, number int) string {
	return fmt.Sprintf("%s/%s#%d", strings.ToLower(f.Login), strings.ToLower(repo), number)
}

func (f *Filter) isSelf(login string) bool {
	if login == "" {
		return false
	}
	if strings.EqualFold(login, f.Login) {
		return true
	}
	for _, self := range f.SelfLogins {
		if strings.EqualFold(login, self) {
			return true
		}
	}
	return false
}

func (f *Filter) evaluate(ctx context.Context, p filterPayload, event string) (Decision, error) {
	if event == "ping" {
		return decision("drop", "ping"), nil
	}
	if p.Repository.FullName == "" {
		return decision("needs_context", "missing_repository"), nil
	}
	// CI relevance is determined by the watched PR and commit, never the actor
	// who caused the workflow to run (which can legitimately be Billy).
	if event == "workflow_run" {
		return f.workflowDecision(ctx, p)
	}
	if p.Sender.Login == "" {
		return decision("needs_context", "missing_sender"), nil
	}
	self := f.isSelf(p.Sender.Login)
	if event == "release" {
		if self {
			return decision("drop", "self_action"), nil
		}
		for _, repo := range f.ReleaseRepositories {
			if strings.EqualFold(repo, p.Repository.FullName) {
				if p.Release == nil {
					return decision("needs_context", "missing_release"), nil
				}
				if p.Action == "published" && !p.Release.Draft && !p.Release.Prerelease {
					return decision("keep", "published_release"), nil
				}
				return decision("drop", "release_noise"), nil
			}
		}
		return decision("drop", "release_not_opted_in"), nil
	}
	switch event {
	case "issues", "issue_comment", "pull_request", "pull_request_review", "pull_request_review_comment", "pull_request_review_thread":
	default:
		return decision("drop", "unsupported_event"), nil
	}
	s := p.PR
	if s == nil {
		s = p.Issue
	}
	if s == nil || s.Number <= 0 {
		return decision("needs_context", "missing_subject"), nil
	}
	key := f.watchKey(p.Repository.FullName, s.Number)
	stored, err := f.Store.WebhookState(ctx, "watch", key)
	if err != nil {
		return Decision{}, err
	}
	// PR authorship subscribes us even when the opening event is our own.
	// Ordinary issue comments carry PR metadata under issue.pull_request.
	isPR := p.PR != nil || (len(s.PullRequest) > 0 && string(s.PullRequest) != "null")
	ownedPR := isPR && f.isSelf(s.User.Login)
	if self && stored == "" && !ownedPR {
		return decision("drop", "self_action"), nil
	}
	// Only text belonging to this event can tag us in. An old mention copied
	// into a parent issue/PR or thread must not subscribe an unrelated delivery.
	text := ""
	switch event {
	case "issues", "pull_request":
		if p.Action == "opened" || p.Action == "reopened" || p.Action == "edited" {
			text = s.Title + "\n" + s.Body
		}
	case "issue_comment", "pull_request_review_comment":
		if p.Action != "deleted" && p.Comment != nil {
			text = p.Comment.Body
		}
	case "pull_request_review":
		if p.Action == "submitted" || p.Action == "edited" {
			if p.Review != nil {
				text = p.Review.Body
			}
		}
	}
	tagged := mention(text, f.Login)
	// Subscribe only to the assignee changed by this event. An assignment to
	// someone else (or an old assignees list copied into a payload) is not a
	// request for us to start work.
	assignedToUs := (event == "issues" || event == "pull_request") &&
		p.Action == "assigned" && strings.EqualFold(p.Assignee.Login, f.Login)
	if stored == "" && !tagged && !ownedPR && !assignedToUs {
		return decision("drop", "not_tagged_or_watched"), nil
	}
	state := watchState{}
	if stored != "" {
		if err := json.Unmarshal([]byte(stored), &state); err != nil {
			return Decision{}, err
		}
	}
	if s.Head.SHA != "" {
		state.HeadSHA = s.Head.SHA
	}
	encoded, _ := json.Marshal(state)
	if err := f.Store.PutWebhookState(ctx, "watch", key, string(encoded)); err != nil {
		return Decision{}, err
	}
	d := decision("keep", "watched_subject")
	d.Watched, d.NewWatch = true, stored == ""
	if self {
		d.Disposition, d.Reason = "drop", "self_action"
		return d, nil
	}
	if d.NewWatch {
		if assignedToUs {
			d.Reason = "assigned_to_us"
		} else if ownedPR {
			d.Reason = "own_pr"
		} else {
			d.Reason = "tagged_in"
		}
		return d, nil
	}
	d.Disposition, d.Reason = actionDecision(p, event, s)
	return d, nil
}

func mention(text, login string) bool {
	pattern := `(?i)(^|[^a-z0-9_@/.-])@` + regexp.QuoteMeta(login) + `($|[^a-z0-9_-])`
	return regexp.MustCompile(pattern).MatchString(text)
}

func normalized(text string) string { return strings.Join(strings.Fields(text), " ") }

func meaningfulEdit(p filterPayload, current map[string]string) bool {
	for field, value := range current {
		if change, ok := p.Changes[field]; ok && normalized(change.From) != normalized(value) {
			return true
		}
	}
	return false
}

func actionDecision(p filterPayload, event string, s *subject) (string, string) {
	if (event == "issue_comment" || event == "pull_request_review_comment") && p.Action != "deleted" {
		if p.Comment == nil {
			return "needs_context", "missing_comment"
		}
		if normalized(p.Comment.Body) == "" {
			return "drop", "empty_comment"
		}
	}
	if event == "pull_request_review" && p.Review == nil {
		return "needs_context", "missing_review"
	}
	if p.Action == "edited" {
		current := map[string]string{"title": s.Title, "body": s.Body}
		if p.Comment != nil {
			current = map[string]string{"body": p.Comment.Body}
		}
		if p.Review != nil {
			current = map[string]string{"body": p.Review.Body}
		}
		if len(p.Changes) == 0 {
			return "needs_context", "missing_edit_history"
		}
		if meaningfulEdit(p, current) {
			return "keep", "meaningful_edit"
		}
		return "drop", "cosmetic_edit"
	}
	allowed := map[string]string{
		"issues":                      " opened reopened closed assigned unassigned ",
		"issue_comment":               " created ",
		"pull_request":                " opened reopened closed synchronize ready_for_review converted_to_draft assigned unassigned review_requested review_request_removed ",
		"pull_request_review":         " submitted dismissed ",
		"pull_request_review_comment": " created ",
		"pull_request_review_thread":  " resolved unresolved ",
	}
	if p.Action != "" && strings.Contains(allowed[event], " "+p.Action+" ") {
		if event == "pull_request" && s.Draft {
			return "keep", "draft_tracking_only"
		}
		return "keep", "watched_subject"
	}
	return "drop", "action_noise"
}

func (f *Filter) workflowDecision(ctx context.Context, p filterPayload) (Decision, error) {
	r := p.Run
	if r == nil {
		return decision("needs_context", "missing_workflow_run"), nil
	}
	if p.Action != "completed" {
		return decision("drop", "workflow_progress"), nil
	}
	destination, err := f.Store.GitHubWorkflowDestination(ctx, p.Repository.FullName, r.ID, r.WorkflowID)
	if err != nil {
		return Decision{}, err
	}
	explicitRun := strings.Contains(destination, "/actions/runs/")
	prExecution := r.Event == "pull_request" || r.Event == "pull_request_target"
	workflowOwned := strings.Contains(destination, "/actions/workflows/") && !prExecution
	if len(r.PullRequests) == 0 && !explicitRun && !workflowOwned && prExecution {
		numbers, err := f.Store.WatchedGitHubPRs(ctx, f.Login, p.Repository.FullName, r.HeadSHA)
		if err != nil {
			return Decision{}, err
		}
		if len(numbers) > 1 {
			return decision("needs_context", "workflow_ambiguous_commit"), nil
		}
		if len(numbers) == 1 {
			r.PullRequests = []subject{{Number: numbers[0]}}
		}
	}
	if len(r.PullRequests) > 1 && !explicitRun && !workflowOwned {
		return decision("needs_context", "workflow_ambiguous_subject"), nil
	}
	matched := false
	missingHead := false
	for _, pr := range r.PullRequests {
		stored, err := f.Store.WebhookState(ctx, "watch", f.watchKey(p.Repository.FullName, pr.Number))
		if err != nil {
			return Decision{}, err
		}
		if stored == "" {
			continue
		}
		var watch watchState
		if err := json.Unmarshal([]byte(stored), &watch); err != nil {
			return Decision{}, err
		}
		head := watch.HeadSHA
		if head == "" {
			head = pr.Head.SHA
		}
		if head == "" {
			missingHead = true
			continue
		}
		if head != r.HeadSHA {
			continue
		}
		matched = true
	}
	if !matched && !explicitRun && !workflowOwned {
		if missingHead {
			return decision("needs_context", "workflow_missing_head_context"), nil
		}
		if len(r.PullRequests) == 0 {
			return decision("needs_context", "workflow_missing_pr_context"), nil
		}
		return decision("drop", "workflow_unwatched_or_stale_commit"), nil
	}
	if r.WorkflowID == 0 || r.HeadSHA == "" || r.Conclusion == "" {
		return decision("needs_context", "incomplete_workflow"), nil
	}
	// PR health is commit-specific; unattended workflow health spans commits
	// on the same branch. Run ownership never changes this ordering history.
	key := contracts.GitHubWorkflowHistoryKey(f.Login, p.Repository.FullName, r.WorkflowID, r.HeadSHA, r.Event, r.HeadBranch)
	previous, err := f.Store.WebhookState(ctx, "workflow", key)
	if err != nil {
		return Decision{}, err
	}
	var old workflow
	if previous != "" {
		if err := json.Unmarshal([]byte(previous), &old); err != nil {
			return Decision{}, err
		}
	}
	sharedAhead := previous != "" && newerWorkflow(&old, r)
	if !explicitRun && sharedAhead {
		return decision("drop", "superseded_workflow"), nil
	}
	encoded, _ := json.Marshal(r)
	if explicitRun {
		runKey := fmt.Sprintf("%s/%s/run/%d", strings.ToLower(f.Login), strings.ToLower(p.Repository.FullName), r.ID)
		seen, err := f.Store.WebhookState(ctx, "workflow", runKey)
		if err != nil {
			return Decision{}, err
		}
		var oldRun workflow
		if seen != "" {
			if err := json.Unmarshal([]byte(seen), &oldRun); err != nil {
				return Decision{}, err
			}
			if !newerWorkflow(r, &oldRun) {
				return decision("drop", "superseded_workflow"), nil
			}
		}
		if err := f.Store.PutWebhookState(ctx, "workflow", runKey, string(encoded)); err != nil {
			return Decision{}, err
		}
	}
	if !sharedAhead {
		if err := f.Store.PutWebhookState(ctx, "workflow", key, string(encoded)); err != nil {
			return Decision{}, err
		}
	}
	// Prefer an explicitly registered run, then a single watched PR, then an
	// explicit workflow destination for runs without a PR. Never infer an
	// owner from branch names or display titles.
	if !explicitRun && !workflowOwned && len(r.PullRequests) == 1 {
		destination = fmt.Sprintf("https://github.com/%s/pull/%d", p.Repository.FullName, r.PullRequests[0].Number)
	}
	keep := func(reason string) Decision {
		d := decision("keep", reason)
		d.WorkflowTarget = destination
		return d
	}
	if explicitRun {
		return keep("registered_run_completed"), nil
	}
	if workflowFailed(r.Conclusion) {
		return keep("workflow_failure"), nil
	}
	if r.Conclusion == "success" && workflowFailed(old.Conclusion) {
		return keep("workflow_recovered"), nil
	}
	return decision("drop", "routine_workflow_result"), nil
}

func newerWorkflow(current, previous *workflow) bool {
	return current.RunNumber > previous.RunNumber || (current.RunNumber == previous.RunNumber && current.RunAttempt > previous.RunAttempt)
}

func workflowFailed(conclusion string) bool {
	return conclusion == "failure" || conclusion == "timed_out" || conclusion == "action_required" || conclusion == "startup_failure"
}
