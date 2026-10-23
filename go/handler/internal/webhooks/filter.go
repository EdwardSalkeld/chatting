package webhooks

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

type FilterStore interface {
	WebhookState(context.Context, string, string) (string, error)
	PutWebhookState(context.Context, string, string, string) error
	Seen(context.Context, string, string) (bool, error)
	MarkSeen(context.Context, string, string) error
}

type Decision struct {
	Disposition string `json:"disposition"`
	Reason      string `json:"reason"`
	Watched     bool   `json:"watched"`
	NewWatch    bool   `json:"new_watch,omitempty"`
}

// Filter serializes decisions and state writes within the handler process.
// There is deliberately no broker or dispatcher dependency.
type Filter struct {
	mu                  sync.Mutex
	Store               FilterStore
	Login               string
	SelfLogins          []string
	ReleaseRepositories []string
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
	if stored == "" && !tagged && !ownedPR {
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
		if ownedPR {
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
	if !matched {
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
	key := fmt.Sprintf("%s/%s/%d/%s", strings.ToLower(f.Login), strings.ToLower(p.Repository.FullName), r.WorkflowID, r.HeadSHA)
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
	if old.RunNumber > r.RunNumber || (old.RunNumber == r.RunNumber && old.RunAttempt >= r.RunAttempt && previous != "") {
		return decision("drop", "superseded_workflow"), nil
	}
	encoded, _ := json.Marshal(r)
	if err := f.Store.PutWebhookState(ctx, "workflow", key, string(encoded)); err != nil {
		return Decision{}, err
	}
	if workflowFailed(r.Conclusion) {
		return decision("keep", "workflow_failure"), nil
	}
	if r.Conclusion == "success" && workflowFailed(old.Conclusion) {
		return decision("keep", "workflow_recovered"), nil
	}
	return decision("drop", "routine_workflow_result"), nil
}

func workflowFailed(conclusion string) bool {
	return conclusion == "failure" || conclusion == "timed_out" || conclusion == "action_required" || conclusion == "startup_failure"
}
