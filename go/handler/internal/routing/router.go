package routing

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/zulip"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

// Decision contains stable route evidence; persistence belongs to the caller.
type Decision struct {
	Kind   string
	Key    string
	Reason string
	PRKeys []string
	// RunRefs are workflow runs linked from a GitHub notification. CI failure
	// emails link only the run, so the caller resolves these to PRs.
	RunRefs []RunRef
}

// RunRef identifies a GitHub Actions workflow run.
type RunRef struct {
	Repo string
	ID   int64
}

// Router can be replaced when lane selection becomes more sophisticated.
type Router interface {
	Decide(contracts.TaskQueueMessage) Decision
}

type PersistentLaneRouter struct{}

// NormalizePR accepts a GitHub pull request URL and returns its routing key.
func NormalizePR(value string) (string, error) {
	return prKey(value, false)
}

// Issues and PRs share GitHub's repository number namespace.
func NormalizeSubject(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", err
	}
	parts := strings.Split(parsed.Path, "/")
	if len(parts) >= 4 && parts[3] == "issues" {
		parts[3] = "pull"
		parsed.Path = strings.Join(parts, "/")
	}
	return prKey(parsed.String(), false)
}

func NormalizeRepository(value string) (string, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || !validPRName(parts[0]) || !validPRName(parts[1]) {
		return "", fmt.Errorf("invalid GitHub repository")
	}
	return strings.ToLower(value), nil
}

func prKey(value string, allowPathSuffix bool) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, "github.com") || parsed.User != nil {
		return "", fmt.Errorf("invalid GitHub PR URL")
	}
	parts := strings.Split(parsed.Path, "/")
	if len(parts) < 5 || parts[0] != "" || !validPRName(parts[1]) || !validPRName(parts[2]) || parts[3] != "pull" || !validPRNumber(parts[4]) ||
		(!allowPathSuffix && len(parts) != 5 && !(len(parts) == 6 && parts[5] == "")) {
		return "", fmt.Errorf("invalid GitHub PR URL")
	}
	number, err := strconv.Atoi(parts[4])
	if err != nil {
		return "", fmt.Errorf("invalid GitHub PR URL: %w", err)
	}
	return fmt.Sprintf("%s/%s#%d", strings.ToLower(parts[1]), strings.ToLower(parts[2]), number), nil
}

func validPRName(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.') {
			return false
		}
	}
	return true
}

func validPRNumber(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func (PersistentLaneRouter) Decide(task contracts.TaskQueueMessage) Decision {
	envelope := task.Envelope
	reply := envelope.ReplyChannel
	if reply.Type == "zulip" {
		// Canonicalize the address so field order and topic case do not split
		// conversations. Server identity is part of the address, including schedules.
		if target, err := zulip.ParseTarget(reply.Target); err == nil {
			target.Topic = strings.ToLower(target.Topic)
			return Decision{Kind: "zulip", Key: target.String(), Reason: "zulip_conversation"}
		}
	}
	if envelope.Source == "webhook" {
		if repo, ok := reply.Metadata["github_repository"].(string); ok {
			key, err := NormalizeRepository(repo)
			if err == nil {
				return Decision{Kind: "github_repository", Key: key, Reason: "github_repository"}
			}
		}
		if subject, ok := reply.Metadata["github_subject"].(string); ok {
			key, err := NormalizeSubject(subject)
			if err == nil {
				return Decision{Kind: "github_subject", Key: key, Reason: "github_subject", PRKeys: []string{key}}
			}
		}
		// Missing or ambiguous ownership must never fall into General.
		return Decision{Kind: "github_unresolved", Reason: "needs_context"}
	}
	// Reminders and schedules that answer in a chat belong to that chat's work,
	// so they share its session. Notices for chats not yet allowed do not.
	if reply.Type == "telegram" && envelope.Source != "internal" {
		var topic any
		if reply.Metadata != nil {
			topic = reply.Metadata["message_thread_id"]
		}
		key, _ := json.Marshal(map[string]any{"chat": reply.Target, "topic": topic})
		return Decision{Kind: "telegram", Key: string(key), Reason: "telegram_channel"}
	}
	// Unrelated work is grouped by where it replies, so one sender, schedule or
	// GitHub issue never shares a session with another.
	route, _ := json.Marshal(map[string]any{
		"type":   reply.Type,
		"target": reply.Target,
		"thread": reply.Metadata["message_thread_id"],
	})
	decision := Decision{Kind: "general", Key: string(route), Reason: "general_lane"}
	actor := ""
	if envelope.Actor != nil {
		actor = strings.ToLower(*envelope.Actor)
	}
	if reply.Type != "github" && !(envelope.Source == "email" &&
		(actor == "notifications@github.com" || actor == "noreply@github.com")) {
		return decision
	}
	seen := map[string]bool{}
	content := envelope.Content
	if reply.Type == "github" {
		content += " " + reply.Target
	}
	for _, token := range strings.Fields(content) {
		index := strings.Index(token, "https://")
		if index < 0 || (index > 0 && !strings.ContainsRune("(<[\"'", rune(token[index-1]))) {
			continue
		}
		candidate := strings.TrimRight(token[index:], "),.>;!?\"'")
		if run, ok := runRef(candidate); ok {
			if !seen[candidate] {
				seen[candidate] = true
				decision.RunRefs = append(decision.RunRefs, run)
			}
			continue
		}
		key, err := prKey(candidate, true)
		if err != nil {
			continue
		}
		if !seen[key] {
			seen[key] = true
			decision.PRKeys = append(decision.PRKeys, key)
		}
	}
	return decision
}

func runRef(value string) (RunRef, bool) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, "github.com") || parsed.User != nil {
		return RunRef{}, false
	}
	parts := strings.Split(parsed.Path, "/")
	if len(parts) < 6 || parts[0] != "" || !validPRName(parts[1]) || !validPRName(parts[2]) ||
		parts[3] != "actions" || parts[4] != "runs" || !validPRNumber(parts[5]) {
		return RunRef{}, false
	}
	id, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return RunRef{}, false
	}
	return RunRef{Repo: strings.ToLower(parts[1] + "/" + parts[2]), ID: id}, true
}
