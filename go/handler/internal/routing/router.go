package routing

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

// Decision contains stable route evidence; persistence belongs to the caller.
type Decision struct {
	Kind   string
	Key    string
	Reason string
	PRKeys []string
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
	if envelope.Source == "im" && reply.Type == "telegram" {
		var topic any
		if reply.Metadata != nil {
			topic = reply.Metadata["message_thread_id"]
		}
		key, _ := json.Marshal(map[string]any{"chat": reply.Target, "topic": topic})
		return Decision{Kind: "telegram", Key: string(key), Reason: "telegram_channel"}
	}
	decision := Decision{Kind: "general", Key: "default", Reason: "general_lane"}
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
