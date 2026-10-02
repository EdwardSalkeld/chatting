package routing

import (
	"encoding/json"
	"fmt"
	"regexp"
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

var prURL = regexp.MustCompile(`(?i)https://github\.com/([\w.-]+/[\w.-]+)/pull/(\d+)(?:\b|/)`)

// NormalizePR accepts a GitHub pull request URL and returns its routing key.
func NormalizePR(value string) (string, error) {
	match := prURL.FindStringSubmatch(value)
	if match == nil {
		return "", fmt.Errorf("invalid GitHub PR URL")
	}
	number, err := strconv.Atoi(match[2])
	if err != nil {
		return "", fmt.Errorf("invalid GitHub PR URL: %w", err)
	}
	return fmt.Sprintf("%s#%d", strings.ToLower(match[1]), number), nil
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
	for _, match := range prURL.FindAllStringSubmatch(content, -1) {
		number, err := strconv.Atoi(match[2])
		if err != nil {
			continue
		}
		key := fmt.Sprintf("%s#%d", strings.ToLower(match[1]), number)
		if !seen[key] {
			seen[key] = true
			decision.PRKeys = append(decision.PRKeys, key)
		}
	}
	return decision
}
