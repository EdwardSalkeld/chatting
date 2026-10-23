package routing

import (
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestNormalizePRRejectsEmbeddedGitHubURL(t *testing.T) {
	for _, value := range []string{
		"https://evil.example/https://github.com/owner/repo/pull/50",
		"https://github.com.evil.example/owner/repo/pull/50",
		"https://github.com/owner/repo/pull/50.evil",
		"https://github.com/owner/repo/pull/50@evil.example",
		"https://github.com/owner/repo/pull/50/checks",
	} {
		if _, err := NormalizePR(value); err == nil {
			t.Errorf("NormalizePR(%q) accepted an invalid URL", value)
		}
	}
	key, err := NormalizePR("https://github.com/Owner/Repo/pull/50#discussion")
	if err != nil || key != "owner/repo#50" {
		t.Fatalf("NormalizePR(valid URL) = %q, %v", key, err)
	}
	key, err = prKey("https://github.com/Owner/Repo/pull/50/checks", true)
	if err != nil || key != "owner/repo#50" {
		t.Fatalf("prKey(valid notification URL) = %q, %v", key, err)
	}
	for _, value := range []string{
		"https://evil.example/https://github.com/owner/repo/pull/50/checks",
		"https://github.com.evil.example/owner/repo/pull/50/checks",
		"https://github.com/owner/repo/pull/50.evil/checks",
	} {
		if _, err := prKey(value, true); err == nil {
			t.Errorf("prKey(%q, true) accepted an invalid URL", value)
		}
	}
}

func TestDecideCollectsWorkflowRunsOnlyFromGitHubEmail(t *testing.T) {
	content := "View results: https://github.com/Owner/Repo/actions/runs/38644659620\n" +
		"Manage: https://github.com/settings/notifications"
	task := contracts.TaskQueueMessage{Envelope: contracts.TaskEnvelope{
		Source:       "email",
		Content:      content,
		ReplyChannel: contracts.ReplyChannel{Type: "email", Target: "bot@example.com"},
	}}
	actor := "notifications@github.com"
	task.Envelope.Actor = &actor
	decision := PersistentLaneRouter{}.Decide(task)
	if len(decision.PRKeys) != 0 ||
		len(decision.RunRefs) != 1 || decision.RunRefs[0] != (RunRef{Repo: "owner/repo", ID: 38644659620}) {
		t.Fatalf("decision = %#v", decision)
	}

	stranger := "someone@example.com"
	task.Envelope.Actor = &stranger
	if decision := (PersistentLaneRouter{}).Decide(task); len(decision.RunRefs) != 0 {
		t.Fatalf("run links from other senders must be ignored: %#v", decision)
	}
}
