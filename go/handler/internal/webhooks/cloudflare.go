package webhooks

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/net/html"
)

type previewState struct {
	Commit    string    `json:"commit"`
	Outcome   string    `json:"outcome,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

var previewCommit = regexp.MustCompile(`^[a-fA-F0-9]{7,40}$`)

// Only recognise the provider's known deployment table. Human comments and
// unrecognised provider messages retain the ordinary watch/edit rules.
func cloudflarePreview(body string) (string, string, bool) {
	if !strings.HasPrefix(body, "## Deploying ") || !strings.Contains(body, "Cloudflare Pages") {
		return "", "", false
	}
	root, err := html.Parse(strings.NewReader(body))
	if err != nil {
		return "", "", false
	}
	var text func(*html.Node) string
	text = func(n *html.Node) string {
		if n.Type == html.TextNode {
			return n.Data
		}
		var parts []string
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			parts = append(parts, text(c))
		}
		return strings.Join(parts, " ")
	}
	fields := map[string]string{}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			var cells []string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && c.Data == "td" {
					cells = append(cells, normalized(text(c)))
				}
			}
			if len(cells) == 2 {
				// Duplicate labels make the message ambiguous: retain generic handling.
				if _, exists := fields[cells[0]]; exists {
					fields[cells[0]] = ""
				} else {
					fields[cells[0]] = cells[1]
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	commit, status := fields["Latest commit:"], strings.ToLower(fields["Status:"])
	if !previewCommit.MatchString(commit) {
		return "", "", false
	}
	status = strings.TrimFunc(status, func(r rune) bool { return !unicode.IsLetter(r) })
	var outcome string
	switch {
	case status == "deploy successful":
		outcome = "success"
	case status == "deploy failed", status == "build failed":
		outcome = "failure"
	case status == "build in progress", status == "deploy in progress", status == "building", status == "deploying", status == "queued":
		outcome = "progress"
	default:
		return "", "", false
	}
	return strings.ToLower(commit), outcome, true
}

func (f *Filter) cloudflarePreviewDecision(ctx context.Context, p filterPayload, event, key string, state *watchState, d *Decision) (bool, error) {
	if event != "issue_comment" || p.Comment == nil ||
		!strings.EqualFold(p.Sender.Login, "cloudflare-workers-and-pages[bot]") ||
		(p.Action != "created" && p.Action != "edited") || p.Comment.ID <= 0 {
		return false, nil
	}
	updated, err := time.Parse(time.RFC3339Nano, p.Comment.UpdatedAt)
	if err != nil {
		return false, nil
	}
	commit, outcome, ok := cloudflarePreview(p.Comment.Body)
	if !ok {
		return false, nil
	}
	comment := strconv.FormatInt(p.Comment.ID, 10)
	old := state.PreviewComments[comment]
	if updated.Before(old.UpdatedAt) {
		d.Disposition, d.Reason = "drop", "preview_outdated"
		return true, nil
	}
	current := previewState{Commit: commit, Outcome: old.Outcome, UpdatedAt: updated}
	if commit != old.Commit {
		current.Outcome = ""
	}
	switch {
	case outcome == "progress":
		d.Disposition, d.Reason = "drop", "preview_progress"
	case current.Outcome == outcome:
		d.Disposition, d.Reason = "drop", "preview_unchanged"
	default:
		current.Outcome = outcome
		d.Disposition, d.Reason = "keep", "preview_ready"
		if outcome == "failure" {
			d.Reason = "preview_failed"
		}
	}
	if state.PreviewComments == nil {
		state.PreviewComments = map[string]previewState{}
	}
	state.PreviewComments[comment] = current
	encoded, _ := json.Marshal(state)
	return true, f.Store.PutWebhookState(ctx, "watch", key, string(encoded))
}
