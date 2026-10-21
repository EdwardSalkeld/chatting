package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/routing"
)

// RESTRunner returns the JSON body of a GitHub REST API path.
type RESTRunner func(ctx context.Context, path string) ([]byte, error)

func DefaultRESTRunner(ctx context.Context, path string) ([]byte, error) {
	command := exec.CommandContext(ctx, "gh", "api", path)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("github_rest_failed:%s", strings.TrimSpace(stderr.String()))
	}
	return output, nil
}

// RunPullRequests resolves a workflow run to the routing keys of its PRs.
func RunPullRequests(runner RESTRunner) func(context.Context, routing.RunRef) ([]string, error) {
	if runner == nil {
		runner = DefaultRESTRunner
	}
	return func(ctx context.Context, run routing.RunRef) ([]string, error) {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		body, err := runner(ctx, fmt.Sprintf("repos/%s/actions/runs/%d", run.Repo, run.ID))
		if err != nil {
			return nil, err
		}
		var details struct {
			HeadSHA      string `json:"head_sha"`
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		}
		if err := json.Unmarshal(body, &details); err != nil {
			return nil, fmt.Errorf("github_rest_invalid_json:%w", err)
		}
		pulls := details.PullRequests
		// A push-triggered run can leave pull_requests empty; its commit still
		// belongs to the PR.
		if len(pulls) == 0 && details.HeadSHA != "" {
			body, err := runner(ctx, fmt.Sprintf("repos/%s/commits/%s/pulls", run.Repo, details.HeadSHA))
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(body, &pulls); err != nil {
				return nil, fmt.Errorf("github_rest_invalid_json:%w", err)
			}
		}
		keys := make([]string, 0, len(pulls))
		for _, pull := range pulls {
			key, err := routing.NormalizePR(fmt.Sprintf("https://github.com/%s/pull/%d", run.Repo, pull.Number))
			if err == nil {
				keys = append(keys, key)
			}
		}
		return keys, nil
	}
}
