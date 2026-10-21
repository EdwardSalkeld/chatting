package github

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/routing"
)

func TestRunPullRequestsUsesTheRunsPullRequests(t *testing.T) {
	calls := []string{}
	resolve := RunPullRequests(func(_ context.Context, path string) ([]byte, error) {
		calls = append(calls, path)
		return []byte(`{"head_sha":"abc","pull_requests":[{"number":50}]}`), nil
	})

	keys, err := resolve(context.Background(), routing.RunRef{Repo: "owner/repo", ID: 7})

	if err != nil || !reflect.DeepEqual(keys, []string{"owner/repo#50"}) {
		t.Fatalf("keys = %v, err = %v", keys, err)
	}
	if !reflect.DeepEqual(calls, []string{"repos/owner/repo/actions/runs/7"}) {
		t.Fatalf("calls = %v", calls)
	}
}

func TestRunPullRequestsFallsBackToTheHeadCommit(t *testing.T) {
	responses := map[string]string{
		"repos/owner/repo/actions/runs/7":    `{"head_sha":"abc","pull_requests":[]}`,
		"repos/owner/repo/commits/abc/pulls": `[{"number":51}]`,
	}
	resolve := RunPullRequests(func(_ context.Context, path string) ([]byte, error) {
		body, ok := responses[path]
		if !ok {
			return nil, fmt.Errorf("unexpected path %s", path)
		}
		return []byte(body), nil
	})

	keys, err := resolve(context.Background(), routing.RunRef{Repo: "owner/repo", ID: 7})

	if err != nil || !reflect.DeepEqual(keys, []string{"owner/repo#51"}) {
		t.Fatalf("keys = %v, err = %v", keys, err)
	}
}
