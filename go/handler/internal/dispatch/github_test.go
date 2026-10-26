package dispatch

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestGitHubIssueCommentSenderUsesGHCLIForIssueURLTarget(t *testing.T) {
	var seen [][]string
	sender := NewGitHubIssueCommentSender(func(_ context.Context, command []string) error {
		seen = append(seen, append([]string(nil), command...))
		return nil
	})

	if err := sender.Send(context.Background(), "https://github.com/brokensbone/chatting/issues/12", "hello"); err != nil {
		t.Fatal(err)
	}
	expected := [][]string{{"gh", "issue", "comment", "12", "--repo", "brokensbone/chatting", "--body", "hello"}}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatalf("commands = %#v", seen)
	}
}

func TestGitHubIssueCommentSenderUsesGHCLIForSlugTarget(t *testing.T) {
	var seen [][]string
	sender := NewGitHubIssueCommentSender(func(_ context.Context, command []string) error {
		seen = append(seen, append([]string(nil), command...))
		return nil
	})

	if err := sender.Send(context.Background(), "brokensbone/chatting#13", "hello"); err != nil {
		t.Fatal(err)
	}
	expected := [][]string{{"gh", "issue", "comment", "13", "--repo", "brokensbone/chatting", "--body", "hello"}}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatalf("commands = %#v", seen)
	}
}

func TestGitHubIssueCommentSenderUsesGHCLIForPullRequestURLTarget(t *testing.T) {
	var seen [][]string
	sender := NewGitHubIssueCommentSender(func(_ context.Context, command []string) error {
		seen = append(seen, append([]string(nil), command...))
		return nil
	})

	if err := sender.Send(context.Background(), "https://github.com/brokensbone/chatting/pull/60", "hello"); err != nil {
		t.Fatal(err)
	}
	expected := [][]string{{"gh", "issue", "comment", "60", "--repo", "brokensbone/chatting", "--body", "hello"}}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatalf("commands = %#v", seen)
	}
}

func TestGitHubIssueCommentSenderRejectsInvalidTarget(t *testing.T) {
	sender := NewGitHubIssueCommentSender(func(_ context.Context, _ []string) error { return nil })
	err := sender.Send(context.Background(), "not-a-github-target", "hello")
	if err == nil || err.Error() != "github_issue_target_invalid" {
		t.Fatalf("error = %v", err)
	}
}

func TestGitHubReviewReplyUsesThreadEndpoint(t *testing.T) {
	var seen []string
	sender := NewGitHubIssueCommentSender(func(_ context.Context, command []string) error { seen = command; return nil })
	body := "Thanks.\nHere is the fix: `value` costs $0."
	if err := sender.Send(context.Background(), "https://github.com/o/r/pull/71#discussion_r123", body); err != nil {
		t.Fatal(err)
	}
	expected := []string{"gh", "api", "--method", "POST", "repos/o/r/pulls/71/comments/123/replies", "--raw-field", "body=" + body}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatalf("commands = %#v", seen)
	}
	for _, target := range []string{"https://github.com/o/r/pull/71#discussion_rnope", "https://github.com/o/r/issues/70#discussion_r123"} {
		if err := sender.Send(context.Background(), target, body); err == nil {
			t.Fatalf("accepted invalid thread %s", target)
		}
	}
}

func TestGitHubIssueCommentSenderReturnsFailureWhenGHFails(t *testing.T) {
	sender := NewGitHubIssueCommentSender(func(_ context.Context, _ []string) error {
		return errors.New("boom")
	})
	err := sender.Send(context.Background(), "brokensbone/chatting#13", "hello")
	if err == nil || err.Error() != "github_issue_comment_failed" {
		t.Fatalf("error = %v", err)
	}
}
