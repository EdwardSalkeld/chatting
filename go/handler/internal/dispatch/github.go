package dispatch

import (
	"context"
	"errors"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
)

type GitHubCommandRunner func(ctx context.Context, command []string) error

type GitHubIssueCommentSender struct {
	runCommand GitHubCommandRunner
}

func NewGitHubIssueCommentSender(commandRunner GitHubCommandRunner) *GitHubIssueCommentSender {
	if commandRunner == nil {
		commandRunner = defaultGitHubCommandRunner
	}
	return &GitHubIssueCommentSender{runCommand: commandRunner}
}

func (sender *GitHubIssueCommentSender) Send(ctx context.Context, target string, body string) error {
	if strings.TrimSpace(target) == "" {
		return errors.New("github_issue_target_invalid")
	}
	if strings.TrimSpace(body) == "" {
		return errors.New("body is required")
	}

	repository, issueNumber, err := parseGitHubIssueTarget(target)
	if err != nil {
		return err
	}
	command := []string{
		"gh",
		"issue",
		"comment",
		strconv.Itoa(issueNumber),
		"--repo",
		repository,
		"--body",
		body,
	}
	if err := sender.runCommand(ctx, command); err != nil {
		return errors.New("github_issue_comment_failed")
	}
	return nil
}

func defaultGitHubCommandRunner(ctx context.Context, command []string) error {
	if len(command) == 0 {
		return errors.New("missing command")
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	return cmd.Run()
}

func parseGitHubIssueTarget(target string) (string, int, error) {
	stripped := strings.TrimSpace(target)
	if stripped == "" {
		return "", 0, errors.New("github_issue_target_invalid")
	}

	var repository string
	var numberText string
	if strings.HasPrefix(stripped, "http://") || strings.HasPrefix(stripped, "https://") {
		parsed, err := url.Parse(stripped)
		if err != nil || !strings.EqualFold(parsed.Host, "github.com") {
			return "", 0, errors.New("github_issue_target_invalid")
		}
		parts := make([]string, 0, 4)
		for _, part := range strings.Split(parsed.Path, "/") {
			if part != "" {
				parts = append(parts, part)
			}
		}
		if len(parts) < 4 || (parts[2] != "issues" && parts[2] != "pull") {
			return "", 0, errors.New("github_issue_target_invalid")
		}
		repository = parts[0] + "/" + parts[1]
		numberText = parts[3]
	} else {
		repositoryPart, numberPart, ok := strings.Cut(stripped, "#")
		if !ok {
			return "", 0, errors.New("github_issue_target_invalid")
		}
		repository = strings.TrimSpace(repositoryPart)
		numberText = strings.TrimSpace(numberPart)
		if repository == "" || !strings.Contains(repository, "/") {
			return "", 0, errors.New("github_issue_target_invalid")
		}
	}

	issueNumber, err := strconv.Atoi(numberText)
	if err != nil || issueNumber <= 0 {
		return "", 0, errors.New("github_issue_target_invalid")
	}
	return repository, issueNumber, nil
}
