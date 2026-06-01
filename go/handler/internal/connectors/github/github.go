package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const (
	Source                   = "webhook"
	IssueAssignmentsStream   = "issue-assignments"
	PullRequestReviewsStream = "pull-request-reviews"
)

const AssignedEventsQuery = `
query (
  $repoOwner: String!
  $repoName: String!
  $issueLimit: Int!
  $timelineLimit: Int!
) {
  repository(owner: $repoOwner, name: $repoName) {
    id
    nameWithOwner
    issues(first: $issueLimit, orderBy: { field: UPDATED_AT, direction: DESC }) {
      nodes {
        id
        number
        title
        body
        url
        labels(first: 20) {
          nodes {
            name
          }
        }
        timelineItems(last: $timelineLimit, itemTypes: [ASSIGNED_EVENT]) {
          nodes {
            ... on AssignedEvent {
              id
              createdAt
              actor {
                login
              }
              assignee {
                __typename
                ... on User {
                  login
                }
              }
            }
          }
        }
      }
    }
  }
}
`

const PullRequestReviewsQuery = `
query (
  $repoOwner: String!
  $repoName: String!
  $pullRequestLimit: Int!
  $reviewLimit: Int!
) {
  repository(owner: $repoOwner, name: $repoName) {
    id
    nameWithOwner
    pullRequests(
      first: $pullRequestLimit
      states: OPEN
      orderBy: { field: UPDATED_AT, direction: DESC }
    ) {
      nodes {
        id
        number
        title
        body
        url
        author {
          login
        }
        closingIssuesReferences(first: 10) {
          nodes {
            number
            title
            url
          }
        }
        reviews(last: $reviewLimit) {
          nodes {
            id
            submittedAt
            state
            body
            url
            author {
              login
            }
            comments {
              totalCount
            }
          }
        }
      }
    }
  }
}
`

const ViewerLoginQuery = `
query {
  viewer {
    login
  }
}
`

const OwnerRepositoriesQuery = `
query (
  $owner: String!
  $after: String
) {
  organization(login: $owner) {
    repositories(first: 100, after: $after, orderBy: { field: UPDATED_AT, direction: DESC }) {
      nodes {
        nameWithOwner
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
  user(login: $owner) {
    repositories(
      first: 100
      after: $after
      ownerAffiliations: OWNER
      orderBy: { field: UPDATED_AT, direction: DESC }
    ) {
      nodes {
        nameWithOwner
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
`

type GraphQLRunner func(ctx context.Context, query string, variables map[string]any) (map[string]any, error)

type CheckpointStore interface {
	GetGitHubCheckpoint(ctx context.Context, scopeKey string) (*AssignmentCheckpoint, error)
	SetGitHubCheckpoint(ctx context.Context, scopeKey string, checkpoint AssignmentCheckpoint) error
}

type IssueAssignmentEvent struct {
	EventID                 string
	EventCreatedAt          time.Time
	RepositoryID            string
	RepositoryNameWithOwner string
	IssueID                 string
	IssueNumber             int
	IssueTitle              string
	IssueBody               string
	IssueURL                string
	AssigneeLogin           string
	ActorLogin              *string
	Labels                  []string
}

func (event IssueAssignmentEvent) CheckpointEventID() string {
	return event.EventID
}

func (event IssueAssignmentEvent) CheckpointEventCreatedAt() time.Time {
	return event.EventCreatedAt
}

func (event IssueAssignmentEvent) DedupeKey() string {
	return "github:" + event.RepositoryID + ":" + event.IssueID + ":" + event.EventID
}

func (event IssueAssignmentEvent) EnvelopeID() string {
	return fmt.Sprintf("github-assignment:%s:%d:%s", event.RepositoryNameWithOwner, event.IssueNumber, event.EventID)
}

func (event IssueAssignmentEvent) ToTaskEnvelope(contextRefs []string) contracts.TaskEnvelope {
	labelsSummary := "(none)"
	if len(event.Labels) > 0 {
		labelsSummary = strings.Join(event.Labels, ", ")
	}
	actor := "unknown"
	if event.ActorLogin != nil && strings.TrimSpace(*event.ActorLogin) != "" {
		actor = *event.ActorLogin
	}
	issueBody := strings.TrimSpace(event.IssueBody)
	if issueBody == "" {
		issueBody = "(empty)"
	}
	content := "GitHub issue assignment detected.\n\n" +
		fmt.Sprintf("Repository: %s\n", event.RepositoryNameWithOwner) +
		fmt.Sprintf("Issue: #%d %s\n", event.IssueNumber, event.IssueTitle) +
		fmt.Sprintf("URL: %s\n", event.IssueURL) +
		fmt.Sprintf("Assigned to: %s\n", event.AssigneeLogin) +
		fmt.Sprintf("Assigned by: %s\n", actor) +
		fmt.Sprintf("Labels: %s\n", labelsSummary) +
		fmt.Sprintf("Assigned event id: %s\n", event.EventID) +
		fmt.Sprintf("Assigned at: %s\n\n", formatEventTime(event.EventCreatedAt)) +
		"Issue body:\n" + issueBody
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            event.EnvelopeID(),
		Source:        Source,
		ReceivedAt:    contracts.NewTimestamp(event.EventCreatedAt),
		Actor:         event.ActorLogin,
		Content:       content,
		Attachments:   []contracts.AttachmentRef{},
		ContextRefs:   append([]string{}, contextRefs...),
		ReplyChannel:  contracts.ReplyChannel{Type: "github", Target: event.IssueURL},
		DedupeKey:     event.DedupeKey(),
	}
}

type PullRequestReviewEvent struct {
	EventID                 string
	EventCreatedAt          time.Time
	RepositoryID            string
	RepositoryNameWithOwner string
	PullRequestID           string
	PullRequestNumber       int
	PullRequestTitle        string
	PullRequestBody         string
	PullRequestURL          string
	PullRequestAuthorLogin  string
	ReviewAuthorLogin       string
	ReviewState             string
	ReviewBody              string
	ReviewURL               string
	ReviewCommentCount      int
	ClosingIssueRefs        []string
}

func (event PullRequestReviewEvent) CheckpointEventID() string {
	return event.EventID
}

func (event PullRequestReviewEvent) CheckpointEventCreatedAt() time.Time {
	return event.EventCreatedAt
}

func (event PullRequestReviewEvent) DedupeKey() string {
	return "github-review:" + event.RepositoryID + ":" + event.PullRequestID + ":" + event.EventID
}

func (event PullRequestReviewEvent) EnvelopeID() string {
	return fmt.Sprintf("github-review:%s:%d:%s", event.RepositoryNameWithOwner, event.PullRequestNumber, event.EventID)
}

func (event PullRequestReviewEvent) ToTaskEnvelope(contextRefs []string) contracts.TaskEnvelope {
	closingIssuesSummary := "(none linked)"
	if len(event.ClosingIssueRefs) > 0 {
		closingIssuesSummary = strings.Join(event.ClosingIssueRefs, ", ")
	}
	reviewBody := strings.TrimSpace(event.ReviewBody)
	if reviewBody == "" {
		reviewBody = "(empty)"
	}
	pullRequestBody := strings.TrimSpace(event.PullRequestBody)
	if pullRequestBody == "" {
		pullRequestBody = "(empty)"
	}
	actor := event.ReviewAuthorLogin
	content := "GitHub pull request review detected.\n\n" +
		fmt.Sprintf("Repository: %s\n", event.RepositoryNameWithOwner) +
		fmt.Sprintf("Pull request: #%d %s\n", event.PullRequestNumber, event.PullRequestTitle) +
		fmt.Sprintf("Pull request URL: %s\n", event.PullRequestURL) +
		fmt.Sprintf("Pull request author: %s\n", event.PullRequestAuthorLogin) +
		fmt.Sprintf("Review state: %s\n", event.ReviewState) +
		fmt.Sprintf("Review author: %s\n", event.ReviewAuthorLogin) +
		fmt.Sprintf("Review URL: %s\n", event.ReviewURL) +
		fmt.Sprintf("Inline review comments: %d\n", event.ReviewCommentCount) +
		fmt.Sprintf("Linked issues: %s\n", closingIssuesSummary) +
		fmt.Sprintf("Review id: %s\n", event.EventID) +
		fmt.Sprintf("Reviewed at: %s\n\n", formatEventTime(event.EventCreatedAt)) +
		"Read the review and any inline comments on this pull request, then make changes or respond on GitHub as needed.\n\n" +
		"Review body:\n" + reviewBody + "\n\n" +
		"Pull request body:\n" + pullRequestBody
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            event.EnvelopeID(),
		Source:        Source,
		ReceivedAt:    contracts.NewTimestamp(event.EventCreatedAt),
		Actor:         &actor,
		Content:       content,
		Attachments:   []contracts.AttachmentRef{},
		ContextRefs:   append([]string{}, contextRefs...),
		ReplyChannel:  contracts.ReplyChannel{Type: "github", Target: event.PullRequestURL},
		DedupeKey:     event.DedupeKey(),
	}
}

type IssueAssignmentConnector struct {
	repositoryPatterns []string
	assigneeLogin      string
	contextRefs        []string
	checkpointStore    CheckpointStore
	maxIssues          int
	maxTimelineEvents  int
	graphqlRunner      GraphQLRunner
	pendingCheckpoint  *AssignmentCheckpoint
}

type IssueAssignmentConfig struct {
	RepositoryPatterns []string
	AssigneeLogin      string
	ContextRefs        []string
	CheckpointStore    CheckpointStore
	MaxIssues          int
	MaxTimelineEvents  int
	GraphQLRunner      GraphQLRunner
}

func NewIssueAssignmentConnector(config IssueAssignmentConfig) (*IssueAssignmentConnector, error) {
	if len(config.RepositoryPatterns) == 0 {
		return nil, errors.New("repository_patterns are required")
	}
	if strings.TrimSpace(config.AssigneeLogin) == "" {
		return nil, errors.New("assignee_login is required")
	}
	if config.CheckpointStore == nil {
		return nil, errors.New("checkpoint_store is required")
	}
	if config.MaxIssues <= 0 {
		config.MaxIssues = 25
	}
	if config.MaxTimelineEvents <= 0 {
		config.MaxTimelineEvents = 10
	}
	runner := config.GraphQLRunner
	if runner == nil {
		runner = DefaultGraphQLRunner
	}
	return &IssueAssignmentConnector{
		repositoryPatterns: append([]string{}, config.RepositoryPatterns...),
		assigneeLogin:      strings.TrimSpace(config.AssigneeLogin),
		contextRefs:        append([]string{}, config.ContextRefs...),
		checkpointStore:    config.CheckpointStore,
		maxIssues:          config.MaxIssues,
		maxTimelineEvents:  config.MaxTimelineEvents,
		graphqlRunner:      runner,
	}, nil
}

func (connector *IssueAssignmentConnector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := connector.flushPendingCheckpoint(ctx); err != nil {
		return nil, err
	}
	scopeKey, err := CheckpointScopeKey(connector.repositoryPatterns, connector.assigneeLogin, IssueAssignmentsStream)
	if err != nil {
		return nil, err
	}
	checkpoint, err := connector.checkpointStore.GetGitHubCheckpoint(ctx, scopeKey)
	if err != nil {
		return nil, err
	}
	repositories, err := ExpandRepositoryPatterns(ctx, connector.repositoryPatterns, connector.graphqlRunner)
	if err != nil {
		return nil, err
	}
	events := []IssueAssignmentEvent{}
	for _, repository := range repositories {
		owner, name, err := ParseRepoSlug(repository)
		if err != nil {
			continue
		}
		repositoryEvents, err := FetchAssignmentEventsForRepository(ctx, owner, name, connector.assigneeLogin, connector.maxIssues, connector.maxTimelineEvents, connector.graphqlRunner)
		if err != nil {
			continue
		}
		events = append(events, repositoryEvents...)
	}
	newEvents := SelectEventsAfterCheckpoint(events, checkpoint)
	if len(newEvents) > 0 {
		latest := newEvents[len(newEvents)-1]
		connector.pendingCheckpoint = &AssignmentCheckpoint{
			EventCreatedAt: latest.EventCreatedAt,
			EventID:        latest.EventID,
		}
	}
	envelopes := make([]contracts.TaskEnvelope, 0, len(newEvents))
	for _, event := range newEvents {
		envelopes = append(envelopes, event.ToTaskEnvelope(connector.contextRefs))
	}
	return envelopes, nil
}

func (connector *IssueAssignmentConnector) flushPendingCheckpoint(ctx context.Context) error {
	if connector.pendingCheckpoint == nil {
		return nil
	}
	scopeKey, err := CheckpointScopeKey(connector.repositoryPatterns, connector.assigneeLogin, IssueAssignmentsStream)
	if err != nil {
		return err
	}
	if err := connector.checkpointStore.SetGitHubCheckpoint(ctx, scopeKey, *connector.pendingCheckpoint); err != nil {
		return err
	}
	connector.pendingCheckpoint = nil
	return nil
}

type PullRequestReviewConnector struct {
	repositoryPatterns []string
	authorLogin        string
	contextRefs        []string
	checkpointStore    CheckpointStore
	maxPullRequests    int
	maxReviews         int
	graphqlRunner      GraphQLRunner
	pendingCheckpoint  *AssignmentCheckpoint
}

type PullRequestReviewConfig struct {
	RepositoryPatterns []string
	AuthorLogin        string
	ContextRefs        []string
	CheckpointStore    CheckpointStore
	MaxPullRequests    int
	MaxReviews         int
	GraphQLRunner      GraphQLRunner
}

func NewPullRequestReviewConnector(config PullRequestReviewConfig) (*PullRequestReviewConnector, error) {
	if len(config.RepositoryPatterns) == 0 {
		return nil, errors.New("repository_patterns are required")
	}
	if strings.TrimSpace(config.AuthorLogin) == "" {
		return nil, errors.New("author_login is required")
	}
	if config.CheckpointStore == nil {
		return nil, errors.New("checkpoint_store is required")
	}
	if config.MaxPullRequests <= 0 {
		config.MaxPullRequests = 25
	}
	if config.MaxReviews <= 0 {
		config.MaxReviews = 10
	}
	runner := config.GraphQLRunner
	if runner == nil {
		runner = DefaultGraphQLRunner
	}
	return &PullRequestReviewConnector{
		repositoryPatterns: append([]string{}, config.RepositoryPatterns...),
		authorLogin:        strings.TrimSpace(config.AuthorLogin),
		contextRefs:        append([]string{}, config.ContextRefs...),
		checkpointStore:    config.CheckpointStore,
		maxPullRequests:    config.MaxPullRequests,
		maxReviews:         config.MaxReviews,
		graphqlRunner:      runner,
	}, nil
}

func (connector *PullRequestReviewConnector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := connector.flushPendingCheckpoint(ctx); err != nil {
		return nil, err
	}
	scopeKey, err := CheckpointScopeKey(connector.repositoryPatterns, connector.authorLogin, PullRequestReviewsStream)
	if err != nil {
		return nil, err
	}
	checkpoint, err := connector.checkpointStore.GetGitHubCheckpoint(ctx, scopeKey)
	if err != nil {
		return nil, err
	}
	repositories, err := ExpandRepositoryPatterns(ctx, connector.repositoryPatterns, connector.graphqlRunner)
	if err != nil {
		return nil, err
	}
	events := []PullRequestReviewEvent{}
	for _, repository := range repositories {
		owner, name, err := ParseRepoSlug(repository)
		if err != nil {
			continue
		}
		repositoryEvents, err := FetchPullRequestReviewEventsForRepository(ctx, owner, name, connector.authorLogin, connector.maxPullRequests, connector.maxReviews, connector.graphqlRunner)
		if err != nil {
			continue
		}
		events = append(events, repositoryEvents...)
	}
	newEvents := SelectEventsAfterCheckpoint(events, checkpoint)
	if len(newEvents) > 0 {
		latest := newEvents[len(newEvents)-1]
		connector.pendingCheckpoint = &AssignmentCheckpoint{
			EventCreatedAt: latest.EventCreatedAt,
			EventID:        latest.EventID,
		}
	}
	envelopes := make([]contracts.TaskEnvelope, 0, len(newEvents))
	for _, event := range newEvents {
		envelopes = append(envelopes, event.ToTaskEnvelope(connector.contextRefs))
	}
	return envelopes, nil
}

func (connector *PullRequestReviewConnector) flushPendingCheckpoint(ctx context.Context) error {
	if connector.pendingCheckpoint == nil {
		return nil
	}
	scopeKey, err := CheckpointScopeKey(connector.repositoryPatterns, connector.authorLogin, PullRequestReviewsStream)
	if err != nil {
		return err
	}
	if err := connector.checkpointStore.SetGitHubCheckpoint(ctx, scopeKey, *connector.pendingCheckpoint); err != nil {
		return err
	}
	connector.pendingCheckpoint = nil
	return nil
}

func DefaultGraphQLRunner(ctx context.Context, query string, variables map[string]any) (map[string]any, error) {
	args := []string{"api", "graphql", "-f", "query=" + query}
	keys := make([]string, 0, len(variables))
	for key := range variables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := variables[key]
		switch typed := value.(type) {
		case bool:
			args = append(args, "-F", fmt.Sprintf("%s=%t", key, typed))
		case int:
			args = append(args, "-F", fmt.Sprintf("%s=%d", key, typed))
		default:
			args = append(args, "-f", fmt.Sprintf("%s=%v", key, typed))
		}
	}
	command := exec.CommandContext(ctx, "gh", args...)
	output, err := command.Output()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) && len(exitError.Stderr) > 0 {
			return nil, fmt.Errorf("github_graphql_failed:%s", strings.TrimSpace(string(exitError.Stderr)))
		}
		return nil, fmt.Errorf("github_graphql_failed:%v", err)
	}
	var payload map[string]any
	if err := json.NewDecoder(bytes.NewReader(output)).Decode(&payload); err != nil {
		return nil, errors.New("github_graphql_invalid_json")
	}
	return payload, nil
}

func FetchAuthenticatedViewerLogin(ctx context.Context, runner GraphQLRunner) (string, error) {
	if runner == nil {
		runner = DefaultGraphQLRunner
	}
	payload, err := runner(ctx, ViewerLoginQuery, map[string]any{})
	if err != nil {
		return "", err
	}
	if hasGraphQLErrors(payload) {
		return "", fmt.Errorf("github_graphql_errors:%v", payload["errors"])
	}
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return "", errors.New("github_graphql_missing_data")
	}
	viewer, ok := data["viewer"].(map[string]any)
	if !ok {
		return "", errors.New("github_graphql_invalid_viewer")
	}
	return requireString(viewer["login"], "viewer.login")
}

func ExpandRepositoryPatterns(ctx context.Context, repositoryPatterns []string, runner GraphQLRunner) ([]string, error) {
	if len(repositoryPatterns) == 0 {
		return nil, errors.New("repository_patterns are required")
	}
	expanded := []string{}
	seen := map[string]bool{}
	for _, pattern := range repositoryPatterns {
		owner, repo, err := parseRepositoryPattern(pattern)
		if err != nil {
			return nil, err
		}
		if repo == "*" {
			repositories, err := ListOwnerRepositories(ctx, owner, runner)
			if err != nil {
				return nil, err
			}
			for _, repository := range repositories {
				normalizedOwner, normalizedName, err := ParseRepoSlug(repository)
				if err != nil {
					continue
				}
				normalized := normalizedOwner + "/" + normalizedName
				if !seen[normalized] {
					seen[normalized] = true
					expanded = append(expanded, normalized)
				}
			}
			continue
		}
		normalized := owner + "/" + repo
		if !seen[normalized] {
			seen[normalized] = true
			expanded = append(expanded, normalized)
		}
	}
	return expanded, nil
}

func ListOwnerRepositories(ctx context.Context, owner string, runner GraphQLRunner) ([]string, error) {
	if strings.TrimSpace(owner) == "" {
		return nil, errors.New("owner must be a non-empty string")
	}
	if runner == nil {
		runner = DefaultGraphQLRunner
	}
	repositories := []string{}
	var after any
	for {
		variables := map[string]any{"owner": owner}
		if after != nil {
			variables["after"] = after
		}
		payload, err := runner(ctx, OwnerRepositoriesQuery, variables)
		if err != nil {
			return nil, err
		}
		data, ok := payload["data"].(map[string]any)
		if !ok {
			return nil, errors.New("github_graphql_missing_data")
		}
		ownerNode, _ := data["organization"].(map[string]any)
		if ownerNode == nil {
			ownerNode, _ = data["user"].(map[string]any)
		}
		if ownerNode == nil {
			if hasGraphQLErrors(payload) {
				return nil, fmt.Errorf("github_graphql_errors:%v", payload["errors"])
			}
			return repositories, nil
		}
		connection, ok := ownerNode["repositories"].(map[string]any)
		if !ok {
			return nil, errors.New("github_graphql_invalid_owner_repositories")
		}
		nodes, ok := connection["nodes"].([]any)
		if !ok {
			return nil, errors.New("github_graphql_invalid_owner_repository_nodes")
		}
		for _, rawNode := range nodes {
			node, ok := rawNode.(map[string]any)
			if !ok {
				continue
			}
			if slug, ok := node["nameWithOwner"].(string); ok && strings.TrimSpace(slug) != "" {
				repositories = append(repositories, strings.TrimSpace(slug))
			}
		}
		pageInfo, ok := connection["pageInfo"].(map[string]any)
		if !ok {
			return nil, errors.New("github_graphql_invalid_owner_repositories_page_info")
		}
		hasNextPage, ok := pageInfo["hasNextPage"].(bool)
		if !ok {
			return nil, errors.New("github_graphql_invalid_owner_repositories_has_next_page")
		}
		if !hasNextPage {
			break
		}
		endCursor, ok := pageInfo["endCursor"].(string)
		if !ok || strings.TrimSpace(endCursor) == "" {
			return nil, errors.New("github_graphql_invalid_owner_repositories_end_cursor")
		}
		after = endCursor
	}
	return repositories, nil
}

func FetchAssignmentEventsForRepository(ctx context.Context, repoOwner string, repoName string, assigneeLogin string, issueLimit int, timelineLimit int, runner GraphQLRunner) ([]IssueAssignmentEvent, error) {
	if issueLimit <= 0 {
		return nil, errors.New("issue_limit must be positive")
	}
	if timelineLimit <= 0 {
		return nil, errors.New("timeline_limit must be positive")
	}
	if runner == nil {
		runner = DefaultGraphQLRunner
	}
	payload, err := runner(ctx, AssignedEventsQuery, map[string]any{
		"repoOwner":     repoOwner,
		"repoName":      repoName,
		"issueLimit":    issueLimit,
		"timelineLimit": timelineLimit,
	})
	if err != nil {
		return nil, err
	}
	if hasGraphQLErrors(payload) {
		return nil, fmt.Errorf("github_graphql_errors:%v", payload["errors"])
	}
	repository, err := repositoryPayload(payload)
	if err != nil || repository == nil {
		return nil, err
	}
	repositoryID, err := requireString(repository["id"], "repository.id")
	if err != nil {
		return nil, err
	}
	repositoryNameWithOwner, err := requireString(repository["nameWithOwner"], "repository.nameWithOwner")
	if err != nil {
		return nil, err
	}
	issues, ok := repository["issues"].(map[string]any)
	if !ok {
		return nil, errors.New("github_graphql_invalid_issues")
	}
	issueNodes, ok := issues["nodes"].([]any)
	if !ok {
		return nil, errors.New("github_graphql_invalid_issue_nodes")
	}
	expectedAssigneeLogin := strings.ToLower(assigneeLogin)
	events := []IssueAssignmentEvent{}
	for _, rawIssue := range issueNodes {
		issue, ok := rawIssue.(map[string]any)
		if !ok {
			continue
		}
		issueID, issueIDErr := requireString(issue["id"], "issue.id")
		issueNumber, issueNumberErr := requirePositiveInt(issue["number"], "issue.number")
		issueTitle, issueTitleErr := requireString(issue["title"], "issue.title")
		issueURL, issueURLErr := requireString(issue["url"], "issue.url")
		if issueIDErr != nil || issueNumberErr != nil || issueTitleErr != nil || issueURLErr != nil {
			continue
		}
		issueBody, _ := issue["body"].(string)
		labels := parseLabels(issue["labels"])
		timelineItems, ok := issue["timelineItems"].(map[string]any)
		if !ok {
			continue
		}
		timelineNodes, ok := timelineItems["nodes"].([]any)
		if !ok {
			continue
		}
		for _, rawTimelineNode := range timelineNodes {
			timelineNode, ok := rawTimelineNode.(map[string]any)
			if !ok {
				continue
			}
			assignee, ok := timelineNode["assignee"].(map[string]any)
			if !ok || assignee["__typename"] != "User" {
				continue
			}
			assigneeNodeLogin, ok := assignee["login"].(string)
			if !ok || strings.ToLower(assigneeNodeLogin) != expectedAssigneeLogin {
				continue
			}
			eventID, eventIDErr := requireString(timelineNode["id"], "assigned_event.id")
			createdAtRaw, createdAtRawErr := requireString(timelineNode["createdAt"], "assigned_event.createdAt")
			createdAt, createdAtErr := parseEventTime(createdAtRaw)
			if eventIDErr != nil || createdAtRawErr != nil || createdAtErr != nil {
				continue
			}
			events = append(events, IssueAssignmentEvent{
				EventID:                 eventID,
				EventCreatedAt:          createdAt,
				RepositoryID:            repositoryID,
				RepositoryNameWithOwner: repositoryNameWithOwner,
				IssueID:                 issueID,
				IssueNumber:             issueNumber,
				IssueTitle:              issueTitle,
				IssueBody:               issueBody,
				IssueURL:                issueURL,
				AssigneeLogin:           assigneeNodeLogin,
				ActorLogin:              parseActorLogin(timelineNode["actor"]),
				Labels:                  labels,
			})
		}
	}
	return events, nil
}

func FetchPullRequestReviewEventsForRepository(ctx context.Context, repoOwner string, repoName string, authorLogin string, pullRequestLimit int, reviewLimit int, runner GraphQLRunner) ([]PullRequestReviewEvent, error) {
	if pullRequestLimit <= 0 {
		return nil, errors.New("pull_request_limit must be positive")
	}
	if reviewLimit <= 0 {
		return nil, errors.New("review_limit must be positive")
	}
	if runner == nil {
		runner = DefaultGraphQLRunner
	}
	payload, err := runner(ctx, PullRequestReviewsQuery, map[string]any{
		"repoOwner":        repoOwner,
		"repoName":         repoName,
		"pullRequestLimit": pullRequestLimit,
		"reviewLimit":      reviewLimit,
	})
	if err != nil {
		return nil, err
	}
	if hasGraphQLErrors(payload) {
		return nil, fmt.Errorf("github_graphql_errors:%v", payload["errors"])
	}
	repository, err := repositoryPayload(payload)
	if err != nil || repository == nil {
		return nil, err
	}
	repositoryID, err := requireString(repository["id"], "repository.id")
	if err != nil {
		return nil, err
	}
	repositoryNameWithOwner, err := requireString(repository["nameWithOwner"], "repository.nameWithOwner")
	if err != nil {
		return nil, err
	}
	pullRequests, ok := repository["pullRequests"].(map[string]any)
	if !ok {
		return nil, errors.New("github_graphql_invalid_pull_requests")
	}
	pullRequestNodes, ok := pullRequests["nodes"].([]any)
	if !ok {
		return nil, errors.New("github_graphql_invalid_pull_request_nodes")
	}
	expectedAuthorLogin := strings.ToLower(authorLogin)
	events := []PullRequestReviewEvent{}
	for _, rawPullRequest := range pullRequestNodes {
		pullRequest, ok := rawPullRequest.(map[string]any)
		if !ok {
			continue
		}
		pullRequestAuthorLogin := parseActorLogin(pullRequest["author"])
		if pullRequestAuthorLogin == nil || strings.ToLower(*pullRequestAuthorLogin) != expectedAuthorLogin {
			continue
		}
		pullRequestID, pullRequestIDErr := requireString(pullRequest["id"], "pull_request.id")
		pullRequestNumber, pullRequestNumberErr := requirePositiveInt(pullRequest["number"], "pull_request.number")
		pullRequestTitle, pullRequestTitleErr := requireString(pullRequest["title"], "pull_request.title")
		pullRequestURL, pullRequestURLErr := requireString(pullRequest["url"], "pull_request.url")
		if pullRequestIDErr != nil || pullRequestNumberErr != nil || pullRequestTitleErr != nil || pullRequestURLErr != nil {
			continue
		}
		pullRequestBody, _ := pullRequest["body"].(string)
		closingIssueRefs := parseIssueReferenceSummaries(pullRequest["closingIssuesReferences"])
		reviews, ok := pullRequest["reviews"].(map[string]any)
		if !ok {
			continue
		}
		reviewNodes, ok := reviews["nodes"].([]any)
		if !ok {
			continue
		}
		for _, rawReview := range reviewNodes {
			review, ok := rawReview.(map[string]any)
			if !ok {
				continue
			}
			reviewAuthorLogin := parseActorLogin(review["author"])
			if reviewAuthorLogin == nil || strings.ToLower(*reviewAuthorLogin) == expectedAuthorLogin {
				continue
			}
			submittedAt, ok := review["submittedAt"].(string)
			if !ok || strings.TrimSpace(submittedAt) == "" {
				continue
			}
			eventID, eventIDErr := requireString(review["id"], "review.id")
			reviewState, reviewStateErr := requireString(review["state"], "review.state")
			reviewURL, reviewURLErr := requireString(review["url"], "review.url")
			reviewCreatedAt, reviewCreatedAtErr := parseEventTime(submittedAt)
			reviewCommentCount, reviewCommentCountErr := parseTotalCount(review["comments"], "review.comments.totalCount")
			if eventIDErr != nil || reviewStateErr != nil || reviewURLErr != nil || reviewCreatedAtErr != nil || reviewCommentCountErr != nil {
				continue
			}
			reviewBody, _ := review["body"].(string)
			events = append(events, PullRequestReviewEvent{
				EventID:                 eventID,
				EventCreatedAt:          reviewCreatedAt,
				RepositoryID:            repositoryID,
				RepositoryNameWithOwner: repositoryNameWithOwner,
				PullRequestID:           pullRequestID,
				PullRequestNumber:       pullRequestNumber,
				PullRequestTitle:        pullRequestTitle,
				PullRequestBody:         pullRequestBody,
				PullRequestURL:          pullRequestURL,
				PullRequestAuthorLogin:  *pullRequestAuthorLogin,
				ReviewAuthorLogin:       *reviewAuthorLogin,
				ReviewState:             reviewState,
				ReviewBody:              reviewBody,
				ReviewURL:               reviewURL,
				ReviewCommentCount:      reviewCommentCount,
				ClosingIssueRefs:        closingIssueRefs,
			})
		}
	}
	return events, nil
}

func ParseRepoSlug(value string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(value), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("repository slug must be owner/repo")
	}
	return parts[0], parts[1], nil
}

func parseRepositoryPattern(value string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(value), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("repository selector must be owner/repo or owner/*")
	}
	if parts[1] != "*" && strings.Contains(parts[1], "*") {
		return "", "", errors.New("repository selector must be owner/repo or owner/*")
	}
	return parts[0], parts[1], nil
}

func repositoryPayload(payload map[string]any) (map[string]any, error) {
	data, ok := payload["data"].(map[string]any)
	if !ok {
		return nil, errors.New("github_graphql_missing_data")
	}
	repository, ok := data["repository"].(map[string]any)
	if !ok {
		if data["repository"] == nil {
			return nil, nil
		}
		return nil, errors.New("github_graphql_invalid_repository")
	}
	return repository, nil
}

func parseLabels(raw any) []string {
	rawLabels, ok := raw.(map[string]any)
	if !ok {
		return []string{}
	}
	nodes, ok := rawLabels["nodes"].([]any)
	if !ok {
		return []string{}
	}
	labels := []string{}
	for _, rawNode := range nodes {
		node, ok := rawNode.(map[string]any)
		if !ok {
			continue
		}
		name, ok := node["name"].(string)
		if ok && strings.TrimSpace(name) != "" {
			labels = append(labels, strings.TrimSpace(name))
		}
	}
	return labels
}

func parseActorLogin(raw any) *string {
	actor, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	login, ok := actor["login"].(string)
	if !ok || strings.TrimSpace(login) == "" {
		return nil
	}
	return &login
}

func parseIssueReferenceSummaries(raw any) []string {
	references, ok := raw.(map[string]any)
	if !ok {
		return []string{}
	}
	nodes, ok := references["nodes"].([]any)
	if !ok {
		return []string{}
	}
	result := []string{}
	for _, rawNode := range nodes {
		node, ok := rawNode.(map[string]any)
		if !ok {
			continue
		}
		number, err := positiveInt(node["number"])
		if err != nil {
			continue
		}
		title, ok := node["title"].(string)
		if ok && strings.TrimSpace(title) != "" {
			result = append(result, fmt.Sprintf("#%d %s", number, strings.TrimSpace(title)))
		} else {
			result = append(result, fmt.Sprintf("#%d", number))
		}
	}
	return result
}

func parseTotalCount(raw any, fieldName string) (int, error) {
	connection, ok := raw.(map[string]any)
	if !ok {
		return 0, fmt.Errorf("%s must be an object", fieldName)
	}
	totalCount, err := positiveIntAllowZero(connection["totalCount"])
	if err != nil {
		return 0, fmt.Errorf("%s must be a non-negative integer", fieldName)
	}
	return totalCount, nil
}

func requireString(value any, fieldName string) (string, error) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%s must be a non-empty string", fieldName)
	}
	return text, nil
}

func requirePositiveInt(value any, fieldName string) (int, error) {
	result, err := positiveInt(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a positive integer", fieldName)
	}
	return result, nil
}

func positiveInt(value any) (int, error) {
	result, err := positiveIntAllowZero(value)
	if err != nil || result <= 0 {
		return 0, errors.New("positive integer required")
	}
	return result, nil
}

func positiveIntAllowZero(value any) (int, error) {
	switch typed := value.(type) {
	case int:
		if typed < 0 {
			return 0, errors.New("non-negative integer required")
		}
		return typed, nil
	case int64:
		if typed < 0 {
			return 0, errors.New("non-negative integer required")
		}
		return int(typed), nil
	case float64:
		if typed < 0 || typed != float64(int(typed)) {
			return 0, errors.New("non-negative integer required")
		}
		return int(typed), nil
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		if err != nil || parsed < 0 {
			return 0, errors.New("non-negative integer required")
		}
		return parsed, nil
	default:
		return 0, errors.New("non-negative integer required")
	}
}

func hasGraphQLErrors(payload map[string]any) bool {
	errorsValue, ok := payload["errors"]
	if !ok || errorsValue == nil {
		return false
	}
	if list, ok := errorsValue.([]any); ok {
		return len(list) > 0
	}
	return true
}

func parseEventTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func formatEventTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05Z")
}
