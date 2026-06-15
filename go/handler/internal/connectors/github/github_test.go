package github

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type memoryCheckpointStore struct {
	checkpoints map[string]AssignmentCheckpoint
}

func newMemoryCheckpointStore() *memoryCheckpointStore {
	return &memoryCheckpointStore{checkpoints: map[string]AssignmentCheckpoint{}}
}

func (store *memoryCheckpointStore) GetGitHubCheckpoint(_ context.Context, scopeKey string) (*AssignmentCheckpoint, error) {
	checkpoint, ok := store.checkpoints[scopeKey]
	if !ok {
		return nil, nil
	}
	return &checkpoint, nil
}

func (store *memoryCheckpointStore) SetGitHubCheckpoint(_ context.Context, scopeKey string, checkpoint AssignmentCheckpoint) error {
	store.checkpoints[scopeKey] = checkpoint
	return nil
}

func TestFetchAssignmentEventsForRepositoryFiltersByAssigneeLogin(t *testing.T) {
	payload := map[string]any{
		"data": map[string]any{
			"repository": map[string]any{
				"id":            "R_123",
				"nameWithOwner": "brokensbone/chatting",
				"issues": map[string]any{
					"nodes": []any{
						map[string]any{
							"id":     "I_1",
							"number": 12,
							"title":  "Plan milestone 5",
							"body":   "Body text",
							"url":    "https://github.com/brokensbone/chatting/issues/12",
							"labels": map[string]any{"nodes": []any{map[string]any{"name": "enhancement"}, map[string]any{"name": "ai"}}},
							"timelineItems": map[string]any{"nodes": []any{
								map[string]any{
									"id":        "AE_1",
									"createdAt": "2026-03-07T10:47:35Z",
									"actor":     map[string]any{"login": "BillyAcachofa"},
									"assignee":  map[string]any{"__typename": "User", "login": "BillyAcachofa"},
								},
								map[string]any{
									"id":        "AE_2",
									"createdAt": "2026-03-07T10:49:35Z",
									"actor":     map[string]any{"login": "BillyAcachofa"},
									"assignee":  map[string]any{"__typename": "User", "login": "someoneelse"},
								},
							}},
						},
					},
				},
			},
		},
	}

	events, err := FetchAssignmentEventsForRepository(
		context.Background(),
		"brokensbone",
		"chatting",
		"billyacachofa",
		20,
		10,
		func(_ context.Context, _ string, _ map[string]any) (map[string]any, error) {
			return payload, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	if events[0].EventID != "AE_1" {
		t.Fatalf("EventID = %q", events[0].EventID)
	}
	if !reflect.DeepEqual(events[0].Labels, []string{"enhancement", "ai"}) {
		t.Fatalf("Labels = %#v", events[0].Labels)
	}
}

func TestFetchPullRequestReviewEventsForRepositoryFiltersByAuthorLogin(t *testing.T) {
	payload := map[string]any{
		"data": map[string]any{
			"repository": map[string]any{
				"id":            "R_123",
				"nameWithOwner": "brokensbone/chatting",
				"pullRequests": map[string]any{"nodes": []any{
					map[string]any{
						"id":     "PR_1",
						"number": 60,
						"title":  "Add ingress from GitHub reviews",
						"body":   "Implements the review ingress flow.",
						"url":    "https://github.com/brokensbone/chatting/pull/60",
						"author": map[string]any{"login": "BillyAcachofa"},
						"closingIssuesReferences": map[string]any{"nodes": []any{
							map[string]any{"number": 60, "title": "Add ingress from GitHub reviews", "url": "https://github.com/brokensbone/chatting/issues/60"},
						}},
						"reviews": map[string]any{"nodes": []any{
							map[string]any{
								"id":          "PRR_1",
								"submittedAt": "2026-03-07T12:00:00Z",
								"state":       "CHANGES_REQUESTED",
								"body":        "Please wire this into ingress.",
								"url":         "https://github.com/brokensbone/chatting/pull/60#pullrequestreview-1",
								"author":      map[string]any{"login": "brokensbone"},
								"comments":    map[string]any{"totalCount": 2},
							},
							map[string]any{
								"id":          "PRR_2",
								"submittedAt": "2026-03-07T12:05:00Z",
								"state":       "COMMENTED",
								"body":        "Self review should be ignored.",
								"url":         "https://github.com/brokensbone/chatting/pull/60#pullrequestreview-2",
								"author":      map[string]any{"login": "BillyAcachofa"},
								"comments":    map[string]any{"totalCount": 0},
							},
						}},
					},
					map[string]any{
						"id":                      "PR_2",
						"number":                  61,
						"title":                   "Other author's PR",
						"body":                    "",
						"url":                     "https://github.com/brokensbone/chatting/pull/61",
						"author":                  map[string]any{"login": "someoneelse"},
						"closingIssuesReferences": map[string]any{"nodes": []any{}},
						"reviews":                 map[string]any{"nodes": []any{}},
					},
				}},
			},
		},
	}

	events, err := FetchPullRequestReviewEventsForRepository(
		context.Background(),
		"brokensbone",
		"chatting",
		"billyacachofa",
		20,
		10,
		func(_ context.Context, _ string, _ map[string]any) (map[string]any, error) {
			return payload, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	if events[0].EventID != "PRR_1" {
		t.Fatalf("EventID = %q", events[0].EventID)
	}
	if !reflect.DeepEqual(events[0].ClosingIssueRefs, []string{"#60 Add ingress from GitHub reviews"}) {
		t.Fatalf("ClosingIssueRefs = %#v", events[0].ClosingIssueRefs)
	}
}

func TestExpandRepositoryPatternsExpandsOwnerWildcard(t *testing.T) {
	calls := 0
	repositories, err := ExpandRepositoryPatterns(
		context.Background(),
		[]string{"brokensbone/chatting", "brokensbone/*", "brokensbone/chatting"},
		func(_ context.Context, query string, variables map[string]any) (map[string]any, error) {
			calls++
			if query != OwnerRepositoriesQuery {
				t.Fatalf("query = %q", query)
			}
			if variables["owner"] != "brokensbone" {
				t.Fatalf("variables = %#v", variables)
			}
			return map[string]any{
				"data": map[string]any{
					"organization": map[string]any{
						"repositories": map[string]any{
							"nodes":    []any{map[string]any{"nameWithOwner": "brokensbone/chatting"}, map[string]any{"nameWithOwner": "brokensbone/bbmb"}},
							"pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil},
						},
					},
					"user": nil,
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
	if !reflect.DeepEqual(repositories, []string{"brokensbone/chatting", "brokensbone/bbmb"}) {
		t.Fatalf("repositories = %#v", repositories)
	}
}

func TestListOwnerRepositoriesHandlesPartialOrganizationNotFoundError(t *testing.T) {
	repositories, err := ListOwnerRepositories(
		context.Background(),
		"brokensbone",
		func(_ context.Context, query string, variables map[string]any) (map[string]any, error) {
			if query != OwnerRepositoriesQuery {
				t.Fatalf("query = %q", query)
			}
			if variables["owner"] != "brokensbone" {
				t.Fatalf("variables = %#v", variables)
			}
			return map[string]any{
				"data": map[string]any{
					"organization": nil,
					"user": map[string]any{
						"repositories": map[string]any{
							"nodes":    []any{map[string]any{"nameWithOwner": "brokensbone/chatting"}},
							"pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil},
						},
					},
				},
				"errors": []any{
					map[string]any{
						"type":    "NOT_FOUND",
						"path":    []any{"organization"},
						"message": "Could not resolve to an Organization",
					},
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(repositories, []string{"brokensbone/chatting"}) {
		t.Fatalf("repositories = %#v", repositories)
	}
}

func TestDefaultGraphQLRunnerParsesJSONWhenGHExitsNonZero(t *testing.T) {
	tempDir := t.TempDir()
	fakeGHPath := filepath.Join(tempDir, "gh")
	script := `#!/bin/sh
printf '%s\n' '{"data":{"viewer":{"login":"BillyAcachofa"}},"errors":[{"path":["organization"]}]}'
printf '%s\n' 'gh: Could not resolve to an Organization' >&2
exit 1
`
	if err := os.WriteFile(fakeGHPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	originalPath := os.Getenv("PATH")
	t.Setenv("PATH", tempDir+string(os.PathListSeparator)+originalPath)

	payload, err := DefaultGraphQLRunner(context.Background(), ViewerLoginQuery, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["data"].(map[string]any); !ok {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestIssueAssignmentConnectorPollNormalizesAndCheckpoints(t *testing.T) {
	responses := []map[string]any{
		assignmentPayload("AE_1"),
		assignmentPayload("AE_1"),
	}
	store := newMemoryCheckpointStore()
	connector, err := NewIssueAssignmentConnector(IssueAssignmentConfig{
		RepositoryPatterns: []string{"brokensbone/chatting"},
		AssigneeLogin:      "BillyAcachofa",
		ContextRefs:        []string{"repo:/home/edward/chatting"},
		CheckpointStore:    store,
		GraphQLRunner: func(_ context.Context, _ string, variables map[string]any) (map[string]any, error) {
			if variables["repoOwner"] != "brokensbone" || variables["repoName"] != "chatting" {
				t.Fatalf("variables = %#v", variables)
			}
			response := responses[0]
			responses = responses[1:]
			return response, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	firstPoll, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondPoll, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(firstPoll) != 1 {
		t.Fatalf("firstPoll = %#v", firstPoll)
	}
	envelope := firstPoll[0]
	if envelope.ID != "github-assignment:brokensbone/chatting:12:AE_1" {
		t.Fatalf("envelope.ID = %q", envelope.ID)
	}
	if envelope.ReplyChannel.Type != "github" || envelope.ReplyChannel.Target != "https://github.com/brokensbone/chatting/issues/12" {
		t.Fatalf("reply channel = %#v", envelope.ReplyChannel)
	}
	if !reflect.DeepEqual(envelope.ContextRefs, []string{"repo:/home/edward/chatting"}) {
		t.Fatalf("context refs = %#v", envelope.ContextRefs)
	}
	if envelope.DedupeKey != "github:R_1:I_1:AE_1" {
		t.Fatalf("dedupe key = %q", envelope.DedupeKey)
	}
	if len(secondPoll) != 0 {
		t.Fatalf("secondPoll = %#v", secondPoll)
	}
}

func TestPullRequestReviewConnectorPollNormalizesAndCheckpoints(t *testing.T) {
	responses := []map[string]any{
		reviewPayload("PRR_1"),
		reviewPayload("PRR_1"),
	}
	store := newMemoryCheckpointStore()
	connector, err := NewPullRequestReviewConnector(PullRequestReviewConfig{
		RepositoryPatterns: []string{"brokensbone/chatting"},
		AuthorLogin:        "BillyAcachofa",
		ContextRefs:        []string{"repo:/home/edward/chatting"},
		CheckpointStore:    store,
		GraphQLRunner: func(_ context.Context, _ string, variables map[string]any) (map[string]any, error) {
			if variables["pullRequestLimit"] != 25 || variables["reviewLimit"] != 10 {
				t.Fatalf("variables = %#v", variables)
			}
			response := responses[0]
			responses = responses[1:]
			return response, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	firstPoll, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondPoll, err := connector.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(firstPoll) != 1 {
		t.Fatalf("firstPoll = %#v", firstPoll)
	}
	envelope := firstPoll[0]
	if envelope.ID != "github-review:brokensbone/chatting:60:PRR_1" {
		t.Fatalf("envelope.ID = %q", envelope.ID)
	}
	if envelope.ReplyChannel.Target != "https://github.com/brokensbone/chatting/pull/60" {
		t.Fatalf("reply target = %q", envelope.ReplyChannel.Target)
	}
	if envelope.DedupeKey != "github-review:R_1:PR_1:PRR_1" {
		t.Fatalf("dedupe key = %q", envelope.DedupeKey)
	}
	if !strings.Contains(envelope.Content, "Linked issues: #60 Add ingress from GitHub reviews") {
		t.Fatalf("content = %q", envelope.Content)
	}
	if len(secondPoll) != 0 {
		t.Fatalf("secondPoll = %#v", secondPoll)
	}
}

func assignmentPayload(eventID string) map[string]any {
	return map[string]any{
		"data": map[string]any{
			"repository": map[string]any{
				"id":            "R_1",
				"nameWithOwner": "brokensbone/chatting",
				"issues": map[string]any{"nodes": []any{
					map[string]any{
						"id":     "I_1",
						"number": 12,
						"title":  "Plan milestone 5",
						"body":   "Body text",
						"url":    "https://github.com/brokensbone/chatting/issues/12",
						"labels": map[string]any{"nodes": []any{map[string]any{"name": "enhancement"}}},
						"timelineItems": map[string]any{"nodes": []any{
							map[string]any{
								"id":        eventID,
								"createdAt": "2026-03-07T10:47:35Z",
								"actor":     map[string]any{"login": "edward"},
								"assignee":  map[string]any{"__typename": "User", "login": "BillyAcachofa"},
							},
						}},
					},
				}},
			},
		},
	}
}

func reviewPayload(eventID string) map[string]any {
	return map[string]any{
		"data": map[string]any{
			"repository": map[string]any{
				"id":            "R_1",
				"nameWithOwner": "brokensbone/chatting",
				"pullRequests": map[string]any{"nodes": []any{
					map[string]any{
						"id":     "PR_1",
						"number": 60,
						"title":  "Add ingress from GitHub reviews",
						"body":   "PR body",
						"url":    "https://github.com/brokensbone/chatting/pull/60",
						"author": map[string]any{"login": "BillyAcachofa"},
						"closingIssuesReferences": map[string]any{"nodes": []any{
							map[string]any{"number": 60, "title": "Add ingress from GitHub reviews", "url": "https://github.com/brokensbone/chatting/issues/60"},
						}},
						"reviews": map[string]any{"nodes": []any{
							map[string]any{
								"id":          eventID,
								"submittedAt": "2026-03-07T12:00:00Z",
								"state":       "CHANGES_REQUESTED",
								"body":        "Please address the comments.",
								"url":         "https://github.com/brokensbone/chatting/pull/60#pullrequestreview-1",
								"author":      map[string]any{"login": "brokensbone"},
								"comments":    map[string]any{"totalCount": 2},
							},
						}},
					},
				}},
			},
		},
	}
}

func TestSelectEventsAfterCheckpointUsesUTCBoundary(t *testing.T) {
	first := mustTime(t, "2026-03-07T10:47:35Z")
	second := mustTime(t, "2026-03-07T10:48:35Z")
	events := []IssueAssignmentEvent{
		{EventID: "AE_2", EventCreatedAt: second},
		{EventID: "AE_1", EventCreatedAt: first},
	}
	selected := SelectEventsAfterCheckpoint(events, &AssignmentCheckpoint{
		EventCreatedAt: first,
		EventID:        "AE_1",
	})
	if len(selected) != 1 || selected[0].EventID != "AE_2" {
		t.Fatalf("selected = %#v", selected)
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
