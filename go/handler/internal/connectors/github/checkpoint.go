package github

import (
	"errors"
	"sort"
	"strings"
	"time"
)

type AssignmentCheckpoint struct {
	EventCreatedAt time.Time
	EventID        string
}

type CheckpointedEvent interface {
	CheckpointEventID() string
	CheckpointEventCreatedAt() time.Time
}

func CheckpointScopeKey(repositories []string, assigneeLogin string, streamName string) (string, error) {
	if len(repositories) == 0 {
		return "", errors.New("repositories are required")
	}
	if strings.TrimSpace(streamName) == "" {
		return "", errors.New("stream_name is required")
	}
	normalizedRepositories := make([]string, 0, len(repositories))
	for _, repository := range repositories {
		normalizedRepositories = append(normalizedRepositories, strings.TrimSpace(repository))
	}
	sort.Strings(normalizedRepositories)
	return strings.ToLower(strings.TrimSpace(streamName)) + ":" +
		strings.ToLower(assigneeLogin) + ":" +
		strings.Join(normalizedRepositories, ","), nil
}

func SelectEventsAfterCheckpoint[T CheckpointedEvent](events []T, checkpoint *AssignmentCheckpoint) []T {
	uniqueByID := map[string]T{}
	for _, event := range events {
		uniqueByID[event.CheckpointEventID()] = event
	}
	ordered := make([]T, 0, len(uniqueByID))
	for _, event := range uniqueByID {
		ordered = append(ordered, event)
	}
	sort.Slice(ordered, func(leftIndex, rightIndex int) bool {
		left := ordered[leftIndex]
		right := ordered[rightIndex]
		leftCreatedAt := left.CheckpointEventCreatedAt().UTC()
		rightCreatedAt := right.CheckpointEventCreatedAt().UTC()
		if !leftCreatedAt.Equal(rightCreatedAt) {
			return leftCreatedAt.Before(rightCreatedAt)
		}
		return left.CheckpointEventID() < right.CheckpointEventID()
	})
	if checkpoint == nil {
		return ordered
	}
	boundaryCreatedAt := checkpoint.EventCreatedAt.UTC()
	filtered := make([]T, 0, len(ordered))
	for _, event := range ordered {
		eventCreatedAt := event.CheckpointEventCreatedAt().UTC()
		if eventCreatedAt.After(boundaryCreatedAt) ||
			(eventCreatedAt.Equal(boundaryCreatedAt) && event.CheckpointEventID() > checkpoint.EventID) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}
