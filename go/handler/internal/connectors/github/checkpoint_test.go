package github

import (
	"testing"
	"time"
)

type testCheckpointEvent struct {
	id        string
	createdAt time.Time
}

func (event testCheckpointEvent) CheckpointEventID() string {
	return event.id
}

func (event testCheckpointEvent) CheckpointEventCreatedAt() time.Time {
	return event.createdAt
}

func TestCheckpointScopeKeyMatchesPythonFormat(t *testing.T) {
	scopeKey, err := CheckpointScopeKey(
		[]string{"EdwardSalkeld/workout-data", "EdwardSalkeld/chatting"},
		"BillyAcachofa",
		"reviews",
	)
	if err != nil {
		t.Fatal(err)
	}
	if scopeKey != "reviews:billyacachofa:EdwardSalkeld/chatting,EdwardSalkeld/workout-data" {
		t.Fatalf("scopeKey = %q", scopeKey)
	}
}

func TestCheckpointScopeKeyDefaultsValidateRequiredInputs(t *testing.T) {
	if _, err := CheckpointScopeKey(nil, "BillyAcachofa", "assignments"); err == nil || err.Error() != "repositories are required" {
		t.Fatalf("missing repositories err = %v", err)
	}
	if _, err := CheckpointScopeKey([]string{"EdwardSalkeld/chatting"}, "BillyAcachofa", " "); err == nil || err.Error() != "stream_name is required" {
		t.Fatalf("missing stream err = %v", err)
	}
}

func TestSelectEventsAfterCheckpointSortsDedupesAndFiltersBoundary(t *testing.T) {
	first := mustCheckpointTime(t, "2026-05-30T12:00:00Z")
	second := mustCheckpointTime(t, "2026-05-30T12:01:00Z")
	events := []testCheckpointEvent{
		{id: "evt-3", createdAt: second},
		{id: "evt-1", createdAt: first},
		{id: "evt-2", createdAt: first},
		{id: "evt-2", createdAt: first},
	}

	selected := SelectEventsAfterCheckpoint(events, &AssignmentCheckpoint{
		EventCreatedAt: first,
		EventID:        "evt-1",
	})

	if len(selected) != 2 || selected[0].id != "evt-2" || selected[1].id != "evt-3" {
		t.Fatalf("selected = %#v", selected)
	}
}

func TestSelectEventsAfterCheckpointReturnsOrderedEventsWithoutCheckpoint(t *testing.T) {
	first := mustCheckpointTime(t, "2026-05-30T12:00:00Z")
	second := mustCheckpointTime(t, "2026-05-30T12:01:00Z")
	events := []testCheckpointEvent{
		{id: "evt-3", createdAt: second},
		{id: "evt-1", createdAt: first},
	}

	selected := SelectEventsAfterCheckpoint(events, nil)

	if len(selected) != 2 || selected[0].id != "evt-1" || selected[1].id != "evt-3" {
		t.Fatalf("selected = %#v", selected)
	}
}

func mustCheckpointTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
