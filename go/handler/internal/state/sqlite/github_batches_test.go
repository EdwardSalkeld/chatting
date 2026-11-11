package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

func TestSuccessfulRunBatchesAreDurableAndOwnerSpecific(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.Close() }()
	owner := ciOwner(t, store, "-100")
	other := ciOwner(t, store, "-200")
	add := func(id int, taskID, sha, conclusion string) {
		t.Helper()
		url := fmt.Sprintf("https://github.com/o/r/actions/runs/%d", id)
		if err := store.RegisterRun(ctx, taskID, url); err != nil {
			t.Fatal(err)
		}
		ciReceive(t, store, ciBody(id, id, 1, sha, conclusion, "pull_request", "feature"), "registered_run_completed")
	}
	add(1, owner.TaskID, "abc", "success")
	add(2, owner.TaskID, "abc", "success")
	add(3, owner.TaskID, "def", "success")
	add(4, other.TaskID, "abc", "success")
	add(5, owner.TaskID, "abc", "failure")
	before := time.Now()
	ready, err := store.PendingGitHubBatched(ctx, before, time.Minute)
	if err != nil || len(ready) != 1 || ready[0].ReplyChannel.Metadata["conclusion"] != "failure" {
		t.Fatalf("failure must bypass batching: %v %v", ready, err)
	}
	if err := store.AckGitHub(ctx, ready[0].ID); err != nil {
		t.Fatal(err)
	}
	ready, err = store.PendingGitHubBatched(ctx, before.Add(2*time.Minute), time.Minute)
	if err != nil || len(ready) != 3 {
		t.Fatalf("expected three owner/commit groups: %v %v", ready, err)
	}
	var batch contracts.TaskEnvelope
	for _, e := range ready {
		if e.ReplyChannel.Metadata["completed_runs"] != nil {
			batch = e
		}
	}
	if batch.ID == "" {
		t.Fatal("missing batch")
	}
	assigned, err := store.AssignTask(ctx, contracts.NewTaskQueueMessage(batch, "batch", time.Now()))
	if err != nil || assigned.WorkItemID != owner.WorkItemID {
		t.Fatalf("wrong owner: %+v %v", assigned, err)
	}
	// Simulate failed publication / process restart, then a later run arrives.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	add(6, owner.TaskID, "abc", "success")
	ready, err = store.PendingGitHubBatched(ctx, time.Now(), time.Minute)
	if err != nil || len(ready) != 3 {
		t.Fatalf("frozen batches changed: %v %v", ready, err)
	}
	for _, e := range ready {
		if e.ReplyChannel.Metadata["completed_runs"] != nil && e.ID != batch.ID {
			t.Fatal("retry identity changed")
		}
		if err := store.AckGitHub(ctx, e.ID); err != nil {
			t.Fatal(err)
		}
	}
	ready, err = store.PendingGitHubBatched(ctx, time.Now().Add(2*time.Minute), time.Minute)
	if err != nil || len(ready) != 1 {
		t.Fatalf("later run lost: %v %v", ready, err)
	}
}
