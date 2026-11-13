package dispatch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultGitHubCommandRunnerTimesOutWhenGHHangs(t *testing.T) {
	stub := filepath.Join(t.TempDir(), "gh")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	previous := gitHubCommandTimeout
	gitHubCommandTimeout = 100 * time.Millisecond
	t.Cleanup(func() { gitHubCommandTimeout = previous })

	started := time.Now()
	err := defaultGitHubCommandRunner(context.Background(), []string{stub, "issue", "comment"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("returned after %s", elapsed)
	}
}
