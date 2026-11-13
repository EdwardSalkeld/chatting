package github

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultGraphQLRunnerTimesOutWhenGHHangs(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	previous := graphQLTimeout
	graphQLTimeout = 100 * time.Millisecond
	t.Cleanup(func() { graphQLTimeout = previous })

	started := time.Now()
	_, err := DefaultGraphQLRunner(context.Background(), "query { viewer { login } }", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("returned after %s", elapsed)
	}
}
