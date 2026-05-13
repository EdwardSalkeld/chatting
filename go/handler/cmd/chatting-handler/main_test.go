package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
)

func TestRunPrintsVersion(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	status := run([]string{"--version"}, &stdout, &stderr, nil)

	if status != 0 {
		t.Fatalf("status = %d", status)
	}
	if strings.TrimSpace(stdout.String()) != version {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunParsesConfigFlagBeforeBootstrapExit(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "handler.json")
	if err := os.WriteFile(configPath, []byte(`{"db_path": "/tmp/handler.db"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	status := run([]string{"--config", configPath}, &stdout, &stderr, nil)

	if status != 2 {
		t.Fatalf("status = %d", status)
	}
	if !strings.Contains(stderr.String(), "runtime not implemented yet") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if strings.Contains(stderr.String(), "config error") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "handler.json")
	if err := os.WriteFile(configPath, []byte(`{"db_path": ""}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	status := run([]string{"--config", configPath}, &stdout, &stderr, nil)

	if status != 2 {
		t.Fatalf("status = %d", status)
	}
	if !strings.Contains(stderr.String(), "config error: config db_path must be a non-empty string") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if strings.Contains(stderr.String(), "runtime not implemented yet") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunUsesConfigPathFromEnvironment(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "handler.json")
	if err := os.WriteFile(configPath, []byte(`{"db_path": "/tmp/handler.db"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	status := run(
		nil,
		&stdout,
		&stderr,
		map[string]string{handlerconfig.MessageHandlerConfigPathEnvVar: configPath},
	)

	if status != 2 {
		t.Fatalf("status = %d", status)
	}
	if !strings.Contains(stderr.String(), "runtime not implemented yet") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
