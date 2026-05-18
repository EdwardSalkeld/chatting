package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/heartbeat"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
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
	if err := os.WriteFile(configPath, []byte(`{"db_path": "/tmp/handler.db", "max_loops": 1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	factory := &fakeRunnerFactory{}

	status := runWithFactory(context.Background(), []string{"--config", configPath}, &stdout, &stderr, nil, factory.newRunner)

	if status != 0 {
		t.Fatalf("status = %d", status)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if !factory.called {
		t.Fatal("runner factory was not called")
	}
	if factory.config.DBPath != "/tmp/handler.db" {
		t.Fatalf("db path = %q", factory.config.DBPath)
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
	if err := os.WriteFile(configPath, []byte(`{"db_path": "/tmp/handler.db", "max_loops": 1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	factory := &fakeRunnerFactory{}

	status := runWithFactory(
		context.Background(),
		nil,
		&stdout,
		&stderr,
		map[string]string{handlerconfig.MessageHandlerConfigPathEnvVar: configPath},
		factory.newRunner,
	)

	if status != 0 {
		t.Fatalf("status = %d", status)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunReportsRuntimeSetupError(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	factory := &fakeRunnerFactory{err: errors.New("boom")}

	status := runWithFactory(context.Background(), nil, &stdout, &stderr, nil, factory.newRunner)

	if status != 2 {
		t.Fatalf("status = %d", status)
	}
	if !strings.Contains(stderr.String(), "runtime setup error: boom") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunReportsRuntimeError(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	factory := &fakeRunnerFactory{runner: &fakeRunner{err: errors.New("failed")}}

	status := runWithFactory(context.Background(), nil, &stdout, &stderr, nil, factory.newRunner)

	if status != 1 {
		t.Fatalf("status = %d", status)
	}
	if !strings.Contains(stderr.String(), "runtime error: failed") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestUnsupportedDispatcherAcceptsInternalHeartbeatLogPong(t *testing.T) {
	body := `{"kind":"heartbeat_pong"}`
	message := contracts.OutboundMessage{
		Channel: "log",
		Target:  "heartbeat",
		Body:    &body,
	}

	dispatched, err := unsupportedDispatcher{}.Dispatch(
		context.Background(),
		message,
		heartbeat.BuildEnvelope(1, mustTime(t, "2026-03-09T12:00:00Z")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched == nil || dispatched.Channel != "log" || dispatched.Target != "heartbeat" {
		t.Fatalf("dispatched = %#v", dispatched)
	}
}

func TestBuildDispatcherRequiresSMTPPasswordEnvWhenUsernameConfigured(t *testing.T) {
	t.Setenv("MISSING_SMTP_PASSWORD", "")
	_, err := buildDispatcher(handlerconfig.Config{
		SMTPHost:        "smtp.example.com",
		SMTPPort:        587,
		SMTPUsername:    "bot@example.com",
		SMTPPasswordEnv: "MISSING_SMTP_PASSWORD",
		SMTPFrom:        "bot@example.com",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "missing SMTP password env var: MISSING_SMTP_PASSWORD") {
		t.Fatalf("error = %v", err)
	}
}

func TestBuildDispatcherRequiresTelegramTokenWhenEnabled(t *testing.T) {
	t.Setenv("MISSING_TELEGRAM_TOKEN", "")
	_, err := buildDispatcher(handlerconfig.Config{
		TelegramEnabled:     true,
		TelegramBotTokenEnv: "MISSING_TELEGRAM_TOKEN",
		TelegramAPIBaseURL:  "https://api.telegram.org",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "missing Telegram bot token env var: MISSING_TELEGRAM_TOKEN") {
		t.Fatalf("error = %v", err)
	}
}

type fakeRunnerFactory struct {
	called bool
	config handlerconfig.Config
	runner runner
	err    error
}

func (factory *fakeRunnerFactory) newRunner(ctx context.Context, config handlerconfig.Config) (runner, error) {
	factory.called = true
	factory.config = config
	if factory.err != nil {
		return nil, factory.err
	}
	if factory.runner != nil {
		return factory.runner, nil
	}
	return &fakeRunner{}, nil
}

type fakeRunner struct {
	err error
}

func (runner *fakeRunner) Run(ctx context.Context) error {
	return runner.err
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
