package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	handlerconfig "github.com/EdwardSalkeld/chatting/go/handler/internal/config"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/dispatch"
)

func TestRunRequiresConfigFlag(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	status := runWithFactory(context.Background(), nil, &stdout, &stderr, newRuntimeRunner)

	if status != 2 {
		t.Fatalf("status = %d", status)
	}
	if !strings.Contains(stderr.String(), "config path must not be empty") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestShouldNotifyEgressDrop(t *testing.T) {
	for _, test := range []struct {
		reason string
		want   bool
	}{
		{reason: "telegram_attachment_path_not_allowed", want: false},
		{reason: " telegram_attachment_missing ", want: false},
		{reason: "telegram_dispatch_failed", want: true},
		{reason: "disallowed_channel", want: true},
	} {
		if got := shouldNotifyEgressDrop(test.reason); got != test.want {
			t.Errorf("shouldNotifyEgressDrop(%q) = %v, want %v", test.reason, got, test.want)
		}
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

	status := runWithFactory(context.Background(), []string{"--config", configPath}, &stdout, &stderr, factory.newRunner)

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

func TestRunReportsFailures(t *testing.T) {
	for _, tc := range []struct {
		name, config, message string
		factory               fakeRunnerFactory
		status                int
	}{
		{name: "config", config: `{"db_path":""}`, message: "config error: config db_path must be a non-empty string", status: 2},
		{name: "setup", config: `{"db_path":"/tmp/handler.db"}`, factory: fakeRunnerFactory{err: errors.New("boom")}, message: "runtime setup error: boom", status: 2},
		{name: "runtime", config: `{"db_path":"/tmp/handler.db"}`, factory: fakeRunnerFactory{runner: &fakeRunner{err: errors.New("failed")}}, message: "runtime error: failed", status: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "handler.json")
			if err := os.WriteFile(path, []byte(tc.config), 0o600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			if status := runWithFactory(context.Background(), []string{"--config", path}, &stdout, &stderr, tc.factory.newRunner); status != tc.status || !strings.Contains(stderr.String(), tc.message) {
				t.Fatalf("status=%d stderr=%q", status, stderr.String())
			}
		})
	}
}

func TestBuildDispatcherRequiresSMTPPasswordEnvWhenUsernameConfigured(t *testing.T) {
	t.Setenv("MISSING_SMTP_PASSWORD", "")
	_, _, err := buildDispatcher(handlerconfig.Config{
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
	_, _, err := buildDispatcher(handlerconfig.Config{
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

func TestBuildDispatcherGitHubAvailability(t *testing.T) {
	originalLookPath := ghLookPath
	t.Cleanup(func() { ghLookPath = originalLookPath })
	for _, available := range []bool{true, false} {
		ghLookPath = func(file string) (string, error) {
			if file != "gh" {
				t.Fatalf("unexpected binary lookup = %q", file)
			}
			if available {
				return "/usr/bin/gh", nil
			}
			return "", exec.ErrNotFound
		}
		dispatcher, _, err := buildDispatcher(handlerconfig.Config{})
		if err != nil {
			t.Fatal(err)
		}
		typed, ok := dispatcher.(dispatch.Dispatcher)
		if !ok || (typed.GitHubSender != nil) != available {
			t.Fatalf("available=%t dispatcher=%#v", available, dispatcher)
		}
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
