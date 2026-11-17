package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadAppliesDefaults(t *testing.T) {
	for _, payload := range []string{`{}`, `{"bbmb_address": null, "db_path": null, "allowed_egress_channels": null, "maintenance_cron": null}`} {
		config, err := Load([]byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(config, Defaults()) {
			t.Fatalf("%s: config = %#v, want %#v", payload, config, Defaults())
		}
	}
}

func TestLoadMapsEveryKey(t *testing.T) {
	config, err := Load([]byte(`{
		"maintenance_cron": "0 4 * * *",
		"retention_days": 21,
		"bbmb_address": "10.0.0.1:9999",
		"db_path": "/tmp/handler.db",
		"max_loops": 20,
		"poll_interval_seconds": 0.5,
		"metrics_host": "0.0.0.0",
		"metrics_port": 9555,
		"webhook_http_port": 0,
		"github_webhook_login": "other-login",
		"github_webhook_release_repositories": ["o/r"],
		"allowed_egress_channels": ["log", "email"],
		"context_refs": ["repo:/workspace/chatting"],
		"imap_host": "imap.example.com",
		"imap_port": 143,
		"imap_username": "inbox@example.com",
		"imap_password_env": "IMAP_PASSWORD",
		"imap_mailbox": "Tasks",
		"imap_search": "ALL",
		"imap_use_ssl": false,
		"smtp_host": "smtp.example.com",
		"smtp_port": 587,
		"smtp_username": "bot@example.com",
		"smtp_password_env": "SMTP_PASSWORD",
		"smtp_from": "replies@example.com",
		"smtp_starttls": true,
		"smtp_use_ssl": false,
		"telegram_enabled": true,
		"telegram_bot_token_env": "TELEGRAM_TOKEN",
		"telegram_api_base_url": "https://telegram.example.test",
		"telegram_poll_timeout_seconds": 12,
		"telegram_allowed_chat_ids": ["12345"],
		"telegram_allowed_channel_ids": ["-100999"],
		"telegram_attachment_dir": "/tmp/telegram-attachments",
		"telegram_context_refs": ["repo:/workspace/telegram"]
	}`))
	if err != nil {
		t.Fatal(err)
	}

	want := Defaults()
	want.MaintenanceCron = "0 4 * * *"
	want.RetentionDays = 21
	want.BBMBAddress = "10.0.0.1:9999"
	want.DBPath = "/tmp/handler.db"
	want.MaxLoops = 20
	want.PollIntervalSeconds = 0.5
	want.MetricsHost = "0.0.0.0"
	want.MetricsPort = 9555
	want.WebhookHTTPPort = 0
	want.GitHubWebhookLogin = "other-login"
	want.GitHubWebhookReleaseRepositories = []string{"o/r"}
	want.AllowedEgressChannels = []string{"log", "email"}
	want.ContextRefs = []string{"repo:/workspace/chatting"}
	want.IMAPHost = "imap.example.com"
	want.IMAPPort = 143
	want.IMAPUsername = "inbox@example.com"
	want.IMAPPasswordEnv = "IMAP_PASSWORD"
	want.IMAPMailbox = "Tasks"
	want.IMAPSearch = "ALL"
	want.IMAPUseSSL = false
	want.SMTPHost = "smtp.example.com"
	want.SMTPPort = 587
	want.SMTPUsername = "bot@example.com"
	want.SMTPPasswordEnv = "SMTP_PASSWORD"
	want.SMTPFrom = "replies@example.com"
	want.SMTPStartTLS = true
	want.SMTPUseSSL = false
	want.TelegramEnabled = true
	want.TelegramBotTokenEnv = "TELEGRAM_TOKEN"
	want.TelegramAPIBaseURL = "https://telegram.example.test"
	want.TelegramPollTimeoutSeconds = 12
	want.TelegramAllowedChatIDs = []string{"12345"}
	want.TelegramAllowedChannelIDs = []string{"-100999"}
	want.TelegramAttachmentDir = "/tmp/telegram-attachments"
	want.TelegramContextRefs = []string{"repo:/workspace/telegram"}
	if !reflect.DeepEqual(config, want) {
		t.Fatalf("config = %#v, want %#v", config, want)
	}
}

func TestLoadIgnoresAndReportsUnknownKeys(t *testing.T) {
	config, err := Load([]byte(`{"db_path": "/tmp/handler.db", "surprise": true, "another": [1]}`))
	if err != nil {
		t.Fatal(err)
	}
	if config.DBPath != "/tmp/handler.db" || !reflect.DeepEqual(config.UnknownKeys, []string{"another", "surprise"}) {
		t.Fatalf("config = %#v", config)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		raw     string
		message string
	}{
		{`{"db_path": "  "}`, "config db_path must be a non-empty string"},
		{`{"bbmb_address": 12}`, "config bbmb_address must be a non-empty string"},
		{`{"maintenance_cron": ""}`, "config maintenance_cron must be a non-empty string"},
		{`{"max_loops": 0}`, "config max_loops must be a positive integer"},
		{`{"retention_days": true}`, "config retention_days must be a positive integer"},
		{`{"poll_interval_seconds": -0.5}`, "config poll_interval_seconds must be positive"},
		{`{"poll_interval_seconds": true}`, "config poll_interval_seconds must be numeric"},
		{`{"webhook_http_port": 65536}`, "config webhook_http_port must be an integer between 0 and 65535"},
		{`{"webhook_http_port": "9468"}`, "config webhook_http_port must be an integer between 0 and 65535"},
		{`{"allowed_egress_channels": ["log", ""]}`, "allowed_egress_channel entries must not be empty"},
		{`{"allowed_egress_channels": ["log", 5]}`, "config allowed_egress_channels must be a list of strings"},
		{`{"imap_host": "imap.example.com"}`, "imap_username is required when imap_host is set"},
		{`{"smtp_host": "smtp.example.com"}`, "smtp_from or smtp_username is required when smtp_host is set"},
		{`{"smtp_starttls": "yes"}`, "config smtp_starttls must be a boolean"},
	}

	for _, tt := range tests {
		_, err := Load([]byte(tt.raw))
		if err == nil || !strings.Contains(err.Error(), tt.message) {
			t.Errorf("%s: error = %v, want %q", tt.raw, err, tt.message)
		}
	}
}

func TestLoadFile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "handler.json")
	if err := os.WriteFile(configPath, []byte(`{"db_path": "/tmp/from-file.db"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if config.DBPath != "/tmp/from-file.db" {
		t.Fatalf("DBPath = %q", config.DBPath)
	}
	if _, err := LoadFile(" "); err == nil {
		t.Fatal("expected error for a blank path")
	}
}
