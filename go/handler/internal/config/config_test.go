package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadAppliesDefaults(t *testing.T) {
	config, err := Load([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}

	expected := Defaults()
	if !reflect.DeepEqual(config, expected) {
		t.Fatalf("config = %#v, want %#v", config, expected)
	}
	if !reflect.DeepEqual(config.AllowedEgressChannels, []string{"email", "telegram", "telegram_reaction", "github", "log"}) {
		t.Fatalf("AllowedEgressChannels = %#v", config.AllowedEgressChannels)
	}
}

func TestMaintenanceUsesOneConfigurableRetentionWindow(t *testing.T) {
	if Defaults().RetentionDays != 14 {
		t.Fatal("expected 14-day default")
	}
	cfg, err := Load([]byte(`{"maintenance_cron":"0 4 * * *","retention_days":21}`))
	if err != nil || cfg.RetentionDays != 21 || cfg.MaintenanceCron != "0 4 * * *" {
		t.Fatalf("%+v %v", cfg, err)
	}
	for _, value := range []string{"0", "-1", "true"} {
		if _, err := Load([]byte(`{"retention_days":` + value + `}`)); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}

func TestMaintenanceIsDisabledWithoutCron(t *testing.T) {
	for _, payload := range []string{`{}`, `{"maintenance_cron":null}`} {
		cfg, err := Load([]byte(payload))
		if err != nil || cfg.MaintenanceCron != "" {
			t.Fatalf("%s: %+v %v", payload, cfg, err)
		}
	}
	for _, payload := range []string{`{"maintenance_cron":""}`, `{"maintenance_cron":true}`} {
		if _, err := Load([]byte(payload)); err == nil {
			t.Fatalf("accepted %s", payload)
		}
	}
}

func TestWebhookFilterConfiguration(t *testing.T) {
	defaults, err := Load([]byte(`{}`))
	if err != nil || defaults.GitHubWebhookLogin != "BillyAcachofa" || len(defaults.GitHubWebhookReleaseRepositories) != 0 {
		t.Fatalf("defaults: %+v %v", defaults, err)
	}
	c, err := Load([]byte(`{"github_webhook_login":"other-login","github_webhook_release_repositories":["o/r"]}`))
	if err != nil || c.GitHubWebhookLogin != "other-login" || !reflect.DeepEqual(c.GitHubWebhookReleaseRepositories, []string{"o/r"}) {
		t.Fatalf("config: %+v %v", c, err)
	}
}

func TestWebhookPort(t *testing.T) {
	for _, raw := range []string{`{"webhook_http_port":0}`, `{"webhook_http_port":9468}`} {
		cfg, err := Load([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(raw, "9468") && cfg.WebhookHTTPPort != 9468 {
			t.Fatal("port not loaded")
		}
		if !strings.Contains(raw, "9468") && cfg.WebhookHTTPPort != 0 {
			t.Fatal("listener should be disabled")
		}
	}
	for _, raw := range []string{`-1`, `65536`, `"9468"`, `1.5`} {
		if _, err := Load([]byte(`{"webhook_http_port":` + raw + `}`)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestLoadAcceptsMinimalRuntimeConfig(t *testing.T) {
	config, err := Load([]byte(`{
		"bbmb_address": "10.0.0.1:9999",
		"db_path": "/tmp/handler.db",
		"max_loops": 20,
		"poll_interval_seconds": 0.5,
		"metrics_host": "0.0.0.0",
		"metrics_port": 9555,
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

	if config.BBMBAddress != "10.0.0.1:9999" {
		t.Fatalf("BBMBAddress = %q", config.BBMBAddress)
	}
	if config.DBPath != "/tmp/handler.db" {
		t.Fatalf("DBPath = %q", config.DBPath)
	}
	if config.MaxLoops != 20 {
		t.Fatalf("MaxLoops = %d", config.MaxLoops)
	}
	if config.PollIntervalSeconds != 0.5 {
		t.Fatalf("PollIntervalSeconds = %f", config.PollIntervalSeconds)
	}
	if config.MetricsHost != "0.0.0.0" {
		t.Fatalf("MetricsHost = %q", config.MetricsHost)
	}
	if config.MetricsPort != 9555 {
		t.Fatalf("MetricsPort = %d", config.MetricsPort)
	}
	if !reflect.DeepEqual(config.AllowedEgressChannels, []string{"log", "email"}) {
		t.Fatalf("AllowedEgressChannels = %#v", config.AllowedEgressChannels)
	}
	if !reflect.DeepEqual(config.ContextRefs, []string{"repo:/workspace/chatting"}) {
		t.Fatalf("ContextRefs = %#v", config.ContextRefs)
	}
	if config.IMAPHost != "imap.example.com" {
		t.Fatalf("IMAPHost = %q", config.IMAPHost)
	}
	if config.IMAPPort != 143 {
		t.Fatalf("IMAPPort = %d", config.IMAPPort)
	}
	if config.IMAPUsername != "inbox@example.com" {
		t.Fatalf("IMAPUsername = %q", config.IMAPUsername)
	}
	if config.IMAPPasswordEnv != "IMAP_PASSWORD" {
		t.Fatalf("IMAPPasswordEnv = %q", config.IMAPPasswordEnv)
	}
	if config.IMAPMailbox != "Tasks" {
		t.Fatalf("IMAPMailbox = %q", config.IMAPMailbox)
	}
	if config.IMAPSearch != "ALL" {
		t.Fatalf("IMAPSearch = %q", config.IMAPSearch)
	}
	if config.IMAPUseSSL {
		t.Fatal("IMAPUseSSL = true")
	}
	if config.SMTPHost != "smtp.example.com" {
		t.Fatalf("SMTPHost = %q", config.SMTPHost)
	}
	if config.SMTPPort != 587 {
		t.Fatalf("SMTPPort = %d", config.SMTPPort)
	}
	if config.SMTPUsername != "bot@example.com" {
		t.Fatalf("SMTPUsername = %q", config.SMTPUsername)
	}
	if config.SMTPPasswordEnv != "SMTP_PASSWORD" {
		t.Fatalf("SMTPPasswordEnv = %q", config.SMTPPasswordEnv)
	}
	if config.SMTPFrom != "replies@example.com" {
		t.Fatalf("SMTPFrom = %q", config.SMTPFrom)
	}
	if !config.SMTPStartTLS {
		t.Fatal("SMTPStartTLS = false")
	}
	if config.SMTPUseSSL {
		t.Fatal("SMTPUseSSL = true")
	}
	if !config.TelegramEnabled {
		t.Fatal("TelegramEnabled = false")
	}
	if config.TelegramBotTokenEnv != "TELEGRAM_TOKEN" {
		t.Fatalf("TelegramBotTokenEnv = %q", config.TelegramBotTokenEnv)
	}
	if config.TelegramAPIBaseURL != "https://telegram.example.test" {
		t.Fatalf("TelegramAPIBaseURL = %q", config.TelegramAPIBaseURL)
	}
	if config.TelegramPollTimeoutSeconds != 12 {
		t.Fatalf("TelegramPollTimeoutSeconds = %d", config.TelegramPollTimeoutSeconds)
	}
	if !reflect.DeepEqual(config.TelegramAllowedChatIDs, []string{"12345"}) {
		t.Fatalf("TelegramAllowedChatIDs = %#v", config.TelegramAllowedChatIDs)
	}
	if !reflect.DeepEqual(config.TelegramAllowedChannelIDs, []string{"-100999"}) {
		t.Fatalf("TelegramAllowedChannelIDs = %#v", config.TelegramAllowedChannelIDs)
	}
	if config.TelegramAttachmentDir != "/tmp/telegram-attachments" {
		t.Fatalf("TelegramAttachmentDir = %q", config.TelegramAttachmentDir)
	}
	if !reflect.DeepEqual(config.TelegramContextRefs, []string{"repo:/workspace/telegram"}) {
		t.Fatalf("TelegramContextRefs = %#v", config.TelegramContextRefs)
	}
}

func TestLoadIgnoresAndReportsUnknownKeys(t *testing.T) {
	config, err := Load([]byte(`{"db_path": "/tmp/handler.db", "surprise": true, "another": [1]}`))
	if err != nil {
		t.Fatal(err)
	}
	if config.DBPath != "/tmp/handler.db" {
		t.Fatalf("DBPath = %q", config.DBPath)
	}
	if !reflect.DeepEqual(config.UnknownKeys, []string{"another", "surprise"}) {
		t.Fatalf("UnknownKeys = %#v", config.UnknownKeys)
	}

	config, err = Load([]byte(`{"db_path": "/tmp/handler.db"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(config.UnknownKeys) != 0 {
		t.Fatalf("UnknownKeys = %#v", config.UnknownKeys)
	}
}

func TestLoadTreatsNullAsMissing(t *testing.T) {
	config, err := Load([]byte(`{
		"bbmb_address": null,
		"db_path": null,
		"allowed_egress_channels": null
	}`))
	if err != nil {
		t.Fatal(err)
	}

	expected := Defaults()
	if config.BBMBAddress != expected.BBMBAddress || config.DBPath != expected.DBPath {
		t.Fatalf("config = %#v, want defaults %#v", config, expected)
	}
	if !reflect.DeepEqual(config.AllowedEgressChannels, expected.AllowedEgressChannels) {
		t.Fatalf("AllowedEgressChannels = %#v", config.AllowedEgressChannels)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		message string
	}{
		{
			name:    "blank string",
			raw:     `{"db_path": "  "}`,
			message: "config db_path must be a non-empty string",
		},
		{
			name:    "wrong string type",
			raw:     `{"bbmb_address": 12}`,
			message: "config bbmb_address must be a non-empty string",
		},
		{
			name:    "zero max loops",
			raw:     `{"max_loops": 0}`,
			message: "config max_loops must be a positive integer",
		},
		{
			name:    "negative interval",
			raw:     `{"poll_interval_seconds": -0.5}`,
			message: "config poll_interval_seconds must be positive",
		},
		{
			name:    "bool interval",
			raw:     `{"poll_interval_seconds": true}`,
			message: "config poll_interval_seconds must be numeric",
		},
		{
			name:    "bad allowed channels",
			raw:     `{"allowed_egress_channels": ["log", ""]}`,
			message: "allowed_egress_channel entries must not be empty",
		},
		{
			name:    "non-string allowed channels",
			raw:     `{"allowed_egress_channels": ["log", 5]}`,
			message: "config allowed_egress_channels must be a list of strings",
		},
		{
			name:    "imap without username",
			raw:     `{"imap_host": "imap.example.com"}`,
			message: "imap_username is required when imap_host is set",
		},
		{
			name:    "imap bad port",
			raw:     `{"imap_host": "imap.example.com", "imap_username": "bot@example.com", "imap_port": 0}`,
			message: "config imap_port must be a positive integer",
		},
		{
			name:    "imap ssl wrong type",
			raw:     `{"imap_use_ssl": "no"}`,
			message: "config imap_use_ssl must be a boolean",
		},
		{
			name:    "smtp without sender",
			raw:     `{"smtp_host": "smtp.example.com"}`,
			message: "smtp_from or smtp_username is required when smtp_host is set",
		},
		{
			name:    "smtp bad port",
			raw:     `{"smtp_host": "smtp.example.com", "smtp_from": "bot@example.com", "smtp_port": 0}`,
			message: "config smtp_port must be a positive integer",
		},
		{
			name:    "telegram enabled wrong type",
			raw:     `{"telegram_enabled": "yes"}`,
			message: "config telegram_enabled must be a boolean",
		},
		{
			name:    "blank telegram token env",
			raw:     `{"telegram_bot_token_env": ""}`,
			message: "config telegram_bot_token_env must be a non-empty string",
		},
		{
			name:    "smtp starttls wrong type",
			raw:     `{"smtp_starttls": "yes"}`,
			message: "config smtp_starttls must be a boolean",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load([]byte(tt.raw))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("error = %v, want %q", err, tt.message)
			}
		})
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

func TestLoadTreatsRemovedGitHubPollingKeysAsUnknown(t *testing.T) {
	// The shape a deployment that had already switched to webhooks still carries.
	config, err := Load([]byte(`{
		"github_repositories": [],
		"github_assignee_login": "billyacachofa"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(config.UnknownKeys, []string{"github_assignee_login", "github_repositories"}) {
		t.Fatalf("UnknownKeys = %#v", config.UnknownKeys)
	}
}
