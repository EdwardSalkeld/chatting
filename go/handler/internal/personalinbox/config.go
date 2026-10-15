// Package personalinbox is the handler-owned personal mailbox workflow. It never
// publishes personal mail to the general-purpose executor queue.
package personalinbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

const TasksQueue = "chatting.personal-inbox.tasks.v1"
const ResultsQueue = "chatting.personal-inbox.results.v1"
const DeadQueue = "chatting.personal-inbox.invalid.v1"

type Config struct {
	Enabled       bool   `json:"enabled"`
	DBPath        string `json:"db_path"`
	Account       string `json:"account"`
	Host          string `json:"imap_host"`
	Port          int    `json:"imap_port"`
	Username      string `json:"imap_username"`
	PasswordEnv   string `json:"imap_password_env"`
	Mailbox       string `json:"imap_mailbox"`
	Backfill      bool   `json:"backfill"`
	DryRun        bool   `json:"dry_run"`
	MoveNews      bool   `json:"move_news"`
	MarkNewsRead  bool   `json:"mark_news_read"`
	NewsFolder    string `json:"news_folder"`
	OrdersTarget  string `json:"orders_telegram_target"`
	ActionTarget  string `json:"action_telegram_target"`
	DigestTarget  string `json:"digest_telegram_target"`
	DigestSeconds int    `json:"digest_interval_seconds"`
	LinearKeyEnv  string `json:"linear_key_env"`
	LinearTeam    string `json:"linear_team_id"`
	LinearProject string `json:"linear_project_id"`
}

func Decode(raw []byte) (*Config, error) {
	config := Config{Port: 993, Mailbox: "INBOX", DryRun: true, DigestSeconds: 86400,
		PasswordEnv: "CHATTING_PERSONAL_IMAP_PASSWORD", LinearKeyEnv: "CHATTING_PERSONAL_LINEAR_KEY"}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (config Config) Validate() error {
	if !config.Enabled {
		return nil
	}
	for _, value := range []string{config.DBPath, config.Account, config.Host, config.Username,
		config.Mailbox, config.PasswordEnv, config.OrdersTarget, config.ActionTarget, config.DigestTarget,
		config.LinearKeyEnv, config.LinearTeam} {
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("personal_inbox requires database, account, IMAP, Telegram destinations and Linear team")
		}
	}
	if config.Port < 1 || config.Port > 65535 || config.DigestSeconds < 1 {
		return errors.New("invalid personal_inbox port or digest interval")
	}
	if config.MoveNews && (strings.TrimSpace(config.NewsFolder) == "" || strings.EqualFold(config.NewsFolder, config.Mailbox) || strings.ContainsAny(config.NewsFolder, "\r\n\x00")) {
		return errors.New("news_folder must be a different approved mailbox")
	}
	return nil
}
