// Package personalinbox is the handler-owned personal mailbox workflow. It never
// publishes personal mail to the general-purpose executor queue.
package personalinbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
)

const TasksQueue = "chatting.personal-inbox.tasks.v1"
const ResultsQueue = "chatting.personal-inbox.results.v1"
const DeadQueue = "chatting.personal-inbox.invalid.v1"

// Categories is every classification the workflow understands. Anything the
// model returns outside this set is rejected.
var Categories = []string{"news", "order", "action", "review"}

const DefaultLinkTemplate = "https://app.fastmail.com/mail/{folder}/{thread}.{email}?u={account}"

type Config struct {
	Enabled    bool   `json:"enabled"`
	DBPath     string `json:"db_path"`
	Account    string `json:"account"`
	TokenEnv   string `json:"jmap_token_env"`
	SessionURL string `json:"jmap_session_url"`
	Backfill   bool   `json:"backfill"`
	DryRun     bool   `json:"dry_run"`
	// Folders maps a category to the mailbox its mail is filed into. A missing
	// or empty entry leaves that category in the inbox, which is how review
	// stays visible as the queue of mail the workflow did not understand.
	Folders map[string]string `json:"folders"`
	// MarkRead lists the categories filed as already read. Action mail is
	// deliberately left unread even once it has been filed.
	MarkRead      []string `json:"mark_read"`
	LinkTemplate  string   `json:"mail_link_template"`
	OrdersTarget  string   `json:"orders_telegram_target"`
	ActionTarget  string   `json:"action_telegram_target"`
	DigestTarget  string   `json:"digest_telegram_target"`
	DigestSeconds int      `json:"digest_interval_seconds"`
	LinearKeyEnv  string   `json:"linear_key_env"`
	LinearTeam    string   `json:"linear_team_id"`
	LinearProject string   `json:"linear_project_id"`
}

func Decode(raw []byte) (*Config, error) {
	config := Config{DryRun: true, DigestSeconds: 86400, LinkTemplate: DefaultLinkTemplate,
		TokenEnv: "CHATTING_PERSONAL_JMAP_TOKEN", LinearKeyEnv: "CHATTING_PERSONAL_LINEAR_KEY"}
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

func safeValue(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.ContainsAny(value, "\r\n\x00")
}

func (config Config) Validate() error {
	if !config.Enabled {
		return nil
	}
	for _, value := range []string{config.DBPath, config.Account, config.TokenEnv, config.OrdersTarget,
		config.ActionTarget, config.DigestTarget, config.LinearKeyEnv, config.LinearTeam} {
		if !safeValue(value) {
			return errors.New("personal_inbox requires database, account, JMAP token, Telegram destinations and Linear team")
		}
	}
	if config.DigestSeconds < 1 {
		return errors.New("invalid personal_inbox digest interval")
	}
	for category, folder := range config.Folders {
		if !slices.Contains(Categories, category) {
			return errors.New("personal_inbox folders may only name known categories")
		}
		if strings.TrimSpace(folder) != "" && !safeValue(folder) {
			return errors.New("personal_inbox folder names must not contain control characters")
		}
	}
	for _, category := range config.MarkRead {
		if !slices.Contains(Categories, category) {
			return errors.New("personal_inbox mark_read may only name known categories")
		}
	}
	if config.SessionURL != "" && !strings.HasPrefix(config.SessionURL, "https://") {
		return errors.New("personal_inbox jmap_session_url must be https")
	}
	if config.LinkTemplate != "" && !strings.HasPrefix(config.LinkTemplate, "https://") {
		return errors.New("personal_inbox mail_link_template must be https")
	}
	return nil
}

// Folder is the mailbox a category is filed into, or "" to leave it in place.
func (config Config) Folder(category string) string {
	return strings.TrimSpace(config.Folders[category])
}

func (config Config) MarksRead(category string) bool {
	return slices.Contains(config.MarkRead, category)
}
