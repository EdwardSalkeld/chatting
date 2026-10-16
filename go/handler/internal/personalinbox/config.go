// Package personalinbox is the handler-owned personal mailbox workflow. It never
// publishes personal mail to the general-purpose executor queue.
package personalinbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

const TasksQueue = "chatting.personal-inbox.tasks.v1"
const ResultsQueue = "chatting.personal-inbox.results.v1"
const DeadQueue = "chatting.personal-inbox.invalid.v1"

// Categories is every classification the workflow understands. Anything the
// model returns outside this set is rejected.
var Categories = []string{"news", "order", "action", "review"}

const DefaultLinkTemplate = "https://app.fastmail.com/mail/{folder}/{thread}.{email}?u={account}"

const DefaultConfidenceThreshold = 0.90

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
	MarkRead     []string `json:"mark_read"`
	LinkTemplate string   `json:"mail_link_template"`
	OrdersTarget string   `json:"orders_telegram_target"`
	ActionTarget string   `json:"action_telegram_target"`
	DigestTarget string   `json:"digest_telegram_target"`
	// DigestAt is a wall-clock time of day, "HH:MM" in Timezone. The digest is
	// a daily round-up at a time the reader chose, not a per-message delay.
	DigestAt string `json:"digest_at"`
	Timezone string `json:"timezone"`
	// ConfidenceThreshold is the level below which a classification is held
	// for review instead of acted on. Real mail has a long uncertain tail, so
	// this is the dial for how much of it to accept.
	ConfidenceThreshold float64 `json:"confidence_threshold"`
	LinearKeyEnv        string  `json:"linear_key_env"`
	LinearTeam          string  `json:"linear_team_id"`
	LinearProject       string  `json:"linear_project_id"`
}

// Location is the timezone the digest time is read in.
func (config Config) Location() (*time.Location, error) {
	if strings.TrimSpace(config.Timezone) == "" {
		return time.UTC, nil
	}
	return time.LoadLocation(config.Timezone)
}

// DigestTime is the hour and minute the daily digest is sent.
func (config Config) DigestTime() (int, int, error) {
	parsed, err := time.Parse("15:04", strings.TrimSpace(config.DigestAt))
	if err != nil {
		return 0, 0, errors.New("digest_at must be a wall-clock time such as \"08:00\"")
	}
	return parsed.Hour(), parsed.Minute(), nil
}

func Decode(raw []byte) (*Config, error) {
	config := Config{DryRun: true, DigestAt: "08:00", Timezone: "UTC", LinkTemplate: DefaultLinkTemplate,
		ConfidenceThreshold: DefaultConfidenceThreshold,
		TokenEnv:            "CHATTING_PERSONAL_JMAP_TOKEN", LinearKeyEnv: "CHATTING_PERSONAL_LINEAR_KEY"}
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
		config.ActionTarget, config.DigestTarget} {
		if !safeValue(value) {
			return errors.New("personal_inbox requires database, account, JMAP token and Telegram destinations")
		}
	}
	// Ticketing is opt-in. Configuring a team without somewhere to read its key
	// from is a half-configuration rather than a choice, so it is refused.
	if config.LinearTeam != "" && !safeValue(config.LinearKeyEnv) {
		return errors.New("personal_inbox linear_team_id needs linear_key_env")
	}
	if config.LinearTeam != "" && !safeValue(config.LinearTeam) {
		return errors.New("personal_inbox linear_team_id must not contain control characters")
	}
	if _, _, err := config.DigestTime(); err != nil {
		return err
	}
	if config.ConfidenceThreshold <= 0 || config.ConfidenceThreshold > 1 {
		return errors.New("personal_inbox confidence_threshold must be above 0 and at most 1")
	}
	if _, err := config.Location(); err != nil {
		return errors.New("personal_inbox timezone is not a known IANA location")
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

// Threshold is the confidence a classification needs to be acted on.
func (config Config) Threshold() float64 {
	if config.ConfidenceThreshold <= 0 || config.ConfidenceThreshold > 1 {
		return DefaultConfidenceThreshold
	}
	return config.ConfidenceThreshold
}

// CreatesTickets reports whether action mail also opens a Linear issue. With
// no team configured it is simply notified and filed.
func (config Config) CreatesTickets() bool {
	return strings.TrimSpace(config.LinearTeam) != ""
}

func (config Config) MarksRead(category string) bool {
	return slices.Contains(config.MarkRead, category)
}
