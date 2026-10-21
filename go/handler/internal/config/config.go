package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/github"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/personalinbox"
)

const (
	DefaultBBMBAddress         = "127.0.0.1:9876"
	DefaultPollIntervalSeconds = 30.0
	DefaultMetricsHost         = "127.0.0.1"
	DefaultMetricsPort         = 9464
	DefaultEgressHTTPPort      = 9467
)

type Config struct {
	PersonalInbox               *personalinbox.Config
	BBMBAddress                 string
	DBPath                      string
	MaxLoops                    int
	PollIntervalSeconds         float64
	MetricsHost                 string
	MetricsPort                 int
	EgressHTTPPort              int
	WebhookHTTPPort             int
	EgressAttachmentAllowedDirs []string
	AllowedEgressChannels       []string
	ContextRefs                 []string
	IMAPHost                    string
	IMAPPort                    int
	IMAPUsername                string
	IMAPPasswordEnv             string
	IMAPMailbox                 string
	IMAPSearch                  string
	IMAPUseSSL                  bool
	SMTPHost                    string
	SMTPPort                    int
	SMTPUsername                string
	SMTPPasswordEnv             string
	SMTPFrom                    string
	SMTPStartTLS                bool
	SMTPUseSSL                  bool

	TelegramEnabled            bool
	TelegramBotTokenEnv        string
	TelegramAPIBaseURL         string
	TelegramPollTimeoutSeconds int
	TelegramAllowedChatIDs     []string
	TelegramAllowedChannelIDs  []string
	TelegramAttachmentDir      string
	TelegramContextRefs        []string

	GitHubRepositories  []string
	GitHubAssigneeLogin string
}

func Defaults() Config {
	return Config{
		BBMBAddress:                 DefaultBBMBAddress,
		DBPath:                      filepath.Join(os.TempDir(), "chatting-message-handler-state.db"),
		MaxLoops:                    0,
		PollIntervalSeconds:         DefaultPollIntervalSeconds,
		MetricsHost:                 DefaultMetricsHost,
		MetricsPort:                 DefaultMetricsPort,
		EgressHTTPPort:              DefaultEgressHTTPPort,
		EgressAttachmentAllowedDirs: []string{},
		AllowedEgressChannels:       []string{"email", "telegram", "telegram_reaction", "github", "log"},
		ContextRefs:                 []string{},
		IMAPHost:                    "",
		IMAPPort:                    993,
		IMAPUsername:                "",
		IMAPPasswordEnv:             "CHATTING_IMAP_PASSWORD",
		IMAPMailbox:                 "INBOX",
		IMAPSearch:                  "UNSEEN",
		IMAPUseSSL:                  true,
		SMTPHost:                    "",
		SMTPPort:                    465,
		SMTPUsername:                "",
		SMTPPasswordEnv:             "CHATTING_SMTP_PASSWORD",
		SMTPFrom:                    "",
		SMTPStartTLS:                false,
		SMTPUseSSL:                  true,

		TelegramEnabled:            false,
		TelegramBotTokenEnv:        "CHATTING_TELEGRAM_BOT_TOKEN",
		TelegramAPIBaseURL:         "https://api.telegram.org",
		TelegramPollTimeoutSeconds: 20,
		TelegramAllowedChatIDs:     []string{},
		TelegramAllowedChannelIDs:  []string{},
		TelegramAttachmentDir:      "",
		TelegramContextRefs:        []string{},

		GitHubRepositories:  []string{},
		GitHubAssigneeLogin: "",
	}
}

func LoadFile(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, errors.New("config path must not be empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return Load(raw)
}

func Load(raw []byte) (Config, error) {
	var payload map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return Config{}, err
	}
	if payload == nil {
		return Config{}, errors.New("config file must contain a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return Config{}, errors.New("config file must contain a single JSON object")
	} else if !errors.Is(err, io.EOF) {
		return Config{}, err
	}

	config := Defaults()
	fields := config.fields(payload)
	known := make(map[string]bool, len(fields))
	for _, field := range fields {
		known[field.key] = true
	}
	unknownKeys := make([]string, 0)
	for key := range payload {
		if !known[key] {
			unknownKeys = append(unknownKeys, key)
		}
	}
	if len(unknownKeys) > 0 {
		sort.Strings(unknownKeys)
		return Config{}, errors.New("config contains unknown keys: " + strings.Join(unknownKeys, ", "))
	}

	for _, field := range fields {
		rawValue, ok := payload[field.key]
		if !ok || (isNull(rawValue) && !field.keepNull) {
			continue
		}
		if err := field.decode(rawValue); err != nil {
			return Config{}, err
		}
	}
	if config.IMAPHost != "" {
		if config.IMAPUsername == "" {
			return Config{}, errors.New("imap_username is required when imap_host is set")
		}
		if config.IMAPPasswordEnv == "" {
			return Config{}, errors.New("imap_password_env is required when imap_host is set")
		}
	}
	if len(config.GitHubRepositories) > 0 && config.GitHubAssigneeLogin == "" {
		return Config{}, errors.New("github_assignee_login is required when github_repositories is set")
	}
	if config.SMTPHost != "" {
		if config.SMTPFrom == "" {
			config.SMTPFrom = config.SMTPUsername
		}
		if config.SMTPFrom == "" {
			return Config{}, errors.New("smtp_from or smtp_username is required when smtp_host is set")
		}
		if config.SMTPUsername != "" && config.SMTPPasswordEnv == "" {
			return Config{}, errors.New("smtp_password_env is required when smtp_username is set")
		}
	}
	return config, nil
}

type field struct {
	key    string
	decode func(json.RawMessage) error
	// keepNull hands an explicit null to decode instead of treating it as missing.
	keepNull bool
}

func bind[T any](dst *T, key string, decode func(json.RawMessage, string) (T, error)) field {
	return field{key: key, decode: func(raw json.RawMessage) error {
		value, err := decode(raw, key)
		if err != nil {
			return err
		}
		*dst = value
		return nil
	}}
}

func str(dst *string, key string) field       { return bind(dst, key, decodeNonEmptyString) }
func posInt(dst *int, key string) field       { return bind(dst, key, decodePositiveInt) }
func posFloat(dst *float64, key string) field { return bind(dst, key, decodePositiveFloat) }
func boolean(dst *bool, key string) field     { return bind(dst, key, decodeBool) }
func list(dst *[]string, key string) field    { return bind(dst, key, decodeStringList) }

// fields lists every accepted key in the order it is decoded, which is also
// the order validation errors are reported in.
func (c *Config) fields(payload map[string]json.RawMessage) []field {
	return []field{
		{key: "personal_inbox", keepNull: true, decode: func(raw json.RawMessage) error {
			parsed, err := personalinbox.Decode(raw)
			if err != nil {
				return fmt.Errorf("personal_inbox: %w", err)
			}
			c.PersonalInbox = parsed
			return nil
		}},
		str(&c.BBMBAddress, "bbmb_address"),
		str(&c.DBPath, "db_path"),
		posInt(&c.MaxLoops, "max_loops"),
		posFloat(&c.PollIntervalSeconds, "poll_interval_seconds"),
		str(&c.MetricsHost, "metrics_host"),
		posInt(&c.MetricsPort, "metrics_port"),
		posInt(&c.EgressHTTPPort, "egress_http_port"),
		bind(&c.WebhookHTTPPort, "webhook_http_port", func(raw json.RawMessage, name string) (int, error) {
			var port int
			err := json.Unmarshal(raw, &port)
			if err != nil || port < 0 || port > 65535 {
				return 0, fmt.Errorf("config %s must be an integer between 0 and 65535", name)
			}
			return port, err
		}),
		list(&c.EgressAttachmentAllowedDirs, "egress_attachment_allowed_dirs"),
		bind(&c.AllowedEgressChannels, "allowed_egress_channels", decodeAllowedEgressChannels),
		list(&c.ContextRefs, "context_refs"),
		str(&c.IMAPHost, "imap_host"),
		posInt(&c.IMAPPort, "imap_port"),
		str(&c.IMAPUsername, "imap_username"),
		str(&c.IMAPPasswordEnv, "imap_password_env"),
		str(&c.IMAPMailbox, "imap_mailbox"),
		str(&c.IMAPSearch, "imap_search"),
		boolean(&c.IMAPUseSSL, "imap_use_ssl"),
		str(&c.SMTPHost, "smtp_host"),
		posInt(&c.SMTPPort, "smtp_port"),
		str(&c.SMTPUsername, "smtp_username"),
		str(&c.SMTPPasswordEnv, "smtp_password_env"),
		str(&c.SMTPFrom, "smtp_from"),
		{key: "smtp_starttls", decode: func(raw json.RawMessage) error {
			if err := boolean(&c.SMTPStartTLS, "smtp_starttls").decode(raw); err != nil {
				return err
			}
			// Presence of the key, even as null, means the SSL choice was explicit.
			if _, ok := payload["smtp_use_ssl"]; !ok {
				c.SMTPUseSSL = !c.SMTPStartTLS
			}
			return nil
		}},
		boolean(&c.SMTPUseSSL, "smtp_use_ssl"),
		boolean(&c.TelegramEnabled, "telegram_enabled"),
		str(&c.TelegramBotTokenEnv, "telegram_bot_token_env"),
		str(&c.TelegramAPIBaseURL, "telegram_api_base_url"),
		posInt(&c.TelegramPollTimeoutSeconds, "telegram_poll_timeout_seconds"),
		list(&c.TelegramAllowedChatIDs, "telegram_allowed_chat_ids"),
		list(&c.TelegramAllowedChannelIDs, "telegram_allowed_channel_ids"),
		str(&c.TelegramAttachmentDir, "telegram_attachment_dir"),
		list(&c.TelegramContextRefs, "telegram_context_refs"),
		bind(&c.GitHubRepositories, "github_repositories", decodeGitHubRepositories),
		str(&c.GitHubAssigneeLogin, "github_assignee_login"),
	}
}

func isNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

func decodeNonEmptyString(raw json.RawMessage, name string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("config %s must be a non-empty string", name)
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("config %s must be a non-empty string", name)
	}
	return value, nil
}

func decodePositiveInt(raw json.RawMessage, name string) (int, error) {
	var value json.Number
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("config %s must be a positive integer", name)
	}
	parsed, err := strconv.ParseInt(value.String(), 10, 0)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("config %s must be a positive integer", name)
	}
	return int(parsed), nil
}

func decodePositiveFloat(raw json.RawMessage, name string) (float64, error) {
	var value json.Number
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("config %s must be numeric", name)
	}
	parsed, err := strconv.ParseFloat(value.String(), 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("config %s must be numeric", name)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("config %s must be positive", name)
	}
	return parsed, nil
}

func decodeAllowedEgressChannels(raw json.RawMessage, name string) ([]string, error) {
	values, err := decodeStringList(raw, name)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return Defaults().AllowedEgressChannels, nil
	}
	return values, nil
}

func decodeGitHubRepositories(raw json.RawMessage, name string) ([]string, error) {
	values, err := decodeStringList(raw, name)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		owner, name, err := github.ParseRepositoryPattern(value)
		if err != nil {
			return nil, errors.New("github_repositories entries must be owner/repo or owner/*")
		}
		normalized := owner + "/" + name
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		result = append(result, normalized)
	}
	return result, nil
}

func decodeStringList(raw json.RawMessage, name string) ([]string, error) {
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("config %s must be a list of strings", name)
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("%s entries must not be empty", singularListName(name))
		}
	}
	if values == nil {
		return []string{}, nil
	}
	return values, nil
}

func singularListName(name string) string {
	if name == "allowed_egress_channels" {
		return "allowed_egress_channel"
	}
	return name
}

func decodeBool(raw json.RawMessage, name string) (bool, error) {
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("config %s must be a boolean", name)
	}
	return value, nil
}
