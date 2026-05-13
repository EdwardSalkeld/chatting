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
)

const (
	MessageHandlerConfigPathEnvVar = "CHATTING_MESSAGE_HANDLER_CONFIG_PATH"
	DefaultBBMBAddress             = "127.0.0.1:9876"
	DefaultPollIntervalSeconds     = 30.0
	DefaultPollTimeoutSeconds      = 2
	DefaultMetricsHost             = "127.0.0.1"
	DefaultMetricsPort             = 9464
)

var allowedKeys = map[string]bool{
	"bbmb_address":            true,
	"db_path":                 true,
	"max_loops":               true,
	"poll_interval_seconds":   true,
	"poll_timeout_seconds":    true,
	"metrics_host":            true,
	"metrics_port":            true,
	"allowed_egress_channels": true,
	"global_prompt_context":   true,
	"cron_prompt_context":     true,
	"schedule_file":           true,

	"auxiliary_ingress_enabled":      true,
	"auxiliary_ingress_bbmb_address": true,
	"auxiliary_ingress_queues":       true,
	"auxiliary_ingress_context_refs": true,
}

type Config struct {
	BBMBAddress           string
	DBPath                string
	MaxLoops              int
	PollIntervalSeconds   float64
	PollTimeoutSeconds    int
	MetricsHost           string
	MetricsPort           int
	AllowedEgressChannels []string
	GlobalPromptContext   []string
	CronPromptContext     []string
	ScheduleFile          string

	AuxiliaryIngressEnabled     bool
	AuxiliaryIngressBBMBAddress string
	AuxiliaryIngressQueues      []string
	AuxiliaryIngressContextRefs []string
}

func Defaults() Config {
	return Config{
		BBMBAddress:           DefaultBBMBAddress,
		DBPath:                filepath.Join(os.TempDir(), "chatting-message-handler-state.db"),
		MaxLoops:              0,
		PollIntervalSeconds:   DefaultPollIntervalSeconds,
		PollTimeoutSeconds:    DefaultPollTimeoutSeconds,
		MetricsHost:           DefaultMetricsHost,
		MetricsPort:           DefaultMetricsPort,
		AllowedEgressChannels: []string{"email", "telegram", "telegram_reaction", "log"},
		GlobalPromptContext:   []string{},
		CronPromptContext:     []string{},
		ScheduleFile:          "",

		AuxiliaryIngressEnabled:     false,
		AuxiliaryIngressBBMBAddress: "",
		AuxiliaryIngressQueues:      []string{},
		AuxiliaryIngressContextRefs: []string{},
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

func LoadFromEnv(configPath string, environ map[string]string) (Config, error) {
	path := configPath
	if path == "" && environ != nil {
		if raw, ok := environ[MessageHandlerConfigPathEnvVar]; ok {
			if strings.TrimSpace(raw) == "" {
				return Config{}, fmt.Errorf("%s must not be empty", MessageHandlerConfigPathEnvVar)
			}
			path = raw
		}
	}
	if path == "" {
		return Defaults(), nil
	}
	return LoadFile(path)
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

	unknownKeys := make([]string, 0)
	for key := range payload {
		if !allowedKeys[key] {
			unknownKeys = append(unknownKeys, key)
		}
	}
	if len(unknownKeys) > 0 {
		sort.Strings(unknownKeys)
		return Config{}, errors.New("config contains unknown keys: " + strings.Join(unknownKeys, ", "))
	}

	config := Defaults()
	var err error
	if rawValue, ok := payload["bbmb_address"]; ok && !isNull(rawValue) {
		config.BBMBAddress, err = decodeNonEmptyString(rawValue, "bbmb_address")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["db_path"]; ok && !isNull(rawValue) {
		config.DBPath, err = decodeNonEmptyString(rawValue, "db_path")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["max_loops"]; ok && !isNull(rawValue) {
		config.MaxLoops, err = decodePositiveInt(rawValue, "max_loops")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["poll_interval_seconds"]; ok && !isNull(rawValue) {
		config.PollIntervalSeconds, err = decodePositiveFloat(rawValue, "poll_interval_seconds")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["poll_timeout_seconds"]; ok && !isNull(rawValue) {
		config.PollTimeoutSeconds, err = decodePositiveInt(rawValue, "poll_timeout_seconds")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["metrics_host"]; ok && !isNull(rawValue) {
		config.MetricsHost, err = decodeNonEmptyString(rawValue, "metrics_host")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["metrics_port"]; ok && !isNull(rawValue) {
		config.MetricsPort, err = decodePositiveInt(rawValue, "metrics_port")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["allowed_egress_channels"]; ok && !isNull(rawValue) {
		config.AllowedEgressChannels, err = decodeAllowedEgressChannels(rawValue)
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["global_prompt_context"]; ok && !isNull(rawValue) {
		config.GlobalPromptContext, err = decodeStringList(rawValue, "global_prompt_context")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["cron_prompt_context"]; ok && !isNull(rawValue) {
		config.CronPromptContext, err = decodeStringList(rawValue, "cron_prompt_context")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["schedule_file"]; ok && !isNull(rawValue) {
		config.ScheduleFile, err = decodeNonEmptyString(rawValue, "schedule_file")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["auxiliary_ingress_enabled"]; ok && !isNull(rawValue) {
		config.AuxiliaryIngressEnabled, err = decodeBool(rawValue, "auxiliary_ingress_enabled")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["auxiliary_ingress_bbmb_address"]; ok && !isNull(rawValue) {
		config.AuxiliaryIngressBBMBAddress, err = decodeNonEmptyString(rawValue, "auxiliary_ingress_bbmb_address")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["auxiliary_ingress_queues"]; ok && !isNull(rawValue) {
		config.AuxiliaryIngressQueues, err = decodeStringList(rawValue, "auxiliary_ingress_queues")
		if err != nil {
			return Config{}, err
		}
	}
	if rawValue, ok := payload["auxiliary_ingress_context_refs"]; ok && !isNull(rawValue) {
		config.AuxiliaryIngressContextRefs, err = decodeStringList(rawValue, "auxiliary_ingress_context_refs")
		if err != nil {
			return Config{}, err
		}
	}
	if config.AuxiliaryIngressEnabled && len(config.AuxiliaryIngressQueues) == 0 {
		return Config{}, errors.New("auxiliary ingress requires auxiliary_ingress_queues")
	}
	return config, nil
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

func decodeAllowedEgressChannels(raw json.RawMessage) ([]string, error) {
	values, err := decodeStringList(raw, "allowed_egress_channels")
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return Defaults().AllowedEgressChannels, nil
	}
	return values, nil
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
