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
}

func TestLoadAcceptsMinimalRuntimeConfig(t *testing.T) {
	config, err := Load([]byte(`{
		"bbmb_address": "10.0.0.1:9999",
		"db_path": "/tmp/handler.db",
		"max_loops": 20,
		"poll_interval_seconds": 0.5,
		"poll_timeout_seconds": 3,
		"metrics_host": "0.0.0.0",
		"metrics_port": 9555,
		"allowed_egress_channels": ["log", "email"]
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
	if config.PollTimeoutSeconds != 3 {
		t.Fatalf("PollTimeoutSeconds = %d", config.PollTimeoutSeconds)
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
}

func TestLoadRejectsUnknownKeys(t *testing.T) {
	_, err := Load([]byte(`{"db_path": "/tmp/handler.db", "surprise": true}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "config contains unknown keys: surprise") {
		t.Fatalf("error = %v", err)
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
			name:    "fractional timeout",
			raw:     `{"poll_timeout_seconds": 1.2}`,
			message: "config poll_timeout_seconds must be a positive integer",
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

func TestLoadFileAndEnvPath(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "handler.json")
	if err := os.WriteFile(configPath, []byte(`{"db_path": "/tmp/from-file.db"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	config, err := LoadFromEnv("", map[string]string{MessageHandlerConfigPathEnvVar: configPath})
	if err != nil {
		t.Fatal(err)
	}
	if config.DBPath != "/tmp/from-file.db" {
		t.Fatalf("DBPath = %q", config.DBPath)
	}

	_, err = LoadFromEnv("", map[string]string{MessageHandlerConfigPathEnvVar: " "})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), MessageHandlerConfigPathEnvVar+" must not be empty") {
		t.Fatalf("error = %v", err)
	}
}
