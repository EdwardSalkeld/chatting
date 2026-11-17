package personalinbox

import (
	"encoding/json"
	"testing"
)

func validConfig() Config {
	return Config{Enabled: true, DBPath: "/data/inbox.db", Account: "main", TokenEnv: "JMAP_TOKEN",
		OrdersTarget: "orders", ActionTarget: "actions", DigestTarget: "digest",
		DigestAt: "08:00", Timezone: "Europe/London", ConfidenceThreshold: 0.8,
		LinearKeyEnv: "LINEAR_KEY", LinearTeam: "team", LinkTemplate: DefaultLinkTemplate,
		Folders: map[string]string{"news": "Triage/News", "order": "Triage/Orders"}}
}

func TestConfigOptInAndStrictPolicy(t *testing.T) {
	config, err := Decode([]byte(`{"enabled":false}`))
	if err != nil || config.Enabled || !config.DryRun {
		t.Fatalf("%+v %v", config, err)
	}
	if config.LinkTemplate != DefaultLinkTemplate {
		t.Fatalf("link template default lost: %q", config.LinkTemplate)
	}
	for _, raw := range []string{`{"enabled":true}`, `{"enabled":false,"arbitrary_smtp":true}`} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	raw, _ := json.Marshal(validConfig())
	if _, err := Decode(raw); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRejectsUnknownCategoriesAndUnsafeValues(t *testing.T) {
	cases := map[string]func(*Config){
		"unknown folder category":  func(c *Config) { c.Folders = map[string]string{"spam": "Junk"} },
		"unknown mark_read":        func(c *Config) { c.MarkRead = []string{"everything"} },
		"action folder":            func(c *Config) { c.Folders = map[string]string{"action": "Triage/Actions"} },
		"folder with newline":      func(c *Config) { c.Folders = map[string]string{"news": "News\r\nEXPUNGE"} },
		"plaintext session url":    func(c *Config) { c.SessionURL = "http://api.example.com/jmap/session" },
		"plaintext link template":  func(c *Config) { c.LinkTemplate = "http://mail.example.com/{email}" },
		"missing token env":        func(c *Config) { c.TokenEnv = "" },
		"out of range digest time": func(c *Config) { c.DigestAt = "25:00" },
		"unknown timezone":         func(c *Config) { c.Timezone = "Mars/Olympus" },
		"threshold above one":      func(c *Config) { c.ConfidenceThreshold = 1.5 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			config := validConfig()
			mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestThresholdIsConfigurableWithASafeDefault(t *testing.T) {
	config := validConfig()
	if config.Threshold() != 0.8 {
		t.Fatalf("%v", config.Threshold())
	}
	// An unset threshold falls back rather than accepting everything, which is
	// what a zero value would otherwise mean.
	config.ConfidenceThreshold = 0
	if config.Threshold() != DefaultConfidenceThreshold {
		t.Fatalf("%v", config.Threshold())
	}
}

func TestTicketingIsOptional(t *testing.T) {
	config := validConfig()
	config.LinearTeam = ""
	config.LinearKeyEnv = ""
	if err := config.Validate(); err != nil {
		t.Fatalf("a configuration with no ticketing was refused: %v", err)
	}
	if config.CreatesTickets() {
		t.Fatal("no team should mean no tickets")
	}
	// A team with nowhere to read its key from is a half-configuration.
	config.LinearTeam = "team"
	if err := config.Validate(); err == nil {
		t.Fatal("team without a key environment variable accepted")
	}
	config.LinearKeyEnv = "LINEAR_KEY"
	if err := config.Validate(); err != nil || !config.CreatesTickets() {
		t.Fatalf("%v %v", err, config.CreatesTickets())
	}
}
