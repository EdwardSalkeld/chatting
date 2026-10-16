package personalinbox

import (
	"encoding/json"
	"testing"
)

func validConfig() Config {
	return Config{Enabled: true, DBPath: "/data/inbox.db", Account: "main", TokenEnv: "JMAP_TOKEN",
		OrdersTarget: "orders", ActionTarget: "actions", DigestTarget: "digest", DigestSeconds: 3600,
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
		"unknown folder category": func(c *Config) { c.Folders = map[string]string{"spam": "Junk"} },
		"unknown mark_read":       func(c *Config) { c.MarkRead = []string{"everything"} },
		"folder with newline":     func(c *Config) { c.Folders = map[string]string{"news": "News\r\nEXPUNGE"} },
		"plaintext session url":   func(c *Config) { c.SessionURL = "http://api.example.com/jmap/session" },
		"plaintext link template": func(c *Config) { c.LinkTemplate = "http://mail.example.com/{email}" },
		"missing token env":       func(c *Config) { c.TokenEnv = "" },
		"zero digest interval":    func(c *Config) { c.DigestSeconds = 0 },
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

func TestFolderAndMarkReadLookups(t *testing.T) {
	config := validConfig()
	config.MarkRead = []string{"news"}
	if config.Folder("news") != "Triage/News" {
		t.Fatalf("%q", config.Folder("news"))
	}
	// An unconfigured category stays in the inbox, which is how review remains
	// the queue of mail the workflow did not understand.
	if config.Folder("review") != "" {
		t.Fatalf("review should have no folder, got %q", config.Folder("review"))
	}
	if !config.MarksRead("news") || config.MarksRead("action") {
		t.Fatal("mark_read lookup wrong")
	}
}
