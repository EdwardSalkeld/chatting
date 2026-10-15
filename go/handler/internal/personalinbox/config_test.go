package personalinbox

import (
	"encoding/json"
	"testing"
)

func TestConfigOptInAndStrictPolicy(t *testing.T) {
	config, err := Decode([]byte(`{"enabled":false}`))
	if err != nil || config.Enabled || !config.DryRun {
		t.Fatalf("%+v %v", config, err)
	}
	for _, raw := range []string{`{"enabled":true}`, `{"enabled":false,"arbitrary_smtp":true}`} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	valid := Config{Enabled: true, DBPath: "/data/inbox.db", Account: "main", Host: "imap.example.com", Port: 993, Username: "ed", Mailbox: "INBOX", PasswordEnv: "IMAP_KEY", OrdersTarget: "orders", ActionTarget: "actions", DigestTarget: "digest", DigestSeconds: 3600, LinearKeyEnv: "LINEAR_KEY", LinearTeam: "team", MoveNews: true, NewsFolder: "News"}
	raw, _ := json.Marshal(valid)
	if _, err := Decode(raw); err != nil {
		t.Fatal(err)
	}
	valid.NewsFolder = "INBOX"
	if err := valid.Validate(); err == nil {
		t.Fatal("same mailbox accepted")
	}
	valid.NewsFolder = "News\r\nEXPUNGE"
	if err := valid.Validate(); err == nil {
		t.Fatal("command injection accepted")
	}
}
