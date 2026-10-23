package webhooks

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGitHubAuthentication(t *testing.T) {
	secret := []byte("test-webhook-secret")
	body := `{"action":"published","repository":{"full_name":"BillyAcachofa/test"}}`
	sign := func(body string) string {
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write([]byte(body))
		return "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}
	for _, tc := range []struct {
		name, method, path, body, signature string
		secret                              []byte
		status                              int
	}{
		{"release", "POST", GitHubPath, body, sign(body), secret, 200},
		{"ping", "POST", GitHubPath, `{"zen":"hello"}`, sign(`{"zen":"hello"}`), secret, 200},
		{"unconfigured", "POST", GitHubPath, body, sign(body), nil, 503},
		{"unsigned", "POST", GitHubPath, body, "", secret, 401},
		{"tampered", "POST", GitHubPath, body + " ", sign(body), secret, 401},
		{"malformed signature", "POST", GitHubPath, body, "sha256=garbage", secret, 401},
		{"sha1", "POST", GitHubPath, body, "sha1=obsolete", secret, 401},
		{"invalid json", "POST", GitHubPath, "{", sign("{"), secret, 400},
		{"method", "GET", GitHubPath, "", "", secret, 405},
		{"subpath", "POST", GitHubPath + "/extra", body, sign(body), secret, 404},
		{"oversized", "POST", GitHubPath, strings.Repeat("x", MaxBodyBytes+1), "", secret, 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			r := httptest.NewRequest(tc.method, tc.path+"?token=private", strings.NewReader(tc.body))
			r.Header.Set("X-Hub-Signature-256", tc.signature)
			r.Header.Set("Authorization", "private")
			r.Header.Set("X-GitHub-Event", "release")
			r.Header.Set("X-GitHub-Delivery", "delivery-id")
			w := httptest.NewRecorder()
			Handler(log.New(&logs, "", 0), tc.secret).ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d: %s", w.Code, tc.status, w.Body.String())
			}
			if tc.status != 200 {
				if logs.Len() != 0 {
					t.Fatalf("rejected request logged: %s", logs.String())
				}
				return
			}
			var record map[string]any
			if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["event"] != "github_webhook_received" || record["github_delivery"] != "delivery-id" || record["path"] != GitHubPath || record["request_id"] != w.Header().Get("X-Request-ID") {
				t.Fatalf("incorrect log: %s", logs.String())
			}
			if strings.Contains(logs.String(), "private") || strings.Contains(logs.String(), tc.signature) {
				t.Fatal("credentials leaked into log")
			}
		})
	}
}
