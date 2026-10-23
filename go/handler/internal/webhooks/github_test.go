package webhooks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
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
			if _, exists := record["task_preview"]; exists || record["mode"] != "log_only" || record["filter"] == nil {
				t.Fatalf("unconfigured filter must log a decision without a preview: %s", logs.String())
			}
		})
	}
}

func TestSignedFilterIngress(t *testing.T) {
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	secret := []byte("secret")
	var logs bytes.Buffer
	handler := Handler(log.New(&logs, "", 0), secret, &Filter{Store: store, Login: "BillyAcachofa"})
	send := func(body, delivery string, valid bool, status int) map[string]any {
		t.Helper()
		logs.Reset()
		r := httptest.NewRequest("POST", GitHubPath, strings.NewReader(body))
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write([]byte(body))
		if valid {
			r.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		}
		r.Header.Set("X-GitHub-Event", "issue_comment")
		r.Header.Set("X-GitHub-Delivery", delivery)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		if status != 200 {
			return nil
		}
		var record map[string]any
		if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		return record
	}
	body := `{"action":"created","repository":{"full_name":"o/r"},"sender":{"login":"alice"},"issue":{"number":7},"comment":{"id":99,"body":"@BillyAcachofa help"}}`
	send(body, "tag", false, 401)
	stored, err := store.WebhookState(context.Background(), "watch", "billyacachofa/o/r#7")
	if err != nil || stored != "" {
		t.Fatalf("unsigned request touched watches: %q %v", stored, err)
	}
	kept := send(body, "tag", true, 200)
	encoded, _ := json.Marshal(kept["task_preview"])
	preview, err := contracts.DecodeTaskQueueMessage(encoded)
	if err != nil || preview.TaskID != "task:github-webhook:tag" || preview.Envelope.ReplyChannel.Type != "log" {
		t.Fatalf("preview %s: %v", encoded, err)
	}
	// Both a changed header and changed body formatting must still dedupe.
	for _, retry := range []struct{ body, delivery string }{{body, "other-id"}, {body + " ", "tag"}} {
		dropped := send(retry.body, retry.delivery, true, 200)
		if dropped["task_preview"] != nil || dropped["filter"].(map[string]any)["reason"] != "duplicate" {
			t.Fatalf("duplicate produced preview: %+v", dropped)
		}
	}
	// Matching uses the whole accepted payload even beyond the log preview cap.
	long := strings.Replace(body, "@BillyAcachofa help", strings.Repeat("x", MaxLogBodyBytes)+" @BillyAcachofa", 1)
	long = strings.Replace(long, `"number":7`, `"number":8`, 1)
	if got := send(long, "long-tag", true, 200); got["task_preview"] == nil || got["body_truncated"] != true {
		t.Fatalf("long mention missed: %+v", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	send(strings.Replace(body, `"number":7`, `"number":9`, 1), "state-error", true, 500)
}
