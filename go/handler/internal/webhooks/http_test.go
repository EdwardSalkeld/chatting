package webhooks

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReceipt(t *testing.T) {
	var logs bytes.Buffer
	h := Handler(log.New(&logs, "", 0))
	body := "hello\nforged log\x00" + strings.Repeat("x", MaxLogBodyBytes)
	r := httptest.NewRequest("POST", Path+"?token=private", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer private")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var reply map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if reply["request_id"] == "" || record["request_id"] != reply["request_id"] || w.Header().Get("X-Request-ID") != reply["request_id"] {
		t.Fatal("request IDs do not match")
	}
	if record["body_truncated"] != true || len(record["body"].(string)) != MaxLogBodyBytes {
		t.Fatal("body was not bounded")
	}
	if strings.Contains(logs.String(), "private") || strings.Count(logs.String(), "\n") != 1 {
		t.Fatal("unsafe log output")
	}
}

func TestRejections(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", Path, "", 405},
		{"POST", "/metrics", "", 404},
		{"POST", "/egress", "", 404},
		{"POST", "/api/schedules", "", 404},
		{"POST", Path + "/extra", "", 404},
		{"POST", Path, strings.Repeat("x", MaxBodyBytes+1), 413},
	} {
		var logs bytes.Buffer
		w := httptest.NewRecorder()
		Handler(log.New(&logs, "", 0)).ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.status || logs.Len() != 0 {
			t.Fatalf("%s %s: status=%d logs=%s", tc.method, tc.path, w.Code, logs.String())
		}
	}
}
