// Package webhooks implements logging-only public webhook receivers. It never
// submits tasks.
package webhooks

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

const Path = "/webhooks/test"
const GitHubPath = "/webhooks/github"
const MaxBodyBytes = 64 << 10
const MaxLogBodyBytes = 4096

func Handler(logger *log.Logger, githubSecret []byte) http.Handler {
	mux := http.NewServeMux()
	receive := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == GitHubPath && len(githubSecret) == 0 {
			http.Error(w, "github webhook is not configured", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "could not read body", http.StatusBadRequest)
			}
			return
		}
		if r.URL.Path == GitHubPath {
			signature := r.Header.Get("X-Hub-Signature-256")
			mac := hmac.New(sha256.New, githubSecret)
			_, _ = mac.Write(body)
			expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
			if !hmac.Equal([]byte(signature), []byte(expected)) {
				http.Error(w, "invalid signature", http.StatusUnauthorized)
				return
			}
			if !json.Valid(body) {
				http.Error(w, "invalid json", http.StatusBadRequest)
				return
			}
		}
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			http.Error(w, "could not generate request id", http.StatusInternalServerError)
			return
		}
		requestID := hex.EncodeToString(id[:])
		logged := body
		if len(logged) > MaxLogBodyBytes {
			logged = logged[:MaxLogBodyBytes]
		}
		// JSON escaping keeps hostile payloads on a single log line. Do not log
		// credentials, query strings, or caller-supplied request IDs.
		fields := map[string]any{
			"event": "webhook_received", "request_id": requestID,
			"path": r.URL.Path, "body_bytes": len(body), "body": string(logged),
			"body_truncated": len(logged) < len(body),
		}
		if r.URL.Path == GitHubPath {
			fields["event"] = "github_webhook_received"
			// These routing headers are useful for correlating GitHub deliveries,
			// but are not covered by GitHub's body signature. Bound their size.
			fields["github_event"] = boundedHeader(r, "X-GitHub-Event")
			fields["github_delivery"] = boundedHeader(r, "X-GitHub-Delivery")
		}
		record, _ := json.Marshal(fields)
		logger.Print(string(record))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-ID", requestID)
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "received", "request_id": requestID})
	}
	mux.HandleFunc(Path, receive)
	mux.HandleFunc(GitHubPath, receive)
	return mux
}

func boundedHeader(r *http.Request, name string) string {
	value := r.Header.Get(name)
	if len(value) > 256 {
		value = value[:256]
	}
	return value
}

// StartServer binds only IPv4 loopback. Zero disables the optional listener.
func StartServer(port int, githubSecret []byte) (*http.Server, error) {
	if port == 0 {
		return nil, nil
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Handler: Handler(log.Default(), githubSecret), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("webhook_listener_error: %v", err)
		}
	}()
	return server, nil
}
