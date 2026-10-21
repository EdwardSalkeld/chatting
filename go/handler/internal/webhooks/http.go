// Package webhooks implements the logging-only public transit probe. It never
// submits tasks, and deliberately has no sender authentication policy yet.
package webhooks

import (
	"crypto/rand"
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
const MaxBodyBytes = 64 << 10
const MaxLogBodyBytes = 4096

func Handler(logger *log.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(Path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
		// headers, query strings, or caller-supplied request IDs.
		record, _ := json.Marshal(map[string]any{
			"event": "webhook_received", "request_id": requestID,
			"path": Path, "body_bytes": len(body), "body": string(logged),
			"body_truncated": len(logged) < len(body),
		})
		logger.Print(string(record))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-ID", requestID)
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "received", "request_id": requestID})
	})
	return mux
}

// StartServer binds only IPv4 loopback. Zero disables the optional listener.
func StartServer(port int) (*http.Server, error) {
	if port == 0 {
		return nil, nil
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Handler: Handler(log.Default()), ReadHeaderTimeout: 5 * time.Second,
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
