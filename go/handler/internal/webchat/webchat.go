// Package webchat exposes durable web ingress exclusively through a Unix socket.
package webchat

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	"github.com/EdwardSalkeld/chatting/go/handler/internal/httpserver"
	state "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

type Service struct {
	Store       *state.Store
	ContextRefs []string
}

var clientID = regexp.MustCompile("^[a-zA-Z0-9_-]{16,80}$")

func (s Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/conversations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		rows, err := s.Store.ListWebConversations(r.Context())
		if err != nil {
			fail(w, 500, "Could not load conversations")
			return
		}
		respond(w, 200, map[string]any{"conversations": rows})
	})
	mux.HandleFunc("/api/conversations/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/conversations/"), "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] != "messages" {
			fail(w, 404, "Unknown conversation route")
			return
		}
		if r.Method == http.MethodGet {
			rows, err := s.Store.WebSubmissions(r.Context(), parts[0])
			if err != nil {
				fail(w, 500, "Could not load submissions")
				return
			}
			respond(w, 200, map[string]any{"messages": rows})
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		var input struct {
			Message  string `json:"message"`
			ClientID string `json:"client_message_id"`
		}
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
		d.DisallowUnknownFields()
		if err := d.Decode(&input); err != nil || strings.TrimSpace(input.Message) == "" || len(input.Message) > 32000 || !clientID.MatchString(input.ClientID) {
			fail(w, 400, "A message (up to 32000 bytes) and a valid client_message_id are required")
			return
		}
		if d.Decode(new(any)) != io.EOF {
			fail(w, 400, "Expected one JSON object")
			return
		}
		row, err := s.Store.QueueWebMessage(r.Context(), parts[0], input.ClientID, input.Message, s.ContextRefs)
		if errors.Is(err, sql.ErrNoRows) {
			fail(w, 404, "Conversation not found")
			return
		}
		if errors.Is(err, state.ErrWebConflict) {
			fail(w, 409, "This message ID was already used for different content")
			return
		}
		if err != nil {
			fail(w, 500, "Could not queue message")
			return
		}
		respond(w, 202, row)
	})
}
func (s Service) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	return s.Store.PendingWebEnvelopes(ctx)
}
func (s Service) AckEnvelope(ctx context.Context, id string) error {
	return s.Store.MarkWebPublished(ctx, id)
}
func (s Service) Start(path string) (*httpserver.Server, error) {
	mux := http.NewServeMux()
	s.Register(mux)
	return httpserver.StartUnix(path, mux)
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}
