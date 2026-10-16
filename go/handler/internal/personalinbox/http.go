package personalinbox

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// RegisterRoutes belongs on the loopback administrative mux, never the metrics
// listener. No mailbox operation is accepted from a worker response.
func (service *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/personal-inbox", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		messages, err := service.Store.messageViews(request.Context(), request.URL.Query().Get("after"))
		if err != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		effects, err := service.Store.effects(request.Context(), "json_extract(data,'$.status') IN ('pending','uncertain','inflight')")
		if err != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		counts := map[string]int{}
		rows, err := service.Store.db.QueryContext(request.Context(), "SELECT json_extract(data,'$.status'),count(*) FROM inbox_effects GROUP BY json_extract(data,'$.status')")
		if err != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		for rows.Next() {
			var status string
			var count int
			if err := rows.Scan(&status, &count); err != nil {
				_ = rows.Close()
				http.Error(writer, "storage unavailable", 503)
				return
			}
			counts[status] = count
		}
		_ = rows.Close()
		steps, err := service.Store.stepHealth(request.Context())
		if err != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		waiting, oldest, stuck, err := service.Store.awaiting(request.Context(), service.Now().Unix())
		if err != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		deadLetters, _ := service.Store.getMeta(request.Context(), "dead_letters")
		lastSuccess, _ := service.Store.getMeta(request.Context(), "last_success")
		lastError, _ := service.Store.getMeta(request.Context(), "last_error")
		_ = json.NewEncoder(writer).Encode(map[string]any{"work_item_id": "personal-inbox:" + service.Config.Account, "dry_run": service.Config.DryRun, "messages": messages, "effects_requiring_attention": effects, "effect_counts": counts, "last_success": lastSuccess, "last_error": lastError, "steps": steps,
			"awaiting_classification": map[string]any{"count": waiting, "oldest_seconds": oldest, "stuck": stuck},
			"dead_letters":            deadLetters})
	})
	mux.HandleFunc("/api/personal-inbox/effects", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(405)
			return
		}
		effects, err := service.Store.effects(request.Context(), "id>?", request.URL.Query().Get("after"))
		if err != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(effects)
	})
	mux.HandleFunc("/api/personal-inbox/resolve", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input struct {
			ID      string `json:"id"`
			Outcome string `json:"outcome"`
			Note    string `json:"note"`
		}
		raw, err := io.ReadAll(io.LimitReader(request.Body, 8193))
		if err != nil || len(raw) > 8192 || strictJSON(raw, &input) != nil || strings.TrimSpace(input.Note) == "" || (input.Outcome != "done" && input.Outcome != "retry") {
			http.Error(writer, "id, outcome done/retry, and reconciliation note required", 400)
			return
		}
		service.mu.Lock()
		defer service.mu.Unlock()
		var encoded string
		if err := service.Store.db.QueryRowContext(request.Context(), "SELECT data FROM inbox_effects WHERE id=?", input.ID).Scan(&encoded); err != nil {
			http.Error(writer, "effect unavailable", 404)
			return
		}
		var effect Effect
		if json.Unmarshal([]byte(encoded), &effect) != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		if effect.Status != "uncertain" && effect.Status != "inflight" {
			http.Error(writer, "only uncertain effects can be resolved", 409)
			return
		}
		effect.Status = input.Outcome
		if input.Outcome == "retry" {
			effect.Status = "pending"
		}
		effect.Note = input.Note
		if err := service.Store.saveEffect(request.Context(), effect); err != nil {
			http.Error(writer, "storage unavailable", 503)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(effect)
	})
}
