package webhooks

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

type statusStore interface {
	GitHubWebhookStatus(context.Context, int, int) (contracts.GitHubWebhookStatus, error)
}

// RegisterStatusRoutes belongs on the loopback worker API, never public ingress
// or the optionally public metrics listener. Reading cannot replay or clear holds.
func RegisterStatusRoutes(mux *http.ServeMux, store statusStore) {
	mux.HandleFunc("/github/webhooks/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, `{"reason":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		limit, offset := 50, 0
		for key, values := range r.URL.Query() {
			if (key != "limit" && key != "offset") || len(values) != 1 {
				http.Error(w, `{"reason":"use limit 1..100 and offset 0..1000000"}`, http.StatusBadRequest)
				return
			}
			value, err := strconv.Atoi(values[0])
			if err != nil || (key == "limit" && (value < 1 || value > 100)) || (key == "offset" && (value < 0 || value > 1000000)) {
				http.Error(w, `{"reason":"use limit 1..100 and offset 0..1000000"}`, http.StatusBadRequest)
				return
			}
			if key == "limit" {
				limit = value
			} else {
				offset = value
			}
		}
		status, err := store.GitHubWebhookStatus(r.Context(), limit, offset)
		if err != nil {
			http.Error(w, `{"reason":"webhook status unavailable"}`, http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(status)
	})
}
