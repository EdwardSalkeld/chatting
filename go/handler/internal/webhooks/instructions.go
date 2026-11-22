package webhooks

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type InstructionsStore interface {
	RecordInstructionsUpdate(context.Context) error
	InstructionsGeneration(context.Context) (int64, error)
}

func instructionsPush(body []byte, event, repository string) bool {
	if event != "push" || repository == "" {
		return false
	}
	var payload struct {
		Ref        string `json:"ref"`
		Deleted    bool   `json:"deleted"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	return json.Unmarshal(body, &payload) == nil && payload.Ref == "refs/heads/main" && !payload.Deleted && strings.EqualFold(payload.Repository.FullName, repository)
}

// RegisterInstructionsRoutes exposes only the durable generation on the
// handler's loopback worker API. No event data selects a Git revision.
func RegisterInstructionsRoutes(mux *http.ServeMux, store InstructionsStore) {
	mux.HandleFunc("/instructions/generation", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		generation, err := store.InstructionsGeneration(r.Context())
		if err != nil {
			http.Error(w, "could not read instructions generation", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]int64{"generation": generation})
	})
}
