package workitems

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
)

type registrar interface {
	RegisterPR(context.Context, string, string) error
}

// RegisterRoutes exposes registration on the handler's loopback worker API.
func RegisterRoutes(mux *http.ServeMux, store registrar) {
	mux.HandleFunc("/work-items/register-pr", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Method != http.MethodPost {
			http.Error(writer, `{"reason":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		var input struct {
			TaskID string `json:"task_id"`
			PRURL  string `json:"pr_url"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 4096)).Decode(&input); err != nil || input.TaskID == "" || input.PRURL == "" {
			http.Error(writer, `{"reason":"task_id and pr_url are required"}`, http.StatusBadRequest)
			return
		}
		if err := store.RegisterPR(request.Context(), input.TaskID, input.PRURL); err != nil {
			if err == sql.ErrNoRows {
				http.Error(writer, `{"reason":"unknown task"}`, http.StatusNotFound)
			} else {
				http.Error(writer, `{"reason":"invalid or conflicting PR"}`, http.StatusConflict)
			}
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"status":"registered"}`))
	})
}
