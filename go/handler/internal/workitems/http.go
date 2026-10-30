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
type subjectRegistrar interface {
	RegisterSubject(context.Context, string, string) error
}
type repositoryRegistrar interface {
	RegisterRepository(context.Context, string, string) error
}
type actionsRegistrar interface {
	RegisterRun(context.Context, string, string) error
	RegisterWorkflow(context.Context, string, string) error
}

// RegisterRoutes exposes explicit ownership on the handler's loopback worker API.
func RegisterRoutes(mux *http.ServeMux, store registrar) {
	registerRoute(mux, "/work-items/register-pr", "pr_url", store.RegisterPR)
	if subjects, ok := store.(subjectRegistrar); ok {
		registerRoute(mux, "/work-items/register-subject", "subject_url", subjects.RegisterSubject)
	}
	if repositories, ok := store.(repositoryRegistrar); ok {
		registerRoute(mux, "/work-items/register-repository", "repository", repositories.RegisterRepository)
	}
	if actions, ok := store.(actionsRegistrar); ok {
		registerRoute(mux, "/work-items/register-run", "run_url", actions.RegisterRun)
		registerRoute(mux, "/work-items/register-workflow", "workflow_url", actions.RegisterWorkflow)
	}
}

func registerRoute(mux *http.ServeMux, path, field string, register func(context.Context, string, string) error) {
	mux.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Method != http.MethodPost {
			http.Error(writer, `{"reason":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		var input map[string]string
		if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 4096)).Decode(&input); err != nil || input["task_id"] == "" || input[field] == "" {
			writer.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(writer).Encode(map[string]string{"reason": "task_id and " + field + " are required"})
			return
		}
		if err := register(request.Context(), input["task_id"], input[field]); err != nil {
			status := http.StatusConflict
			if err == sql.ErrNoRows {
				status = http.StatusNotFound
			}
			http.Error(writer, `{"reason":"unknown task or invalid/conflicting GitHub ownership"}`, status)
			return
		}
		_, _ = writer.Write([]byte(`{"status":"registered"}`))
	})
}
