package schedules

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/connectors/schedule"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const routePrefix = "/api/schedules"

type Service struct {
	store *sqlitestate.Store
}

func NewService(store *sqlitestate.Store) *Service {
	return &Service{store: store}
}

func RegisterRoutes(mux *http.ServeMux, service *Service) {
	mux.HandleFunc(routePrefix, service.handleCollection)
	mux.HandleFunc(routePrefix+"/", service.handleItem)
}

type scheduleJSON struct {
	ScheduleID         string   `json:"schedule_id"`
	Version            int      `json:"version"`
	Status             string   `json:"status"`
	JobName            string   `json:"job_name"`
	Content            string   `json:"content"`
	Cron               string   `json:"cron"`
	Timezone           string   `json:"timezone"`
	ContextRefs        []string `json:"context_refs"`
	PromptContext      []string `json:"prompt_context"`
	ReplyChannelType   string   `json:"reply_channel_type"`
	ReplyChannelTarget string   `json:"reply_channel_target"`
	CreatedAt          string   `json:"created_at"`
	CreatedBy          string   `json:"created_by"`
}

type scheduleRequest struct {
	JobName            string   `json:"job_name"`
	Content            string   `json:"content"`
	Cron               string   `json:"cron"`
	Timezone           string   `json:"timezone"`
	ContextRefs        []string `json:"context_refs"`
	PromptContext      []string `json:"prompt_context"`
	ReplyChannelType   string   `json:"reply_channel_type"`
	ReplyChannelTarget string   `json:"reply_channel_target"`
	CreatedBy          string   `json:"created_by"`
}

func (service *Service) handleCollection(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		service.listSchedules(writer, request)
	case http.MethodPost:
		service.createSchedule(writer, request)
	default:
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (service *Service) handleItem(writer http.ResponseWriter, request *http.Request) {
	scheduleID := strings.TrimPrefix(request.URL.Path, routePrefix+"/")
	if scheduleID == "" || strings.Contains(scheduleID, "/") {
		writeError(writer, http.StatusNotFound, "schedule not found")
		return
	}
	switch request.Method {
	case http.MethodGet:
		service.getSchedule(writer, request, scheduleID)
	case http.MethodPut:
		service.replaceSchedule(writer, request, scheduleID)
	case http.MethodDelete:
		service.deleteSchedule(writer, request, scheduleID)
	default:
		writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (service *Service) listSchedules(writer http.ResponseWriter, request *http.Request) {
	records, err := service.store.ListActiveSchedules(request.Context())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"schedules": schedulesToJSON(records)})
}

func (service *Service) createSchedule(writer http.ResponseWriter, request *http.Request) {
	input, ok := decodeScheduleInput(writer, request)
	if !ok {
		return
	}
	record, err := service.store.CreateSchedule(request.Context(), input)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"schedule": scheduleToJSON(record)})
}

func (service *Service) getSchedule(writer http.ResponseWriter, request *http.Request, scheduleID string) {
	record, err := service.store.GetActiveSchedule(request.Context(), scheduleID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	if record == nil {
		writeError(writer, http.StatusNotFound, "schedule not found")
		return
	}
	payload := map[string]any{"schedule": scheduleToJSON(*record)}
	if request.URL.Query().Get("history") == "1" {
		history, err := service.store.GetScheduleHistory(request.Context(), scheduleID)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, err.Error())
			return
		}
		payload["history"] = schedulesToJSON(history)
	}
	writeJSON(writer, http.StatusOK, payload)
}

func (service *Service) replaceSchedule(writer http.ResponseWriter, request *http.Request, scheduleID string) {
	input, ok := decodeScheduleInput(writer, request)
	if !ok {
		return
	}
	record, err := service.store.ReplaceSchedule(request.Context(), scheduleID, input)
	if errors.Is(err, sqlitestate.ErrScheduleNotFound) {
		writeError(writer, http.StatusNotFound, "schedule not found")
		return
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"schedule": scheduleToJSON(record)})
}

func (service *Service) deleteSchedule(writer http.ResponseWriter, request *http.Request, scheduleID string) {
	deleted, err := service.store.MarkScheduleDead(request.Context(), scheduleID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	if !deleted {
		writeError(writer, http.StatusNotFound, "schedule not found")
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func decodeScheduleInput(writer http.ResponseWriter, request *http.Request) (sqlitestate.ScheduleInput, bool) {
	var body scheduleRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(writer, http.StatusBadRequest, "malformed request body: "+err.Error())
		return sqlitestate.ScheduleInput{}, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, "request body must contain one JSON object")
		return sqlitestate.ScheduleInput{}, false
	}
	job := schedule.Job{
		JobName:            body.JobName,
		Content:            body.Content,
		Cron:               body.Cron,
		TimezoneName:       body.Timezone,
		ContextRefs:        body.ContextRefs,
		PromptContext:      body.PromptContext,
		ReplyChannelType:   body.ReplyChannelType,
		ReplyChannelTarget: body.ReplyChannelTarget,
	}
	if err := schedule.Validate(job); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return sqlitestate.ScheduleInput{}, false
	}
	return sqlitestate.ScheduleInput{
		JobName:            body.JobName,
		Content:            body.Content,
		Cron:               body.Cron,
		Timezone:           body.Timezone,
		ContextRefs:        body.ContextRefs,
		PromptContext:      body.PromptContext,
		ReplyChannelType:   body.ReplyChannelType,
		ReplyChannelTarget: body.ReplyChannelTarget,
		CreatedBy:          body.CreatedBy,
	}, true
}

func schedulesToJSON(records []sqlitestate.ScheduleRecord) []scheduleJSON {
	payload := make([]scheduleJSON, 0, len(records))
	for _, record := range records {
		payload = append(payload, scheduleToJSON(record))
	}
	return payload
}

func scheduleToJSON(record sqlitestate.ScheduleRecord) scheduleJSON {
	contextRefs := record.ContextRefs
	if contextRefs == nil {
		contextRefs = []string{}
	}
	promptContext := record.PromptContext
	if promptContext == nil {
		promptContext = []string{}
	}
	return scheduleJSON{
		ScheduleID:         record.ScheduleID,
		Version:            record.Version,
		Status:             record.Status,
		JobName:            record.JobName,
		Content:            record.Content,
		Cron:               record.Cron,
		Timezone:           record.Timezone,
		ContextRefs:        contextRefs,
		PromptContext:      promptContext,
		ReplyChannelType:   record.ReplyChannelType,
		ReplyChannelTarget: record.ReplyChannelTarget,
		CreatedAt:          record.CreatedAt.UTC().Format(time.RFC3339),
		CreatedBy:          record.CreatedBy,
	}
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	payload := map[string]any{"error": message}
	if status >= 400 && status < 500 {
		payload["usage"] = map[string]any{
			"endpoints": map[string]string{
				"create": "POST /api/schedules", "list": "GET /api/schedules",
				"get":     "GET /api/schedules/{schedule_id}?history=1",
				"replace": "PUT /api/schedules/{schedule_id}", "delete": "DELETE /api/schedules/{schedule_id}",
			},
			"request_body": map[string]any{
				"job_name": "daily-summary", "content": "Prepare the daily summary",
				"cron": "0 9 * * *", "timezone": "Europe/London",
				"context_refs": []string{}, "prompt_context": []string{},
				"reply_channel_type": "telegram", "reply_channel_target": "-1001234567890", "created_by": "worker",
			},
			"notes": []string{"cron uses exactly five fields", "timezone defaults to UTC", "reply channel type and target must be supplied together"},
		}
	}
	writeJSON(writer, status, payload)
}
