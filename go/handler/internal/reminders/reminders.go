package reminders

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
	sqlitestate "github.com/EdwardSalkeld/chatting/go/handler/internal/state/sqlite"
)

const routePrefix = "/api/reminders"

type Metrics interface {
	RecordReminderCreated(replayed bool)
	RecordReminderCancelled()
}

type Service struct {
	store   *sqlitestate.Store
	metrics Metrics
}

func NewService(store *sqlitestate.Store, metricRecorders ...Metrics) *Service {
	service := &Service{store: store}
	if len(metricRecorders) > 0 {
		service.metrics = metricRecorders[0]
	}
	return service
}

func RegisterRoutes(mux *http.ServeMux, service *Service) {
	mux.HandleFunc(routePrefix, service.handleCollection)
	mux.HandleFunc(routePrefix+"/", service.handleItem)
}

type replyChannelRequest struct {
	Type     string         `json:"type"`
	Target   string         `json:"target"`
	Metadata map[string]any `json:"metadata"`
}

type reminderRequest struct {
	RunAt             string              `json:"run_at"`
	Prompt            string              `json:"prompt"`
	ReplyChannel      replyChannelRequest `json:"reply_channel"`
	ContextRefs       []string            `json:"context_refs"`
	PromptContext     []string            `json:"prompt_context"`
	CreatedFromTaskID string              `json:"created_from_task_id"`
	CreatedBy         string              `json:"created_by"`
	IdempotencyKey    string              `json:"idempotency_key"`
}

type reminderJSON struct {
	ReminderID        string                 `json:"reminder_id"`
	Revision          int                    `json:"revision"`
	Status            string                 `json:"status"`
	RunAt             string                 `json:"run_at"`
	Prompt            string                 `json:"prompt"`
	ReplyChannel      contracts.ReplyChannel `json:"reply_channel"`
	ContextRefs       []string               `json:"context_refs"`
	PromptContext     []string               `json:"prompt_context"`
	CreatedFromTaskID string                 `json:"created_from_task_id"`
	CreatedBy         string                 `json:"created_by"`
	IdempotencyKey    string                 `json:"idempotency_key"`
	CreatedAt         string                 `json:"created_at"`
	UpdatedAt         string                 `json:"updated_at"`
	FiredAt           *string                `json:"fired_at"`
	CancelledAt       *string                `json:"cancelled_at"`
}

func (service *Service) handleCollection(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		service.listReminders(writer, request)
	case http.MethodPost:
		service.createReminder(writer, request)
	default:
		writeAPIError(writer, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (service *Service) handleItem(writer http.ResponseWriter, request *http.Request) {
	reminderID := strings.TrimPrefix(request.URL.Path, routePrefix+"/")
	if reminderID == "" || strings.Contains(reminderID, "/") {
		writeAPIError(writer, http.StatusNotFound, "reminder not found")
		return
	}
	switch request.Method {
	case http.MethodGet:
		service.getReminder(writer, request, reminderID)
	case http.MethodPut:
		service.replaceReminder(writer, request, reminderID)
	case http.MethodDelete:
		service.cancelReminder(writer, request, reminderID)
	default:
		writeAPIError(writer, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (service *Service) listReminders(writer http.ResponseWriter, request *http.Request) {
	status := request.URL.Query().Get("status")
	if status == "" {
		status = sqlitestate.ReminderStatusScheduled
	}
	if err := sqlitestate.ValidateReminderStatus(status); err != nil {
		writeAPIError(writer, http.StatusBadRequest, err.Error())
		return
	}
	records, err := service.store.ListReminders(request.Context(), status)
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"reminders": remindersToJSON(records)})
}

func (service *Service) createReminder(writer http.ResponseWriter, request *http.Request) {
	input, ok := decodeReminderInput(writer, request)
	if !ok {
		return
	}
	record, replay, err := service.store.CreateReminder(request.Context(), input)
	if errors.Is(err, sqlitestate.ErrReminderIdempotencyConflict) {
		writeAPIError(writer, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	if service.metrics != nil {
		service.metrics.RecordReminderCreated(replay)
	}
	log.Printf("reminder_created reminder_id=%q revision=%d replay=%t run_at=%s", record.ReminderID, record.Revision, replay, record.RunAt.UTC().Format(time.RFC3339))
	status := http.StatusCreated
	if replay {
		status = http.StatusOK
	}
	writeJSON(writer, status, map[string]any{"reminder": reminderToJSON(record), "idempotent_replay": replay})
}

func (service *Service) getReminder(writer http.ResponseWriter, request *http.Request, reminderID string) {
	record, err := service.store.GetLatestReminder(request.Context(), reminderID)
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	if record == nil {
		writeAPIError(writer, http.StatusNotFound, "reminder not found")
		return
	}
	payload := map[string]any{"reminder": reminderToJSON(*record)}
	if request.URL.Query().Get("history") == "1" {
		history, err := service.store.GetReminderHistory(request.Context(), reminderID)
		if err != nil {
			writeAPIError(writer, http.StatusInternalServerError, err.Error())
			return
		}
		payload["history"] = remindersToJSON(history)
	}
	writeJSON(writer, http.StatusOK, payload)
}

func (service *Service) replaceReminder(writer http.ResponseWriter, request *http.Request, reminderID string) {
	input, ok := decodeReminderInput(writer, request)
	if !ok {
		return
	}
	record, replay, err := service.store.ReplaceReminder(request.Context(), reminderID, input)
	if errors.Is(err, sqlitestate.ErrReminderIdempotencyConflict) {
		writeAPIError(writer, http.StatusConflict, err.Error())
		return
	}
	if errors.Is(err, sqlitestate.ErrReminderNotFound) {
		latest, lookupErr := service.store.GetLatestReminder(request.Context(), reminderID)
		if lookupErr != nil {
			writeAPIError(writer, http.StatusInternalServerError, lookupErr.Error())
			return
		}
		if latest != nil {
			writeAPIError(writer, http.StatusConflict, "only scheduled reminders can be rescheduled")
			return
		}
		writeAPIError(writer, http.StatusNotFound, "reminder not found")
		return
	}
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("reminder_rescheduled reminder_id=%q revision=%d replay=%t run_at=%s", record.ReminderID, record.Revision, replay, record.RunAt.UTC().Format(time.RFC3339))
	writeJSON(writer, http.StatusOK, map[string]any{"reminder": reminderToJSON(record), "idempotent_replay": replay})
}

func (service *Service) cancelReminder(writer http.ResponseWriter, request *http.Request, reminderID string) {
	cancelled, err := service.store.CancelReminder(request.Context(), reminderID)
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	if !cancelled {
		latest, lookupErr := service.store.GetLatestReminder(request.Context(), reminderID)
		if lookupErr != nil {
			writeAPIError(writer, http.StatusInternalServerError, lookupErr.Error())
			return
		}
		if latest != nil {
			writeAPIError(writer, http.StatusConflict, "only scheduled reminders can be cancelled")
			return
		}
		writeAPIError(writer, http.StatusNotFound, "reminder not found")
		return
	}
	if service.metrics != nil {
		service.metrics.RecordReminderCancelled()
	}
	log.Printf("reminder_cancelled reminder_id=%q", reminderID)
	writer.WriteHeader(http.StatusNoContent)
}

func decodeReminderInput(writer http.ResponseWriter, request *http.Request) (sqlitestate.ReminderInput, bool) {
	var body reminderRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "malformed request body: "+err.Error())
		return sqlitestate.ReminderInput{}, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeAPIError(writer, http.StatusBadRequest, "request body must contain one JSON object")
		return sqlitestate.ReminderInput{}, false
	}
	runAt, err := time.Parse(time.RFC3339, body.RunAt)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "run_at must be RFC3339 with Z or an explicit UTC offset")
		return sqlitestate.ReminderInput{}, false
	}
	channel := contracts.ReplyChannel{Type: strings.TrimSpace(body.ReplyChannel.Type), Target: strings.TrimSpace(body.ReplyChannel.Target), Metadata: body.ReplyChannel.Metadata}
	if err := channel.Validate(); err != nil {
		writeAPIError(writer, http.StatusBadRequest, err.Error())
		return sqlitestate.ReminderInput{}, false
	}
	if strings.TrimSpace(body.Prompt) == "" {
		writeAPIError(writer, http.StatusBadRequest, "prompt is required")
		return sqlitestate.ReminderInput{}, false
	}
	if strings.TrimSpace(body.CreatedFromTaskID) == "" {
		writeAPIError(writer, http.StatusBadRequest, "created_from_task_id is required")
		return sqlitestate.ReminderInput{}, false
	}
	if strings.TrimSpace(body.IdempotencyKey) == "" {
		writeAPIError(writer, http.StatusBadRequest, "idempotency_key is required")
		return sqlitestate.ReminderInput{}, false
	}
	if err := validateStringList(body.ContextRefs, "context_refs"); err != nil {
		writeAPIError(writer, http.StatusBadRequest, err.Error())
		return sqlitestate.ReminderInput{}, false
	}
	if err := validateStringList(body.PromptContext, "prompt_context"); err != nil {
		writeAPIError(writer, http.StatusBadRequest, err.Error())
		return sqlitestate.ReminderInput{}, false
	}
	return sqlitestate.ReminderInput{
		RunAt: runAt.UTC(), Prompt: body.Prompt, ContextRefs: body.ContextRefs,
		PromptContext: body.PromptContext, ReplyChannelType: channel.Type,
		ReplyChannelTarget: channel.Target, ReplyChannelMetadata: channel.Metadata,
		CreatedFromTaskID: body.CreatedFromTaskID, CreatedBy: body.CreatedBy,
		IdempotencyKey: body.IdempotencyKey,
	}, true
}

func validateStringList(values []string, name string) error {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return errors.New(name + " must contain only non-empty strings")
		}
	}
	return nil
}

func remindersToJSON(records []sqlitestate.ReminderRecord) []reminderJSON {
	payload := make([]reminderJSON, 0, len(records))
	for _, record := range records {
		payload = append(payload, reminderToJSON(record))
	}
	return payload
}

func reminderToJSON(record sqlitestate.ReminderRecord) reminderJSON {
	contextRefs := record.ContextRefs
	if contextRefs == nil {
		contextRefs = []string{}
	}
	promptContext := record.PromptContext
	if promptContext == nil {
		promptContext = []string{}
	}
	metadata := record.ReplyChannelMetadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	var firedAt, cancelledAt *string
	if record.FiredAt != nil {
		value := record.FiredAt.UTC().Format(time.RFC3339Nano)
		firedAt = &value
	}
	if record.CancelledAt != nil {
		value := record.CancelledAt.UTC().Format(time.RFC3339Nano)
		cancelledAt = &value
	}
	return reminderJSON{
		ReminderID: record.ReminderID, Revision: record.Revision, Status: record.Status,
		RunAt: record.RunAt.UTC().Format(time.RFC3339), Prompt: record.Prompt,
		ReplyChannel: contracts.ReplyChannel{Type: record.ReplyChannelType, Target: record.ReplyChannelTarget, Metadata: metadata},
		ContextRefs:  contextRefs, PromptContext: promptContext,
		CreatedFromTaskID: record.CreatedFromTaskID, CreatedBy: record.CreatedBy,
		IdempotencyKey: record.IdempotencyKey, CreatedAt: record.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt: record.UpdatedAt.UTC().Format(time.RFC3339Nano), FiredAt: firedAt, CancelledAt: cancelledAt,
	}
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}

func writeAPIError(writer http.ResponseWriter, status int, message string) {
	payload := map[string]any{"error": message}
	if status >= 400 && status < 500 {
		payload["usage"] = apiUsage()
	}
	writeJSON(writer, status, payload)
}

func apiUsage() map[string]any {
	return map[string]any{
		"endpoints": map[string]string{
			"create": "POST /api/reminders", "list": "GET /api/reminders?status=scheduled",
			"get":        "GET /api/reminders/{reminder_id}?history=1",
			"reschedule": "PUT /api/reminders/{reminder_id}", "cancel": "DELETE /api/reminders/{reminder_id}",
		},
		"request_body": map[string]any{
			"run_at": "2026-08-15T14:30:00+01:00", "prompt": "Send the reminder response now",
			"reply_channel": map[string]any{"type": "telegram", "target": "-1001234567890", "metadata": map[string]any{}},
			"context_refs":  []string{}, "prompt_context": []string{},
			"created_from_task_id": "task:telegram:example", "created_by": "worker",
			"idempotency_key": "task:telegram:example:reminder:1",
		},
		"notes": []string{
			"run_at must include Z or an explicit UTC offset; responses normalize it to UTC",
			"POST and PUT require an idempotency_key; reusing it with different data returns 409",
			"copy the current task reply channel unless the user explicitly requests another destination",
			"only scheduled reminders can be rescheduled or cancelled",
		},
	}
}
