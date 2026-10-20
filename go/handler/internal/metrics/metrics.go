package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/httpserver"
)

type Recorder struct {
	mu                sync.Mutex
	now               func() time.Time
	startedAt         time.Time
	loopsTotal        int64
	ingressPublished  int64
	egressReceived    int64
	egressDispatched  int64
	egressDeduped     int64
	egressDropped     int64
	egressDroppedBy   map[string]int64
	egressMessage     int64
	egressCompletion  int64
	reminderCreated   int64
	reminderCancelled int64
	reminderDue       int64
	reminderRetried   int64
	reminderLate      int64
	reminderFired     int64
	lastLoopAt        time.Time
}

func New(startedAt time.Time, now func() time.Time) *Recorder {
	if now == nil {
		now = time.Now
	}
	if startedAt.IsZero() {
		startedAt = now()
	}
	return &Recorder{
		now:             now,
		startedAt:       startedAt.UTC(),
		egressDroppedBy: map[string]int64{},
	}
}

func (recorder *Recorder) RecordLoop(ingressPublished int, completedAt time.Time) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.loopsTotal++
	recorder.ingressPublished += int64(ingressPublished)
	recorder.lastLoopAt = normalizeTime(completedAt)
}

func (recorder *Recorder) RecordEgressResult(status string, reason string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.egressReceived++
	switch status {
	case "dispatched":
		recorder.egressDispatched++
		recorder.egressMessage++
	case "completed":
		recorder.egressDispatched++
		recorder.egressCompletion++
	case "deduped":
		recorder.egressDeduped++
	case "dropped":
		recorder.egressDropped++
		if reason != "" {
			recorder.egressDroppedBy[reason]++
		}
	}
}

func (recorder *Recorder) RecordReminderCreated(replayed bool) {
	if replayed {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.reminderCreated++
}

func (recorder *Recorder) RecordReminderCancelled() {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.reminderCancelled++
}

func (recorder *Recorder) RecordReminderDue(late bool, retry bool) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if retry {
		recorder.reminderRetried++
		return
	}
	recorder.reminderDue++
	if late {
		recorder.reminderLate++
	}
}

func (recorder *Recorder) RecordReminderFired() {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.reminderFired++
}

func (recorder *Recorder) Snapshot() map[string]float64 {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	dedupeRate := 0.0
	if recorder.egressReceived > 0 {
		dedupeRate = (float64(recorder.egressDeduped) / float64(recorder.egressReceived)) * 100
	}

	// reminder_published_total and incremental_dispatched_total repeat
	// reminder_fired_total and dispatched_total: each pair only ever moved
	// together, and the duplicate series are kept for existing dashboards.
	return map[string]float64{
		"uptime_seconds":                        recorder.now().Sub(recorder.startedAt).Seconds(),
		"process_start_time_seconds":            float64(recorder.startedAt.UnixNano()) / float64(time.Second),
		"loops_total":                           float64(recorder.loopsTotal),
		"ingress_published_total":               float64(recorder.ingressPublished),
		"reminder_created_total":                float64(recorder.reminderCreated),
		"reminder_cancelled_total":              float64(recorder.reminderCancelled),
		"reminder_due_total":                    float64(recorder.reminderDue),
		"reminder_published_total":              float64(recorder.reminderFired),
		"reminder_retried_total":                float64(recorder.reminderRetried),
		"reminder_late_total":                   float64(recorder.reminderLate),
		"reminder_fired_total":                  float64(recorder.reminderFired),
		"last_loop_completed_timestamp_seconds": unixTimestamp(recorder.lastLoopAt),
		"received_total":                        float64(recorder.egressReceived),
		"dispatched_total":                      float64(recorder.egressDispatched),
		"deduped_total":                         float64(recorder.egressDeduped),
		"dedupe_hit_rate_pct":                   dedupeRate,
		"dropped_total":                         float64(recorder.egressDropped),
		"dropped_unknown_task_total":            float64(recorder.egressDroppedBy["unknown_task"]),
		"dropped_completed_task_total":          float64(recorder.egressDroppedBy["completed_task"]),
		"dropped_disallowed_channel_total":      float64(recorder.egressDroppedBy["disallowed_channel"]),
		"dropped_missing_event_id_total":        float64(recorder.egressDroppedBy["missing_event_id"]),
		"incremental_dispatched_total":          float64(recorder.egressDispatched),
		"message_dispatched_total":              float64(recorder.egressMessage),
		"completion_applied_total":              float64(recorder.egressCompletion),
	}
}

func RenderPrometheus(snapshot map[string]float64) string {
	var builder strings.Builder
	for _, definition := range definitions {
		builder.WriteString("# HELP ")
		builder.WriteString(definition.name)
		builder.WriteByte(' ')
		builder.WriteString(definition.help)
		builder.WriteByte('\n')
		builder.WriteString("# TYPE ")
		builder.WriteString(definition.name)
		builder.WriteByte(' ')
		builder.WriteString(definition.kind)
		builder.WriteByte('\n')
		builder.WriteString(definition.name)
		builder.WriteByte(' ')
		builder.WriteString(formatValue(snapshot[definition.key]))
		builder.WriteByte('\n')
	}
	return builder.String()
}

func StartServer(recorder *Recorder, host string, port int, routeRegistrars ...func(*http.ServeMux)) (*httpserver.Server, error) {
	if recorder == nil {
		return nil, fmt.Errorf("metrics recorder is required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		payload := []byte(RenderPrometheus(recorder.Snapshot()))
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writer.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(payload)
	})
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	})
	for _, register := range routeRegistrars {
		if register != nil {
			register(mux)
		}
	}
	return httpserver.Start(host, port, mux)
}

func normalizeTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC()
}

func unixTimestamp(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.UnixNano()) / float64(time.Second)
}

func formatValue(value float64) string {
	if value == float64(int64(value)) {
		return strconv.FormatInt(int64(value), 10)
	}
	return strconv.FormatFloat(value, 'f', 6, 64)
}

type metricDefinition struct {
	name string
	kind string
	help string
	key  string
}

var definitions = []metricDefinition{
	{"chatting_message_handler_uptime_seconds", "gauge", "Seconds since the message handler process started.", "uptime_seconds"},
	{"chatting_message_handler_process_start_time_seconds", "gauge", "Unix timestamp when the message handler process started.", "process_start_time_seconds"},
	{"chatting_message_handler_loops_total", "counter", "Completed message handler loops.", "loops_total"},
	{"chatting_message_handler_ingress_published_total", "counter", "Tasks published to the worker ingress queue.", "ingress_published_total"},
	{"chatting_message_handler_reminder_created_total", "counter", "One-off reminders created through the handler API.", "reminder_created_total"},
	{"chatting_message_handler_reminder_cancelled_total", "counter", "Scheduled reminders cancelled through the handler API.", "reminder_cancelled_total"},
	{"chatting_message_handler_reminder_due_total", "counter", "Reminder revisions first observed as due.", "reminder_due_total"},
	{"chatting_message_handler_reminder_published_total", "counter", "Reminder tasks accepted into the worker queue.", "reminder_published_total"},
	{"chatting_message_handler_reminder_retried_total", "counter", "Due reminders emitted again before publish acknowledgement.", "reminder_retried_total"},
	{"chatting_message_handler_reminder_late_total", "counter", "Reminder revisions first observed after their requested run time.", "reminder_late_total"},
	{"chatting_message_handler_reminder_fired_total", "counter", "Reminder revisions marked fired after publish acknowledgement or handler dedupe.", "reminder_fired_total"},
	{"chatting_message_handler_last_loop_completed_timestamp_seconds", "gauge", "Unix timestamp of the most recently completed loop.", "last_loop_completed_timestamp_seconds"},
	{"chatting_message_handler_egress_received_total", "counter", "Egress messages received from the broker.", "received_total"},
	{"chatting_message_handler_egress_dispatched_total", "counter", "Egress events applied by the message-handler.", "dispatched_total"},
	{"chatting_message_handler_egress_deduped_total", "counter", "Egress messages skipped due to deduplication.", "deduped_total"},
	{"chatting_message_handler_egress_dedupe_hit_rate_pct", "gauge", "Percentage of handled egress events skipped by dedupe.", "dedupe_hit_rate_pct"},
	{"chatting_message_handler_egress_dropped_total", "counter", "Egress messages dropped before dispatch.", "dropped_total"},
	{"chatting_message_handler_egress_dropped_unknown_task_total", "counter", "Egress messages dropped because the task ledger entry was missing.", "dropped_unknown_task_total"},
	{"chatting_message_handler_egress_dropped_completed_task_total", "counter", "Egress messages dropped because the task was already completed.", "dropped_completed_task_total"},
	{"chatting_message_handler_egress_dropped_disallowed_channel_total", "counter", "Egress messages dropped because the channel is not allowed.", "dropped_disallowed_channel_total"},
	{"chatting_message_handler_egress_dropped_missing_event_id_total", "counter", "Egress messages dropped because they had no event id.", "dropped_missing_event_id_total"},
	{"chatting_message_handler_egress_incremental_dispatched_total", "counter", "Incremental egress events dispatched in order.", "incremental_dispatched_total"},
	{"chatting_message_handler_egress_message_dispatched_total", "counter", "Task-scoped visible egress messages dispatched in order.", "message_dispatched_total"},
	{"chatting_message_handler_egress_completion_applied_total", "counter", "Internal completion events applied in order.", "completion_applied_total"},
}

func MetricNames() []string {
	names := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		names = append(names, definition.name)
	}
	sort.Strings(names)
	return names
}
