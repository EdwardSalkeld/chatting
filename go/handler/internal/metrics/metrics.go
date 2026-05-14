package metrics

import (
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now().UTC()
}

func (realClock) Since(start time.Time) time.Duration {
	return time.Since(start)
}

type Recorder struct {
	mu                sync.Mutex
	clock             Clock
	startedAt         time.Time
	loopsTotal        int64
	ingressPublished  int64
	egressLoopsTotal  int64
	egressReceived    int64
	egressDispatched  int64
	egressDeduped     int64
	egressDropped     int64
	egressDroppedBy   map[string]int64
	egressIncremental int64
	egressMessage     int64
	egressCompletion  int64
	lastLoopAt        time.Time
	lastEgressLoopAt  time.Time
}

func New(startedAt time.Time, clock Clock) *Recorder {
	if clock == nil {
		clock = realClock{}
	}
	if startedAt.IsZero() {
		startedAt = clock.Now()
	}
	return &Recorder{
		clock:           clock,
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

func (recorder *Recorder) RecordEgressLoop(completedAt time.Time) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.egressLoopsTotal++
	recorder.lastEgressLoopAt = normalizeTime(completedAt)
}

func (recorder *Recorder) RecordEgressResult(status string, reason string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.egressReceived++
	switch status {
	case "dispatched":
		recorder.egressDispatched++
		recorder.egressIncremental++
		recorder.egressMessage++
	case "completed":
		recorder.egressDispatched++
		recorder.egressIncremental++
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

func (recorder *Recorder) Snapshot() map[string]float64 {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	dedupeRate := 0.0
	if recorder.egressReceived > 0 {
		dedupeRate = (float64(recorder.egressDeduped) / float64(recorder.egressReceived)) * 100
	}

	return map[string]float64{
		"uptime_seconds":                                    recorder.clock.Since(recorder.startedAt).Seconds(),
		"process_start_time_seconds":                        float64(recorder.startedAt.UnixNano()) / float64(time.Second),
		"loops_total":                                       float64(recorder.loopsTotal),
		"ingress_published_total":                           float64(recorder.ingressPublished),
		"github_scanned_events_total":                       0,
		"github_new_events_total":                           0,
		"github_published_total":                            0,
		"telegram_attachment_cleanup_deleted_total":         0,
		"telegram_attachment_cleanup_missing_total":         0,
		"telegram_attachment_cleanup_failed_total":          0,
		"telegram_attachment_cleanup_reclaimed_bytes_total": 0,
		"last_loop_completed_timestamp_seconds":             unixTimestamp(recorder.lastLoopAt),
		"egress_loops_total":                                float64(recorder.egressLoopsTotal),
		"last_egress_loop_completed_timestamp_seconds":      unixTimestamp(recorder.lastEgressLoopAt),
		"received_total":                                    float64(recorder.egressReceived),
		"dispatched_total":                                  float64(recorder.egressDispatched),
		"deduped_total":                                     float64(recorder.egressDeduped),
		"dedupe_hit_rate_pct":                               dedupeRate,
		"dropped_total":                                     float64(recorder.egressDropped),
		"dropped_unknown_task_total":                        float64(recorder.egressDroppedBy["unknown_task"]),
		"dropped_completed_task_total":                      float64(recorder.egressDroppedBy["completed_task"]),
		"dropped_disallowed_channel_total":                  float64(recorder.egressDroppedBy["disallowed_channel"]),
		"dropped_missing_event_id_total":                    float64(recorder.egressDroppedBy["missing_event_id"]),
		"incremental_dispatched_total":                      float64(recorder.egressIncremental),
		"message_dispatched_total":                          float64(recorder.egressMessage),
		"completion_applied_total":                          float64(recorder.egressCompletion),
		"dispatch_latency_ms_avg":                           0,
		"dispatch_latency_ms_max":                           0,
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

type Server struct {
	server *http.Server
	done   chan struct{}
	addr   string
}

func StartServer(recorder *Recorder, host string, port int) (*Server, error) {
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

	listener, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: mux}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			return
		}
	}()
	return &Server{server: server, done: done, addr: listener.Addr().String()}, nil
}

func (server *Server) Addr() string {
	if server == nil {
		return ""
	}
	return server.addr
}

func (server *Server) Close() error {
	if server == nil || server.server == nil {
		return nil
	}
	err := server.server.Close()
	<-server.done
	return err
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
	{"chatting_message_handler_github_scanned_events_total", "counter", "GitHub assignment events scanned by the handler.", "github_scanned_events_total"},
	{"chatting_message_handler_github_new_events_total", "counter", "New GitHub assignment events discovered after checkpointing.", "github_new_events_total"},
	{"chatting_message_handler_github_published_total", "counter", "GitHub assignment events published as tasks.", "github_published_total"},
	{"chatting_message_handler_telegram_attachment_cleanup_deleted_total", "counter", "Telegram attachment files deleted by handler-managed cleanup.", "telegram_attachment_cleanup_deleted_total"},
	{"chatting_message_handler_telegram_attachment_cleanup_missing_total", "counter", "Tracked Telegram attachment files already missing from disk during cleanup.", "telegram_attachment_cleanup_missing_total"},
	{"chatting_message_handler_telegram_attachment_cleanup_failed_total", "counter", "Telegram attachment cleanup attempts that failed.", "telegram_attachment_cleanup_failed_total"},
	{"chatting_message_handler_telegram_attachment_cleanup_reclaimed_bytes_total", "counter", "Bytes reclaimed by Telegram attachment cleanup.", "telegram_attachment_cleanup_reclaimed_bytes_total"},
	{"chatting_message_handler_last_loop_completed_timestamp_seconds", "gauge", "Unix timestamp of the most recently completed loop.", "last_loop_completed_timestamp_seconds"},
	{"chatting_message_handler_egress_loops_total", "counter", "Completed egress polling loops.", "egress_loops_total"},
	{"chatting_message_handler_last_egress_loop_completed_timestamp_seconds", "gauge", "Unix timestamp of the most recently completed egress loop.", "last_egress_loop_completed_timestamp_seconds"},
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
	{"chatting_message_handler_egress_dispatch_latency_ms_avg", "gauge", "Average egress dispatch latency in milliseconds.", "dispatch_latency_ms_avg"},
	{"chatting_message_handler_egress_dispatch_latency_ms_max", "gauge", "Maximum egress dispatch latency in milliseconds.", "dispatch_latency_ms_max"},
}

func MetricNames() []string {
	names := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		names = append(names, definition.name)
	}
	sort.Strings(names)
	return names
}
