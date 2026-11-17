package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/testutil"
)

func TestRecorderSnapshotAndPrometheusRendering(t *testing.T) {
	now := testutil.MustTime(t, "2026-03-09T12:00:10Z")
	recorder := New(testutil.MustTime(t, "2026-03-09T12:00:00Z"), func() time.Time { return now })

	recorder.RecordLoop(2, testutil.MustTime(t, "2026-03-09T12:00:04Z"))
	recorder.RecordEgressResult("dispatched", "")
	recorder.RecordEgressResult("completed", "")
	recorder.RecordEgressResult("deduped", "")
	recorder.RecordEgressResult("dropped", "unknown_task")
	recorder.RecordReminderCreated(false)
	recorder.RecordReminderCreated(true)
	recorder.RecordReminderCancelled()
	recorder.RecordReminderDue(true, false)
	recorder.RecordReminderDue(false, true)
	recorder.RecordReminderFired()

	snapshot := recorder.Snapshot()
	for key, want := range map[string]float64{
		"uptime_seconds": 10, "loops_total": 1, "ingress_published_total": 2, "received_total": 4,
		"dispatched_total": 2, "deduped_total": 1, "dropped_unknown_task_total": 1, "dedupe_hit_rate_pct": 25,
		"reminder_created_total": 1, "reminder_cancelled_total": 1, "reminder_due_total": 1, "reminder_retried_total": 1,
		"reminder_late_total": 1, "reminder_published_total": 1, "reminder_fired_total": 1,
	} {
		if got := snapshot[key]; got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}

	rendered := RenderPrometheus(snapshot)
	for _, expected := range []string{
		"# TYPE chatting_message_handler_loops_total counter",
		"chatting_message_handler_egress_received_total 4",
		"chatting_message_handler_egress_dedupe_hit_rate_pct 25",
		"chatting_message_handler_reminder_fired_total 1",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered metrics missing %q:\n%s", expected, rendered)
		}
	}
}
