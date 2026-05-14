package metrics

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRecorderSnapshotAndPrometheusRendering(t *testing.T) {
	clock := &fakeClock{
		now: mustTime(t, "2026-03-09T12:00:10Z"),
	}
	recorder := New(mustTime(t, "2026-03-09T12:00:00Z"), clock)

	recorder.RecordLoop(2, mustTime(t, "2026-03-09T12:00:04Z"))
	recorder.RecordEgressLoop(mustTime(t, "2026-03-09T12:00:05Z"))
	recorder.RecordEgressResult("dispatched", "")
	recorder.RecordEgressResult("completed", "")
	recorder.RecordEgressResult("deduped", "")
	recorder.RecordEgressResult("dropped", "unknown_task")

	snapshot := recorder.Snapshot()
	if snapshot["loops_total"] != 1 {
		t.Fatalf("loops_total = %v", snapshot["loops_total"])
	}
	if snapshot["ingress_published_total"] != 2 {
		t.Fatalf("ingress_published_total = %v", snapshot["ingress_published_total"])
	}
	if snapshot["received_total"] != 4 {
		t.Fatalf("received_total = %v", snapshot["received_total"])
	}
	if snapshot["dispatched_total"] != 2 {
		t.Fatalf("dispatched_total = %v", snapshot["dispatched_total"])
	}
	if snapshot["deduped_total"] != 1 {
		t.Fatalf("deduped_total = %v", snapshot["deduped_total"])
	}
	if snapshot["dropped_unknown_task_total"] != 1 {
		t.Fatalf("dropped_unknown_task_total = %v", snapshot["dropped_unknown_task_total"])
	}
	if snapshot["dedupe_hit_rate_pct"] != 25 {
		t.Fatalf("dedupe_hit_rate_pct = %v", snapshot["dedupe_hit_rate_pct"])
	}

	rendered := RenderPrometheus(snapshot)
	for _, expected := range []string{
		"# TYPE chatting_message_handler_loops_total counter",
		"chatting_message_handler_ingress_published_total 2",
		"chatting_message_handler_egress_received_total 4",
		"chatting_message_handler_egress_dedupe_hit_rate_pct 25",
		"chatting_message_handler_egress_dropped_unknown_task_total 1",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered metrics missing %q:\n%s", expected, rendered)
		}
	}
}

func TestMetricsServerServesMetricsAnd404sOtherPaths(t *testing.T) {
	recorder := New(mustTime(t, "2026-03-09T12:00:00Z"), &fakeClock{now: mustTime(t, "2026-03-09T12:00:01Z")})
	recorder.RecordLoop(1, mustTime(t, "2026-03-09T12:00:01Z"))

	server, err := StartServer(recorder, "127.0.0.1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	response, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.StatusCode, string(body))
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("content type = %q", contentType)
	}
	if !strings.Contains(string(body), "chatting_message_handler_loops_total 1") {
		t.Fatalf("body = %s", string(body))
	}

	notFound, err := http.Get("http://" + server.Addr() + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer notFound.Body.Close()
	if notFound.StatusCode != http.StatusNotFound {
		t.Fatalf("not found status = %d", notFound.StatusCode)
	}
}

type fakeClock struct {
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	return clock.now
}

func (clock *fakeClock) Since(start time.Time) time.Duration {
	return clock.now.Sub(start)
}

func mustTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
