package egress

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func submitEgress(t *testing.T, engine *Engine, method string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	RegisterHTTPRoutes(mux, engine)
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request := httptest.NewRequest(method, EgressRoutePath, reader)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func decodeSubmit(t *testing.T, recorder *httptest.ResponseRecorder) submitResponse {
	t.Helper()
	var response submitResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response %q: %v", recorder.Body.String(), err)
	}
	return response
}

func TestSubmitEndpointDeliversAndReturnsOK(t *testing.T) {
	task := testTaskMessage(t)
	state := newFakeState()
	state.addTask(task)
	dispatcher := &recordingDispatcher{}
	engine := newTestEngine(t, state, dispatcher)

	raw, err := json.Marshal(testEgressMessage(t, task, nil, "evt:http:1", "incremental"))
	if err != nil {
		t.Fatal(err)
	}
	recorder := submitEgress(t, engine, http.MethodPost, raw)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if response := decodeSubmit(t, recorder); response.Status != StatusDispatched {
		t.Fatalf("response = %#v", response)
	}
	if len(dispatcher.messages) != 1 {
		t.Fatalf("dispatched messages = %#v", dispatcher.messages)
	}
}

func TestSubmitEndpointReturns422OnDrop(t *testing.T) {
	// No task registered, so the engine drops the message as unknown_task.
	state := newFakeState()
	engine := newTestEngine(t, state, nil)
	raw, err := json.Marshal(testEgressMessage(t, testTaskMessage(t), nil, "evt:http:2", "incremental"))
	if err != nil {
		t.Fatal(err)
	}

	var recorder *httptest.ResponseRecorder
	logs := captureLog(t, func() {
		recorder = submitEgress(t, engine, http.MethodPost, raw)
	})

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	response := decodeSubmit(t, recorder)
	if response.Status != StatusDropped || response.Reason != "unknown_task" {
		t.Fatalf("response = %#v", response)
	}
	// A drop must stay loud, same as the BBMB path.
	if !bytes.Contains([]byte(logs), []byte("egress_message_dropped")) {
		t.Fatalf("expected a loud drop log, got %q", logs)
	}
}

func TestSubmitEndpointDropsInvalidJSON(t *testing.T) {
	engine := newTestEngine(t, newFakeState(), nil)

	var recorder *httptest.ResponseRecorder
	captureLog(t, func() {
		recorder = submitEgress(t, engine, http.MethodPost, []byte("{not json"))
	})

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", recorder.Code)
	}
	if response := decodeSubmit(t, recorder); response.Reason != "invalid_payload" {
		t.Fatalf("response = %#v", response)
	}
}

func TestSubmitEndpointRejectsNonPost(t *testing.T) {
	engine := newTestEngine(t, newFakeState(), nil)
	recorder := submitEgress(t, engine, http.MethodGet, nil)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestStatusCodeForResult(t *testing.T) {
	cases := map[string]int{
		StatusDispatched: http.StatusOK,
		StatusCompleted:  http.StatusOK,
		StatusDeduped:    http.StatusOK,
		StatusStaged:     http.StatusAccepted,
		StatusDropped:    http.StatusUnprocessableEntity,
		"weird":          http.StatusInternalServerError,
	}
	for status, want := range cases {
		if got := statusCodeForResult(Result{Status: status}); got != want {
			t.Errorf("statusCodeForResult(%q) = %d, want %d", status, got, want)
		}
	}
}
