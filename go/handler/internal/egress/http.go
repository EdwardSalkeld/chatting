package egress

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
)

// EgressRoutePath is the path the synchronous egress-submit endpoint listens on.
const EgressRoutePath = "/egress"

// maxEgressBodyBytes caps the submit body. Egress messages reference attachments
// by URI, never inline bytes, so a reply payload is small; this is a guard, not
// a real limit.
const maxEgressBodyBytes = 5 << 20

type submitResponse struct {
	Status   string         `json:"status"`
	Reason   string         `json:"reason,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// RegisterHTTPRoutes wires the synchronous egress-submit endpoint onto mux. It
// lets the worker (and the reply script) submit an egress message and learn the
// delivery outcome synchronously, instead of publishing to BBMB fire-and-forget
// and never hearing whether the send actually happened. onResult, if non-nil,
// is invoked with each terminal Result so callers can record metrics (this is
// the only egress path now, so metrics live here rather than the drain loop).
func RegisterHTTPRoutes(mux *http.ServeMux, engine *Engine, onResult func(Result)) {
	mux.HandleFunc(EgressRoutePath, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writeSubmitJSON(writer, http.StatusMethodNotAllowed, submitResponse{Status: "error", Reason: "method not allowed"})
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maxEgressBodyBytes))
		if err != nil {
			writeSubmitJSON(writer, http.StatusBadRequest, submitResponse{Status: "error", Reason: "read body: " + err.Error()})
			return
		}
		result, err := engine.HandleRaw(request.Context(), raw)
		if err != nil {
			// Infrastructure/state error: the send did not happen and the caller
			// should retry. Mirrors the BBMB path leaving the message un-acked.
			writeSubmitJSON(writer, http.StatusServiceUnavailable, submitResponse{Status: "error", Reason: err.Error()})
			return
		}
		if onResult != nil {
			onResult(result)
		}
		writeSubmitJSON(writer, statusCodeForResult(result), submitResponse{Status: result.Status, Reason: result.Reason, Metadata: result.Metadata})
	})
}

// statusCodeForResult maps an engine Result to an HTTP status the caller can act
// on: 2xx means the message is delivered (or already was); 422 means it was
// dropped for good (bad payload, unknown task, disallowed channel, dispatch
// failure) and the caller should fall back rather than retry.
func statusCodeForResult(result Result) int {
	switch result.Status {
	case StatusDispatched, StatusCompleted, StatusDeduped:
		return http.StatusOK
	case StatusStaged:
		// Accepted and buffered, waiting on an earlier sequence to arrive; not
		// sent yet, but not an error either.
		return http.StatusAccepted
	case StatusDropped:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func writeSubmitJSON(writer http.ResponseWriter, status int, payload submitResponse) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}

// Server owns the dedicated loopback HTTP server for synchronous egress submits.
type Server struct {
	server *http.Server
	done   chan struct{}
	addr   string
}

// StartHTTPServer binds a loopback-only HTTP server exposing the egress-submit
// endpoint. It is intentionally separate from the metrics server: the metrics
// bind may be 0.0.0.0 (so Prometheus can scrape), but the ability to send
// messages to the outside world must stay reachable only from the local host,
// matching BBMB's loopback posture.
func StartHTTPServer(host string, port int, engine *Engine, onResult func(Result)) (*Server, error) {
	mux := http.NewServeMux()
	RegisterHTTPRoutes(mux, engine, onResult)
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
