// Package httpserver runs the handler's small HTTP listeners.
package httpserver

import (
	"net"
	"net/http"
	"strconv"
	"time"
)

// serverTimeouts stop a client that connects and stalls from holding a
// connection open forever. WriteTimeout is deliberately generous: it runs
// from the end of the request headers to the end of the response, so it must
// outlast the slowest synchronous handler (/egress waits on an upstream send),
// and a handler that overruns it loses its response even if the send worked.
// A var so tests can shorten it.
var serverTimeouts = struct {
	readHeader, read, write, idle time.Duration
}{
	readHeader: 5 * time.Second,
	read:       30 * time.Second,
	write:      5 * time.Minute,
	idle:       60 * time.Second,
}

type Server struct {
	server *http.Server
	done   chan struct{}
	addr   string
}

// Start binds host:port and serves handler in the background. The listener is
// bound before returning, so a port clash is reported to the caller.
func Start(host string, port int, handler http.Handler) (*Server, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: serverTimeouts.readHeader,
		ReadTimeout:       serverTimeouts.read,
		WriteTimeout:      serverTimeouts.write,
		IdleTimeout:       serverTimeouts.idle,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
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
