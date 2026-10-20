// Package httpserver runs the handler's small HTTP listeners.
package httpserver

import (
	"net"
	"net/http"
	"strconv"
)

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
	server := &http.Server{Handler: handler}
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
