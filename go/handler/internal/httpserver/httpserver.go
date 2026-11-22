// Package httpserver runs the handler's small HTTP listeners.
package httpserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
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
	return serve(listener, handler), nil
}

func serve(listener net.Listener, handler http.Handler) *Server {
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
	return &Server{server: server, done: done, addr: listener.Addr().String()}
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

// StartUnix requires an existing protected parent directory. Only a stale socket
// which refuses connections may be removed; files and active listeners are kept.
func StartUnix(path string, handler http.Handler) (*Server, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("socket path must be absolute")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("socket path already exists and is not a socket")
		}
		connection, dialErr := net.DialTimeout("unix", path, 200*time.Millisecond)
		if dialErr == nil {
			connection.Close()
			return nil, fmt.Errorf("socket already has an active listener")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, dialErr
		}
		current, statErr := os.Lstat(path)
		if statErr != nil || !os.SameFile(info, current) {
			return nil, fmt.Errorf("socket changed during startup")
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0660); err != nil {
		listener.Close()
		return nil, err
	}
	return serve(listener, handler), nil
}
