package httpserver

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServerDropsClientThatStallsMidHeaders(t *testing.T) {
	previous := serverTimeouts
	serverTimeouts.readHeader = 100 * time.Millisecond
	t.Cleanup(func() { serverTimeouts = previous })
	server, err := Start("127.0.0.1", 0, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /metrics HTTP/1.1\r\nHost: x\r\n"); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	started := time.Now()
	_, _ = io.ReadAll(conn)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("connection held for %s", elapsed)
	}
}

func TestServerLetsSlowHandlerFinish(t *testing.T) {
	previous := serverTimeouts
	serverTimeouts.readHeader = 100 * time.Millisecond
	serverTimeouts.read = 100 * time.Millisecond
	t.Cleanup(func() { serverTimeouts = previous })
	server, err := Start("127.0.0.1", 0, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(writer, "sent")
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	response, err := http.Get("http://" + server.Addr() + "/egress")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "sent" {
		t.Fatalf("body = %q %v", body, err)
	}
}
