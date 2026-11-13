package idleconn

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

func TestReadFailsWhenPeerStaysSilent(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	conn := Wrap(client, 50*time.Millisecond)
	_, err := conn.Read(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
}

func TestDeadlineMovesWithEachRead(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		for range 4 {
			time.Sleep(30 * time.Millisecond)
			if _, err := server.Write([]byte("x")); err != nil {
				return
			}
		}
	}()

	conn := Wrap(client, 80*time.Millisecond)
	buffer := make([]byte, 1)
	for range 4 {
		if _, err := conn.Read(buffer); err != nil {
			t.Fatal(err)
		}
	}
}
