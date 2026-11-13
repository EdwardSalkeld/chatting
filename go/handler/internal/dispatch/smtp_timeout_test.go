package dispatch

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// newStallingSMTPServer accepts connections, sends greeting (if non-empty),
// then reads whatever arrives and never answers.
func newStallingSMTPServer(t *testing.T, greeting string) (string, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		wg.Wait()
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				if greeting != "" {
					_, _ = io.WriteString(conn, greeting)
				}
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	address := listener.Addr().(*net.TCPAddr)
	return address.IP.String(), address.Port
}

func TestSMTPEmailSenderTimesOutWhenServerStalls(t *testing.T) {
	for name, greeting := range map[string]string{
		"before_greeting": "",
		"after_greeting":  "220 smtp.test ready\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			host, port := newStallingSMTPServer(t, greeting)
			sender, err := NewSMTPEmailSender(SMTPConfig{
				Host: host, Port: port, FromAddress: "bot@example.com", IOTimeout: 100 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			err = sender.Send(context.Background(), "alice@example.com", "Done.", nil)
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("error = %v", err)
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("returned after %s", elapsed)
			}
		})
	}
}
