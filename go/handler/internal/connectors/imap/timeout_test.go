package imap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// newStallingIMAPServer accepts connections, sends greeting (if non-empty),
// then reads whatever arrives and never answers.
func newStallingIMAPServer(t *testing.T, greeting string) (string, int) {
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

func TestDialClientTimesOutWithoutGreeting(t *testing.T) {
	host, port := newStallingIMAPServer(t, "")
	started := time.Now()
	_, err := DialClient(context.Background(), ClientConfig{Host: host, Port: port, IOTimeout: 100 * time.Millisecond})
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("returned after %s", elapsed)
	}
}

func TestNetClientCommandTimesOutWhenServerStalls(t *testing.T) {
	host, port := newStallingIMAPServer(t, "* OK ready\r\n")
	client, err := DialClient(context.Background(), ClientConfig{Host: host, Port: port, IOTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := client.Login("user", "password"); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("returned after %s", elapsed)
	}
	started = time.Now()
	_ = client.Logout()
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("logout reused the stalled session: %s", elapsed)
	}
}

func TestNetClientCancellationUnblocksStalledCommand(t *testing.T) {
	host, port := newStallingIMAPServer(t, "* OK ready\r\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := DialClient(ctx, ClientConfig{Host: host, Port: port, IOTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Logout() }()
	time.AfterFunc(100*time.Millisecond, cancel)
	started := time.Now()
	if _, err := client.Search("UNSEEN"); err == nil {
		t.Fatal("search succeeded")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("returned after %s", elapsed)
	}
}

func TestPollLogsTimeoutAndBacksOffWhenServerStalls(t *testing.T) {
	host, port := newStallingIMAPServer(t, "* OK ready\r\n")
	var output bytes.Buffer
	old := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(old)
	connector, err := New(Config{Host: host, Port: port, Username: "user", Password: "password",
		ClientFactory: func(ctx context.Context, config ClientConfig) (Client, error) {
			config.IOTimeout = 100 * time.Millisecond
			return DialClient(ctx, config)
		}})
	if err != nil {
		t.Fatal(err)
	}
	envelopes, err := connector.Poll(context.Background())
	if err != nil || len(envelopes) != 0 {
		t.Fatalf("poll = %d %v", len(envelopes), err)
	}
	if !strings.Contains(output.String(), "stage=login kind=timeout") {
		t.Fatal(output.String())
	}
	if connector.pollDelay == 0 {
		t.Fatal("no backoff after timeout")
	}
}
