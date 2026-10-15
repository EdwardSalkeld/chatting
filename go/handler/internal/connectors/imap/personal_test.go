package imap

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestUIDWireUsesPeekAndSafeOperations(t *testing.T) {
	connection, server := net.Pipe()
	defer connection.Close()
	defer server.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	client := &NetClient{conn: connection, reader: bufio.NewReader(connection), writer: bufio.NewWriter(connection)}
	commands := make(chan string, 5)
	go func() {
		reader := bufio.NewReader(server)
		for i := 0; i < 5; i++ {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			parts := strings.SplitN(strings.TrimSpace(line), " ", 2)
			commands <- parts[1]
			switch i {
			case 0:
				fmt.Fprintf(server, "* OK [UIDVALIDITY 42] stable\r\n* OK [UIDNEXT 100] next\r\n%s OK selected\r\n", parts[0])
			case 1:
				fmt.Fprintf(server, "* SEARCH 99\r\n%s OK searched\r\n", parts[0])
			case 2:
				raw := "Subject: hello\r\n\r\nbody"
				fmt.Fprintf(server, "* 4 FETCH (UID 99 BODY[] {%d}\r\n%s)\r\n%s OK fetched\r\n", len(raw), raw, parts[0])
			default:
				fmt.Fprintf(server, "%s OK changed\r\n", parts[0])
			}
		}
	}()
	validity, next, err := client.SelectUID("INBOX")
	if err != nil || validity != 42 || next != 100 {
		t.Fatalf("%d %d %v", validity, next, err)
	}
	uids, err := client.SearchUID(100)
	if err != nil || len(uids) != 0 {
		t.Fatal("range star reingested last UID")
	}
	raw, err := client.PeekUID(99)
	if err != nil || !strings.Contains(string(raw), "hello") {
		t.Fatal(err)
	}
	if err := client.MarkReadUID(99); err != nil {
		t.Fatal(err)
	}
	if err := client.MoveUID(99, "News"); err != nil {
		t.Fatal(err)
	}
	expected := []string{`SELECT "INBOX"`, "UID SEARCH UID 100:*", "UID FETCH 99 (BODY.PEEK[])", `UID STORE 99 +FLAGS.SILENT (\Seen)`, `UID MOVE 99 "News"`}
	for _, want := range expected {
		if actual := <-commands; actual != want {
			t.Fatalf("got %s want %s", actual, want)
		}
	}
}

func TestPersonalParseNeverWritesAttachments(t *testing.T) {
	raw := []byte("From: Alice <alice@example.com>\r\nSubject: =?UTF-8?Q?Test_email?=\r\nContent-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\nBody\r\n--x\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=test.pdf\r\n\r\nattachment-data\r\n--x--\r\n")
	mail := ParseText(raw)
	if !mail.Incomplete || mail.Subject != "Test email" || mail.Body != "Body" {
		t.Fatalf("%+v", mail)
	}
	if _, err := safeQuote("INBOX\r\nUID EXPUNGE 1:*"); err == nil {
		t.Fatal("command injection accepted")
	}
	quoted, err := safeQuote(`a\b"c`)
	if err != nil || quoted != `"a\\b\"c"` {
		t.Fatalf("%q %v", quoted, err)
	}
}
