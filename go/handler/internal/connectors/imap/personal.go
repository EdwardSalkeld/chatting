package imap

// The personal inbox uses a separate UID-only interface. The legacy mailbox
// connector deliberately keeps its existing semantics.
import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
)

type UIDClient interface {
	Login(string, string) error
	SelectUID(string) (uint32, uint32, error)
	SearchUID(uint32) ([]uint32, error)
	PeekUID(uint32) ([]byte, error)
	MarkReadUID(uint32) error
	MoveUID(uint32, string) error
	Logout() error
}

func DialUID(ctx context.Context, config ClientConfig) (UIDClient, error) {
	client, err := dialClient(ctx, config, true)
	if err != nil {
		return nil, err
	}
	return &uidSession{client.(*NetClient)}, nil
}

type uidSession struct{ *NetClient }

func (client *uidSession) Login(username, password string) error {
	user, err := safeQuote(username)
	if err != nil {
		return err
	}
	secret, err := safeQuote(password)
	if err != nil {
		return err
	}
	_, err = client.commandOK("LOGIN %s %s", user, secret)
	return err
}

var ErrMessageTooLarge = errors.New("email exceeds 10 MiB fetch limit")

var uidCode = regexp.MustCompile(`(?i)\[(UIDVALIDITY|UIDNEXT) ([0-9]+)\]`)

func (client *NetClient) SelectUID(mailbox string) (uint32, uint32, error) {
	quoted, err := safeQuote(mailbox)
	if err != nil {
		return 0, 0, err
	}
	lines, err := client.commandOK("SELECT %s", quoted)
	if err != nil {
		return 0, 0, err
	}
	values := map[string]uint32{}
	for _, line := range lines {
		for _, match := range uidCode.FindAllStringSubmatch(line, -1) {
			value, err := strconv.ParseUint(match[2], 10, 32)
			if err != nil {
				return 0, 0, err
			}
			values[strings.ToUpper(match[1])] = uint32(value)
		}
	}
	if values["UIDVALIDITY"] == 0 || values["UIDNEXT"] == 0 {
		return 0, 0, errors.New("IMAP server omitted UIDVALIDITY or UIDNEXT")
	}
	return values["UIDVALIDITY"], values["UIDNEXT"], nil
}

func (client *NetClient) SearchUID(start uint32) ([]uint32, error) {
	if start == 0 {
		return nil, errors.New("zero UID")
	}
	lines, err := client.commandOK("UID SEARCH UID %d:*", start)
	if err != nil {
		return nil, err
	}
	uids := []uint32{}
	for _, line := range lines {
		if !strings.HasPrefix(line, "* SEARCH") {
			continue
		}
		for _, field := range strings.Fields(strings.TrimPrefix(line, "* SEARCH")) {
			value, err := strconv.ParseUint(field, 10, 32)
			if err != nil || value == 0 {
				return nil, errors.New("invalid UID search response")
			}
			// IMAP ranges ending in * can include the last UID even when it
			// precedes start. Never ingest it as a new arrival.
			if value >= uint64(start) {
				uids = append(uids, uint32(value))
			}
		}
	}
	return uids, nil
}

func (client *NetClient) PeekUID(uid uint32) ([]byte, error) {
	if uid == 0 {
		return nil, errors.New("zero UID")
	}
	tag := client.nextTag()
	if err := client.writeCommand(tag, "UID FETCH %d (BODY.PEEK[])", uid); err != nil {
		return nil, err
	}
	var payload []byte
	for {
		line, err := client.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, tag+" ") {
			if strings.HasPrefix(line, tag+" OK") {
				return payload, nil
			}
			return nil, errors.New("UID FETCH rejected")
		}
		if size, ok := literalSize(line); ok {
			if size > 10*1024*1024 {
				_ = client.conn.Close()
				return nil, ErrMessageTooLarge
			}
			payload = make([]byte, size)
			if _, err := io.ReadFull(client.reader, payload); err != nil {
				return nil, err
			}
		}
	}
}

func (client *NetClient) MarkReadUID(uid uint32) error {
	if uid == 0 {
		return errors.New("zero UID")
	}
	_, err := client.commandOK("UID STORE %d +FLAGS.SILENT (\\Seen)", uid)
	return err
}

func (client *NetClient) MoveUID(uid uint32, folder string) error {
	if uid == 0 {
		return errors.New("zero UID")
	}
	quoted, err := safeQuote(folder)
	if err != nil {
		return err
	}
	// No COPY/EXPUNGE fallback: EXPUNGE can remove other clients' messages.
	_, err = client.commandOK("UID MOVE %d %s", uid, quoted)
	return err
}

func safeQuote(value string) (string, error) {
	if strings.ContainsAny(value, "\r\n\x00") {
		return "", errors.New("invalid IMAP argument")
	}
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`, nil
}

type TextMessage struct {
	From       string `json:"from"`
	Subject    string `json:"subject"`
	Date       string `json:"date"`
	Body       string `json:"body"`
	Incomplete bool   `json:"incomplete"`
}

func ParseText(raw []byte) TextMessage {
	message, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return TextMessage{Subject: "Unreadable email", Body: "Message could not be parsed; inspect original inbox.", Incomplete: true}
	}
	body, _ := extractMessageContent(message, "", "")
	// v1 does not inspect attachments, encrypted mail, or calendar invitations.
	// Surface these rather than quietly filing information we could not read.
	incomplete := bytes.Contains(bytes.ToLower(raw), []byte("content-disposition: attachment")) ||
		bytes.Contains(bytes.ToLower(raw), []byte("filename=")) ||
		bytes.Contains(bytes.ToLower(raw), []byte("application/")) ||
		bytes.Contains(bytes.ToLower(raw), []byte("text/calendar")) || body == "(empty body)"
	if len(body) > 60000 {
		body = string([]rune(body)[:min(len([]rune(body)), 15000)])
		incomplete = true
	}
	return TextMessage{From: limitText(decodeHeader(message.Header.Get("From")), 300),
		Subject: limitText(decodeHeader(message.Header.Get("Subject")), 300),
		Date:    limitText(message.Header.Get("Date"), 100), Body: body, Incomplete: incomplete}
}

func limitText(value string, maxRunes int) string {
	return string([]rune(value)[:min(len([]rune(value)), maxRunes)])
}
