package imap

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const Source = "email"

type Client interface {
	Login(username string, password string) error
	Select(mailbox string) error
	Search(criterion string) ([]string, error)
	FetchRFC822(uid string) ([]byte, error)
	Logout() error
}

type ClientFactory func(ctx context.Context, config ClientConfig) (Client, error)

type ClientConfig struct {
	Host    string
	Port    int
	UseSSL  bool
	Timeout time.Duration
}

type Config struct {
	Host            string
	Port            int
	Username        string
	Password        string
	Mailbox         string
	SearchCriterion string
	ContextRefs     []string
	PromptContext   contracts.PromptContext
	UseSSL          bool
	Now             func() time.Time
	ClientFactory   ClientFactory
}

type Connector struct {
	config        Config
	clientFactory ClientFactory
	now           func() time.Time
	prompt        *contracts.PromptContext
}

func New(config Config) (*Connector, error) {
	if strings.TrimSpace(config.Host) == "" {
		return nil, errors.New("host is required")
	}
	if strings.TrimSpace(config.Username) == "" {
		return nil, errors.New("username is required")
	}
	if config.Password == "" {
		return nil, errors.New("password is required")
	}
	if config.Port <= 0 {
		return nil, errors.New("port must be positive")
	}
	if strings.TrimSpace(config.Mailbox) == "" {
		config.Mailbox = "INBOX"
	}
	if strings.TrimSpace(config.SearchCriterion) == "" {
		config.SearchCriterion = "UNSEEN"
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	factory := config.ClientFactory
	if factory == nil {
		factory = DialClient
	}
	var prompt *contracts.PromptContext
	if config.PromptContext.HasContent() {
		copy := config.PromptContext
		prompt = &copy
	}
	config.Host = strings.TrimSpace(config.Host)
	config.Username = strings.TrimSpace(config.Username)
	config.Mailbox = strings.TrimSpace(config.Mailbox)
	config.SearchCriterion = strings.TrimSpace(config.SearchCriterion)
	return &Connector{config: config, clientFactory: factory, now: now, prompt: prompt}, nil
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, err := connector.clientFactory(ctx, ClientConfig{
		Host:    connector.config.Host,
		Port:    connector.config.Port,
		UseSSL:  connector.config.UseSSL,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Logout() }()
	if err := client.Login(connector.config.Username, connector.config.Password); err != nil {
		return nil, errors.New("imap_login_failed")
	}
	if err := client.Select(connector.config.Mailbox); err != nil {
		return nil, fmt.Errorf("imap_select_failed:%s", connector.config.Mailbox)
	}
	uids, err := client.Search(connector.config.SearchCriterion)
	if err != nil {
		return nil, errors.New("imap_search_failed")
	}
	envelopes := make([]contracts.TaskEnvelope, 0, len(uids))
	for _, uid := range uids {
		raw, err := client.FetchRFC822(uid)
		if err != nil || len(raw) == 0 {
			continue
		}
		envelope, err := connector.toEnvelope(uid, raw)
		if err != nil {
			continue
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes, nil
}

func (connector *Connector) toEnvelope(uid string, raw []byte) (contracts.TaskEnvelope, error) {
	parsed, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return contracts.TaskEnvelope{}, err
	}
	fromHeader := parsed.Header.Get("From")
	fromAddress := ""
	if address, err := mail.ParseAddress(fromHeader); err == nil {
		fromAddress = address.Address
	}
	target := fromAddress
	if target == "" {
		target = connector.config.Username
	}
	subject := strings.TrimSpace(decodeHeader(parsed.Header.Get("Subject")))
	if subject == "" {
		subject = "(no subject)"
	}
	body := extractBodyText(parsed)
	receivedAt := parseReceivedAt(parsed.Header.Get("Date"), connector.now())
	eventID := "email:" + uid
	var actor *string
	if fromAddress != "" {
		value := fromAddress
		actor = &value
	}
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            eventID,
		Source:        Source,
		ReceivedAt:    contracts.NewTimestamp(receivedAt),
		Actor:         actor,
		Content:       "Subject: " + subject + "\n\n" + body,
		Attachments:   []contracts.AttachmentRef{},
		ContextRefs:   append([]string{}, connector.config.ContextRefs...),
		PromptContext: connector.prompt,
		ReplyChannel:  contracts.ReplyChannel{Type: "email", Target: target},
		DedupeKey:     eventID,
	}, nil
}

func decodeHeader(value string) string {
	decoded, err := new(mime.WordDecoder).DecodeHeader(value)
	if err != nil {
		return value
	}
	return decoded
}

func extractBodyText(message *mail.Message) string {
	contentType := message.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err == nil && strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		reader := multipart.NewReader(message.Body, params["boundary"])
		for {
			part, err := reader.NextPart()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				break
			}
			disposition := strings.ToLower(part.Header.Get("Content-Disposition"))
			if strings.Contains(disposition, "attachment") {
				continue
			}
			partType, _, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
			if strings.ToLower(partType) != "text/plain" {
				continue
			}
			if body := readBody(part.Header, part); body != "" {
				return body
			}
		}
		return "(empty body)"
	}
	if body := readBody(message.Header, message.Body); body != "" {
		return body
	}
	return "(empty body)"
}

type headerGetter interface {
	Get(key string) string
}

func readBody(header headerGetter, reader io.Reader) string {
	var decoded io.Reader = reader
	switch strings.ToLower(strings.TrimSpace(header.Get("Content-Transfer-Encoding"))) {
	case "base64":
		decoded = base64.NewDecoder(base64.StdEncoding, reader)
	case "quoted-printable":
		decoded = quotedprintable.NewReader(reader)
	}
	raw, err := io.ReadAll(decoded)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func parseReceivedAt(value string, fallback time.Time) time.Time {
	if strings.TrimSpace(value) == "" {
		return fallback.UTC()
	}
	parsed, err := mail.ParseDate(value)
	if err != nil {
		return fallback.UTC()
	}
	return parsed.UTC()
}

type NetClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	nextID atomic.Uint64
}

func DialClient(ctx context.Context, config ClientConfig) (Client, error) {
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Second
	}
	address := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	dialer := net.Dialer{Timeout: config.Timeout}
	var conn net.Conn
	var err error
	if config.UseSSL {
		conn, err = tls.DialWithDialer(&dialer, "tcp", address, &tls.Config{
			ServerName: config.Host,
			MinVersion: tls.VersionTLS12,
		})
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return nil, err
	}
	client := &NetClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
	if _, err := client.reader.ReadString('\n'); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

func (client *NetClient) Login(username string, password string) error {
	_, err := client.commandOK("LOGIN %s %s", quoteIMAP(username), quoteIMAP(password))
	return err
}

func (client *NetClient) Select(mailbox string) error {
	_, err := client.commandOK("SELECT %s", quoteIMAP(mailbox))
	return err
}

func (client *NetClient) Search(criterion string) ([]string, error) {
	lines, err := client.commandOK("SEARCH %s", criterion)
	if err != nil {
		return nil, err
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "* SEARCH") {
			fields := strings.Fields(strings.TrimPrefix(line, "* SEARCH"))
			return fields, nil
		}
	}
	return []string{}, nil
}

func (client *NetClient) FetchRFC822(uid string) ([]byte, error) {
	tag := client.nextTag()
	if err := client.writeCommand(tag, "FETCH %s (RFC822)", uid); err != nil {
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
			if strings.Contains(line, " OK") {
				return payload, nil
			}
			return nil, errors.New(line)
		}
		size, ok := literalSize(line)
		if !ok {
			continue
		}
		payload = make([]byte, size)
		if _, err := io.ReadFull(client.reader, payload); err != nil {
			return nil, err
		}
	}
}

func (client *NetClient) Logout() error {
	_, err := client.commandOK("LOGOUT")
	if closeErr := client.conn.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (client *NetClient) commandOK(format string, args ...any) ([]string, error) {
	tag := client.nextTag()
	if err := client.writeCommand(tag, format, args...); err != nil {
		return nil, err
	}
	lines := []string{}
	for {
		line, err := client.reader.ReadString('\n')
		if err != nil {
			return lines, err
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, tag+" ") {
			if strings.Contains(line, " OK") {
				return lines, nil
			}
			return lines, errors.New(line)
		}
		lines = append(lines, line)
	}
}

func (client *NetClient) writeCommand(tag string, format string, args ...any) error {
	if _, err := fmt.Fprintf(client.writer, tag+" "+format+"\r\n", args...); err != nil {
		return err
	}
	return client.writer.Flush()
}

func (client *NetClient) nextTag() string {
	return fmt.Sprintf("A%04d", client.nextID.Add(1))
}

func quoteIMAP(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}

var literalPattern = regexp.MustCompile(`\{(\d+)\}$`)

func literalSize(line string) (int, bool) {
	matches := literalPattern.FindStringSubmatch(line)
	if len(matches) != 2 {
		return 0, false
	}
	size, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, false
	}
	return size, true
}
