package dispatch

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/smtp"
	"strings"
	"time"
)

const defaultSubject = "Automation response"

type SMTPConfig struct {
	Host        string
	Port        int
	FromAddress string
	Username    string
	Password    string
	UseSSL      bool
	StartTLS    bool
	Timeout     time.Duration
}

type SMTPClient interface {
	StartTLS(config *tls.Config) error
	Auth(auth smtp.Auth) error
	Mail(from string) error
	Rcpt(to string) error
	Data() (io.WriteCloser, error)
	Quit() error
	Close() error
}

type SMTPEmailSender struct {
	config SMTPConfig
	dial   func(ctx context.Context, config SMTPConfig) (SMTPClient, error)
}

func NewSMTPEmailSender(config SMTPConfig) (*SMTPEmailSender, error) {
	if strings.TrimSpace(config.Host) == "" {
		return nil, errors.New("smtp host is required")
	}
	if config.Port <= 0 {
		return nil, errors.New("smtp port must be positive")
	}
	if strings.TrimSpace(config.FromAddress) == "" {
		return nil, errors.New("smtp from address is required")
	}
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Second
	}
	return &SMTPEmailSender{config: config, dial: dialSMTP}, nil
}

func NewSMTPEmailSenderForTest(config SMTPConfig, dial func(ctx context.Context, config SMTPConfig) (SMTPClient, error)) (*SMTPEmailSender, error) {
	sender, err := NewSMTPEmailSender(config)
	if err != nil {
		return nil, err
	}
	sender.dial = dial
	return sender, nil
}

func (sender *SMTPEmailSender) Send(ctx context.Context, target string, body string, subject *string) error {
	if strings.TrimSpace(target) == "" {
		return errors.New("target is required")
	}
	if strings.TrimSpace(body) == "" {
		return errors.New("body is required")
	}

	client, err := sender.dial(ctx, sender.config)
	if err != nil {
		return err
	}
	defer client.Close()

	host := sender.config.Host
	if sender.config.StartTLS {
		tlsConfig := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		if err := client.StartTLS(tlsConfig); err != nil {
			return err
		}
	}
	if sender.config.Username != "" && sender.config.Password != "" {
		if err := client.Auth(smtp.PlainAuth("", sender.config.Username, sender.config.Password, host)); err != nil {
			return err
		}
	}
	if err := client.Mail(sender.config.FromAddress); err != nil {
		return err
	}
	if err := client.Rcpt(target); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := writer.Write(renderEmailMessage(sender.config.FromAddress, target, body, subject)); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return client.Quit()
}

func renderEmailMessage(from string, to string, body string, subject *string) []byte {
	subjectValue := defaultSubject
	if subject != nil && strings.TrimSpace(*subject) != "" {
		subjectValue = strings.TrimSpace(*subject)
	}

	var buffer bytes.Buffer
	writeHeader(&buffer, "From", from)
	writeHeader(&buffer, "To", to)
	writeHeader(&buffer, "Subject", mime.QEncoding.Encode("utf-8", subjectValue))
	buffer.WriteString("MIME-Version: 1.0\r\n")
	buffer.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	buffer.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	buffer.WriteString("\r\n")
	buffer.WriteString(body)
	if !strings.HasSuffix(body, "\n") {
		buffer.WriteString("\n")
	}
	return buffer.Bytes()
}

func writeHeader(buffer *bytes.Buffer, key string, value string) {
	buffer.WriteString(key)
	buffer.WriteString(": ")
	buffer.WriteString(strings.ReplaceAll(strings.ReplaceAll(value, "\r", ""), "\n", " "))
	buffer.WriteString("\r\n")
}

func dialSMTP(ctx context.Context, config SMTPConfig) (SMTPClient, error) {
	address := net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port))
	dialer := &net.Dialer{Timeout: config.Timeout}
	if config.UseSSL {
		conn, err := tls.DialWithDialer(dialer, "tcp", address, &tls.Config{
			ServerName: config.Host,
			MinVersion: tls.VersionTLS12,
		})
		if err != nil {
			return nil, err
		}
		return smtp.NewClient(conn, config.Host)
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return smtp.NewClient(conn, config.Host)
}
