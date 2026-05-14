package dispatch

import (
	"context"
	"crypto/tls"
	"io"
	"net/mail"
	"net/smtp"
	"strings"
	"testing"
)

func TestSMTPEmailSenderSendsRenderedMessage(t *testing.T) {
	client := &fakeSMTPClient{}
	sender, err := NewSMTPEmailSenderForTest(SMTPConfig{
		Host:        "smtp.example.com",
		Port:        25,
		FromAddress: "bot@example.com",
	}, func(_ context.Context, _ SMTPConfig) (SMTPClient, error) {
		return client, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	subject := "Re: Quarterly update"

	if err := sender.Send(context.Background(), "alice@example.com", "Done.", &subject); err != nil {
		t.Fatal(err)
	}

	if client.from != "bot@example.com" {
		t.Fatalf("from = %q", client.from)
	}
	if len(client.recipients) != 1 || client.recipients[0] != "alice@example.com" {
		t.Fatalf("recipients = %#v", client.recipients)
	}
	parsed, err := mail.ReadMessage(strings.NewReader(client.data))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Header.Get("From") != "bot@example.com" || parsed.Header.Get("To") != "alice@example.com" {
		t.Fatalf("headers = %#v", parsed.Header)
	}
	if parsed.Header.Get("Subject") != "Re: Quarterly update" {
		t.Fatalf("subject = %q", parsed.Header.Get("Subject"))
	}
	body, err := io.ReadAll(parsed.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "Done.\n" {
		t.Fatalf("body = %q", string(body))
	}
	if !client.quit {
		t.Fatal("quit was not called")
	}
}

func TestSMTPEmailSenderUsesStartTLSAndAuth(t *testing.T) {
	client := &fakeSMTPClient{}
	sender, err := NewSMTPEmailSenderForTest(SMTPConfig{
		Host:        "smtp.example.com",
		Port:        587,
		FromAddress: "bot@example.com",
		Username:    "bot@example.com",
		Password:    "secret",
		StartTLS:    true,
	}, func(_ context.Context, _ SMTPConfig) (SMTPClient, error) {
		return client, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := sender.Send(context.Background(), "alice@example.com", "Done.", nil); err != nil {
		t.Fatal(err)
	}

	if !client.startTLS {
		t.Fatal("starttls was not called")
	}
	if !client.auth {
		t.Fatal("auth was not called")
	}
}

func TestNewSMTPEmailSenderValidatesConfig(t *testing.T) {
	_, err := NewSMTPEmailSender(SMTPConfig{Host: "smtp.example.com", Port: 25})
	if err == nil || !strings.Contains(err.Error(), "smtp from address is required") {
		t.Fatalf("error = %v", err)
	}
}

type fakeSMTPClient struct {
	from       string
	recipients []string
	data       string
	startTLS   bool
	auth       bool
	quit       bool
	closed     bool
}

func (client *fakeSMTPClient) StartTLS(_ *tls.Config) error {
	client.startTLS = true
	return nil
}

func (client *fakeSMTPClient) Auth(_ smtp.Auth) error {
	client.auth = true
	return nil
}

func (client *fakeSMTPClient) Mail(from string) error {
	client.from = from
	return nil
}

func (client *fakeSMTPClient) Rcpt(to string) error {
	client.recipients = append(client.recipients, to)
	return nil
}

func (client *fakeSMTPClient) Data() (io.WriteCloser, error) {
	return &fakeDataWriter{client: client}, nil
}

func (client *fakeSMTPClient) Quit() error {
	client.quit = true
	return nil
}

func (client *fakeSMTPClient) Close() error {
	client.closed = true
	return nil
}

type fakeDataWriter struct {
	client *fakeSMTPClient
}

func (writer *fakeDataWriter) Write(data []byte) (int, error) {
	writer.client.data += string(data)
	return len(data), nil
}

func (writer *fakeDataWriter) Close() error {
	return nil
}
