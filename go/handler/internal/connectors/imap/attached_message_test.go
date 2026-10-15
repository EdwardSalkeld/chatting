package imap

import (
	"strings"
	"testing"
)

func TestAttachedEmailMustNotDisappear(t *testing.T) {
	raw := []byte("From: news@example.com\r\nSubject: Weekly newsletter\r\nContent-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\nWeekly newsletter\r\n--x\r\nContent-Type: message/rfc822\r\nContent-Disposition: attachment; filename=invoice.eml\r\n\r\nFrom: billing@example.com\r\nSubject: Overdue invoice\r\nContent-Type: text/plain\r\n\r\nPay immediately to avoid cancellation\r\n--x--\r\n")
	parsed := ParseText(raw)
	if !parsed.Incomplete && !strings.Contains(parsed.Body, "Pay immediately") {
		t.Fatalf("attached action email silently omitted: %+v", parsed)
	}
}
