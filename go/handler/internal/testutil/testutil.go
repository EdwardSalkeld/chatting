// Package testutil holds helpers shared by the handler's tests.
package testutil

import (
	"testing"
	"time"
)

// MustTime parses an RFC 3339 timestamp, with or without fractional seconds.
func MustTime(t testing.TB, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
