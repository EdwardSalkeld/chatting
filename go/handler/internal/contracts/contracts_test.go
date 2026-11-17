package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestContractRejectsInvalidFields(t *testing.T) {
	for _, tc := range []struct {
		fixture, field string
		value          any
	}{
		{"task.v1.json", "message_type", EgressMessageTypeV2},
		{"egress.v2.sequenced.json", "message_type", "chatting.egress.v1"},
		{"egress.v2.sequenced.json", "event_id", ""},
		{"egress.v2.sequenced.json", "sequence", nil},
	} {
		t.Run(tc.fixture+"/"+tc.field, func(t *testing.T) {
			var payload map[string]any
			if err := json.Unmarshal(readFixture(t, tc.fixture), &payload); err != nil {
				t.Fatal(err)
			}
			payload[tc.field] = tc.value
			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if tc.fixture == "task.v1.json" {
				_, err = DecodeTaskQueueMessage(raw)
			} else {
				_, err = DecodeEgressQueueMessage(raw)
			}
			if err == nil {
				t.Fatal("accepted invalid " + tc.field)
			}
		})
	}
	completion, err := DecodeEgressQueueMessage(readFixture(t, "egress.v2.sequenced.json"))
	if err != nil {
		t.Fatal(err)
	}
	completion.EventKind, completion.Sequence = "completion", nil
	if err := completion.Validate(); err == nil {
		t.Fatal("accepted unsequenced completion")
	}
}

func TestGoldenContractFixtures(t *testing.T) {
	tests := []struct {
		name   string
		decode func([]byte) ([]byte, error)
	}{
		{
			name: "task.v1.json",
			decode: func(raw []byte) ([]byte, error) {
				parsed, err := DecodeTaskQueueMessage(raw)
				if err != nil {
					return nil, err
				}
				return json.Marshal(parsed)
			},
		},
		{
			name: "egress.v2.sequenced.json",
			decode: func(raw []byte) ([]byte, error) {
				parsed, err := DecodeEgressQueueMessage(raw)
				if err != nil {
					return nil, err
				}
				if parsed.Sequence == nil || *parsed.Sequence != 0 {
					t.Fatalf("%s sequence = %#v", "egress.v2.sequenced.json", parsed.Sequence)
				}
				return json.Marshal(parsed)
			},
		},
		{
			name: "egress.v2.incremental.json",
			decode: func(raw []byte) ([]byte, error) {
				parsed, err := DecodeEgressQueueMessage(raw)
				if err != nil {
					return nil, err
				}
				if parsed.Sequence != nil {
					t.Fatalf("%s sequence = %#v", "egress.v2.incremental.json", parsed.Sequence)
				}
				return json.Marshal(parsed)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := readFixture(t, test.name)
			encoded, err := test.decode(raw)
			if err != nil {
				t.Fatal(err)
			}

			var original map[string]any
			if err := json.Unmarshal(raw, &original); err != nil {
				t.Fatal(err)
			}
			var roundTripped map[string]any
			if err := json.Unmarshal(encoded, &roundTripped); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(roundTripped, original) {
				t.Fatalf("round trip changed fixture\noriginal: %#v\nroundTripped: %#v", original, roundTripped)
			}
		})
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate current test file")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "..", "tests", "fixtures", "contracts", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
