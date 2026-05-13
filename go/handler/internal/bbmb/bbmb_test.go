package bbmb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"testing"
)

func TestAdapterPublishJSONEnsuresQueueAndPublishesEncodedPayload(t *testing.T) {
	requests := make(chan bbmbRequest, 4)
	server := newFakeServer(t, func(request bbmbRequest) []byte {
		requests <- request
		switch request.command {
		case commandEnsureQueue:
			return []byte{statusOK}
		case commandAddMessage:
			if request.strings[0] != "chatting.tasks.v1" {
				t.Fatalf("queue = %q", request.strings[0])
			}
			if request.strings[2] != checksum(request.strings[1]) {
				t.Fatalf("checksum = %q", request.strings[2])
			}
			return append([]byte{statusOK}, writeString("guid-1")...)
		default:
			t.Fatalf("unexpected command: %d", request.command)
			return nil
		}
	})
	defer server.close()

	adapter := newTestAdapter(t, server.address)
	guid, err := adapter.PublishJSON(context.Background(), "chatting.tasks.v1", map[string]any{
		"message_type": "chatting.task.v1",
		"task_id":      "task:email:1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if guid != "guid-1" {
		t.Fatalf("guid = %q", guid)
	}

	ensure := <-requests
	if ensure.command != commandEnsureQueue || ensure.strings[0] != "chatting.tasks.v1" {
		t.Fatalf("ensure request = %#v", ensure)
	}
	add := <-requests
	if add.command != commandAddMessage {
		t.Fatalf("add command = %d", add.command)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(add.strings[1]), &payload); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"message_type": "chatting.task.v1", "task_id": "task:email:1"}
	if !reflect.DeepEqual(payload, want) {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestAdapterPickupJSONReturnsObjectPayload(t *testing.T) {
	server := newFakeServer(t, func(request bbmbRequest) []byte {
		if request.command != commandPickupMessage {
			t.Fatalf("unexpected command: %d", request.command)
		}
		if request.strings[0] != "chatting.egress.v1" {
			t.Fatalf("queue = %q", request.strings[0])
		}
		if request.timeoutSeconds != 5 || request.waitSeconds != 2 {
			t.Fatalf("timeouts = %d/%d", request.timeoutSeconds, request.waitSeconds)
		}
		payload := []byte{statusOK}
		payload = append(payload, writeString("guid-egress-1")...)
		payload = append(payload, writeString(`{"message_type":"chatting.egress.v2","sequence":0}`)...)
		payload = append(payload, writeString(checksum(`{"message_type":"chatting.egress.v2","sequence":0}`))...)
		return payload
	})
	defer server.close()

	adapter := newTestAdapter(t, server.address)
	picked, err := adapter.PickupJSON(context.Background(), "chatting.egress.v1", 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	if picked == nil {
		t.Fatal("picked = nil")
	}
	if picked.GUID != "guid-egress-1" {
		t.Fatalf("guid = %q", picked.GUID)
	}
	if picked.Payload["message_type"] != "chatting.egress.v2" {
		t.Fatalf("payload = %#v", picked.Payload)
	}
	if picked.Payload["sequence"] != float64(0) {
		t.Fatalf("sequence = %#v", picked.Payload["sequence"])
	}
}

func TestAdapterPickupJSONReturnsNilForEmptyQueue(t *testing.T) {
	server := newFakeServer(t, func(request bbmbRequest) []byte {
		if request.command != commandPickupMessage {
			t.Fatalf("unexpected command: %d", request.command)
		}
		return []byte{statusEmptyQueue}
	})
	defer server.close()

	adapter := newTestAdapter(t, server.address)
	picked, err := adapter.PickupJSON(context.Background(), "chatting.egress.v1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if picked != nil {
		t.Fatalf("picked = %#v", picked)
	}
}

func TestAdapterPickupJSONRejectsMalformedJSON(t *testing.T) {
	server := newPickupPayloadServer(t, "guid-bad", `{not-json`)
	defer server.close()

	adapter := newTestAdapter(t, server.address)
	_, err := adapter.PickupJSON(context.Background(), "chatting.egress.v1", 0, 0)
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("error = %#v", err)
	}
	if opErr.Operation != "pickup_payload_invalid_json" {
		t.Fatalf("operation = %q", opErr.Operation)
	}
}

func TestAdapterPickupJSONRejectsNonObjectPayload(t *testing.T) {
	server := newPickupPayloadServer(t, "guid-array", `["not","object"]`)
	defer server.close()

	adapter := newTestAdapter(t, server.address)
	_, err := adapter.PickupJSON(context.Background(), "chatting.egress.v1", 0, 0)
	var opErr *OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("error = %#v", err)
	}
	if opErr.Operation != "pickup_payload_invalid_type" {
		t.Fatalf("operation = %q", opErr.Operation)
	}
}

func TestAdapterAckDeletesMessage(t *testing.T) {
	server := newFakeServer(t, func(request bbmbRequest) []byte {
		if request.command != commandDeleteMessage {
			t.Fatalf("unexpected command: %d", request.command)
		}
		if got, want := request.strings, []string{"chatting.egress.v1", "guid-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("delete args = %#v", got)
		}
		return []byte{statusOK}
	})
	defer server.close()

	adapter := newTestAdapter(t, server.address)
	if err := adapter.Ack(context.Background(), "chatting.egress.v1", "guid-1"); err != nil {
		t.Fatal(err)
	}
}

func newTestAdapter(t *testing.T, address string) *Adapter {
	t.Helper()
	client, err := NewClient(address)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAdapterWithClient(client)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func newPickupPayloadServer(t *testing.T, guid string, content string) *fakeServer {
	t.Helper()
	return newFakeServer(t, func(request bbmbRequest) []byte {
		if request.command != commandPickupMessage {
			t.Fatalf("unexpected command: %d", request.command)
		}
		payload := []byte{statusOK}
		payload = append(payload, writeString(guid)...)
		payload = append(payload, writeString(content)...)
		payload = append(payload, writeString(checksum(content))...)
		return payload
	})
}

type fakeServer struct {
	listener net.Listener
	address  string
	done     chan struct{}
}

func newFakeServer(t *testing.T, handler func(bbmbRequest) []byte) *fakeServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeServer{
		listener: listener,
		address:  listener.Addr().String(),
		done:     make(chan struct{}),
	}
	go func() {
		defer close(server.done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				command, payload, err := readFrame(conn)
				if err != nil {
					t.Errorf("read frame: %v", err)
					return
				}
				request, err := parseRequest(command, payload)
				if err != nil {
					t.Errorf("parse request: %v", err)
					return
				}
				if err := writeFrame(conn, command, handler(request)); err != nil {
					t.Errorf("write frame: %v", err)
				}
			}()
		}
	}()
	return server
}

func (server *fakeServer) close() {
	_ = server.listener.Close()
	<-server.done
}

type bbmbRequest struct {
	command        byte
	strings        []string
	timeoutSeconds int
	waitSeconds    int
}

func parseRequest(command byte, payload []byte) (bbmbRequest, error) {
	request := bbmbRequest{command: command}
	offset := 0
	readNextString := func() error {
		value, nextOffset, err := readString(payload, offset)
		if err != nil {
			return err
		}
		request.strings = append(request.strings, value)
		offset = nextOffset
		return nil
	}
	switch command {
	case commandEnsureQueue:
		if err := readNextString(); err != nil {
			return request, err
		}
	case commandAddMessage:
		for range 3 {
			if err := readNextString(); err != nil {
				return request, err
			}
		}
	case commandPickupMessage:
		if err := readNextString(); err != nil {
			return request, err
		}
		if offset+4 > len(payload) {
			return request, errors.New("missing timeout")
		}
		request.timeoutSeconds = int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if offset+4 <= len(payload) {
			request.waitSeconds = int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		}
	case commandDeleteMessage:
		for range 2 {
			if err := readNextString(); err != nil {
				return request, err
			}
		}
	default:
		return request, errors.New("unknown command")
	}
	return request, nil
}
