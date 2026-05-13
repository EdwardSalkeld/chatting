package bbmb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
	DefaultAddress = "localhost:9876"
	MaxMessageSize = 1024 * 1024
)

const (
	commandEnsureQueue   byte = 0x01
	commandAddMessage    byte = 0x02
	commandPickupMessage byte = 0x03
	commandDeleteMessage byte = 0x04
)

const (
	statusOK              byte = 0x00
	statusEmptyQueue      byte = 0x01
	statusNotFound        byte = 0x02
	statusInvalidChecksum byte = 0x03
	statusMessageTooLarge byte = 0x04
	statusInternalError   byte = 0x05
)

var (
	ErrQueueEmpty      = errors.New("queue is empty")
	ErrNotFound        = errors.New("message not found")
	ErrInvalidChecksum = errors.New("server rejected checksum")
	ErrMessageTooLarge = errors.New("message too large")
	ErrServer          = errors.New("server internal error")
)

type OperationError struct {
	Operation string
	Queue     string
	GUID      string
	Err       error
}

func (err *OperationError) Error() string {
	if strings.HasPrefix(err.Operation, "pickup_payload_invalid_") {
		return fmt.Sprintf("%s:%s:%v", err.Operation, err.Queue, err.Err)
	}
	if err.GUID != "" {
		return fmt.Sprintf("%s_failed:%s:%s:%v", err.Operation, err.Queue, err.GUID, err.Err)
	}
	return fmt.Sprintf("%s_failed:%s:%v", err.Operation, err.Queue, err.Err)
}

func (err *OperationError) Unwrap() error {
	return err.Err
}

type PickedMessage struct {
	GUID    string
	Payload map[string]any
}

type Message struct {
	GUID     string
	Content  string
	Checksum string
}

type DialFunc func(ctx context.Context, network string, address string) (net.Conn, error)

type Client struct {
	address string
	dial    DialFunc
}

func NewClient(address string) (*Client, error) {
	return NewClientWithDialer(address, (&net.Dialer{}).DialContext)
}

func NewClientWithDialer(address string, dial DialFunc) (*Client, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("address is required")
	}
	if dial == nil {
		return nil, errors.New("dial is required")
	}
	return &Client{address: normalizeAddress(address), dial: dial}, nil
}

func (client *Client) EnsureQueue(ctx context.Context, queueName string) error {
	if err := validateQueueName(queueName); err != nil {
		return err
	}
	response, err := client.roundTrip(ctx, commandEnsureQueue, writeString(queueName))
	if err != nil {
		return err
	}
	status, _, err := readStatus(response)
	if err != nil {
		return err
	}
	if status != statusOK {
		return fmt.Errorf("ensure_queue_failed_status:%d", status)
	}
	return nil
}

func (client *Client) AddMessage(ctx context.Context, queueName string, content string) (string, error) {
	if err := validateQueueName(queueName); err != nil {
		return "", err
	}
	if len(content) > MaxMessageSize {
		return "", ErrMessageTooLarge
	}
	checksum := checksum(content)
	payload := writeString(queueName)
	payload = append(payload, writeString(content)...)
	payload = append(payload, writeString(checksum)...)
	response, err := client.roundTrip(ctx, commandAddMessage, payload)
	if err != nil {
		return "", err
	}
	status, offset, err := readStatus(response)
	if err != nil {
		return "", err
	}
	switch status {
	case statusOK:
		guid, _, err := readString(response, offset)
		if err != nil {
			return "", err
		}
		return guid, nil
	case statusInvalidChecksum:
		return "", ErrInvalidChecksum
	case statusMessageTooLarge:
		return "", ErrMessageTooLarge
	case statusInternalError:
		return "", ErrServer
	default:
		return "", fmt.Errorf("add_message_failed_status:%d", status)
	}
}

func (client *Client) PickupMessage(ctx context.Context, queueName string, timeoutSeconds int, waitSeconds int) (Message, error) {
	if err := validateQueueName(queueName); err != nil {
		return Message{}, err
	}
	if timeoutSeconds < 0 {
		return Message{}, errors.New("timeout_seconds must be non-negative")
	}
	if waitSeconds < 0 {
		return Message{}, errors.New("wait_seconds must be non-negative")
	}
	payload := writeString(queueName)
	payload = binary.BigEndian.AppendUint32(payload, uint32(timeoutSeconds))
	if waitSeconds > 0 {
		payload = binary.BigEndian.AppendUint32(payload, uint32(waitSeconds))
	}
	response, err := client.roundTrip(ctx, commandPickupMessage, payload)
	if err != nil {
		return Message{}, err
	}
	status, offset, err := readStatus(response)
	if err != nil {
		return Message{}, err
	}
	switch status {
	case statusOK:
		guid, offset, err := readString(response, offset)
		if err != nil {
			return Message{}, err
		}
		content, offset, err := readString(response, offset)
		if err != nil {
			return Message{}, err
		}
		messageChecksum, _, err := readString(response, offset)
		if err != nil {
			return Message{}, err
		}
		return Message{GUID: guid, Content: content, Checksum: messageChecksum}, nil
	case statusEmptyQueue:
		return Message{}, ErrQueueEmpty
	case statusInternalError:
		return Message{}, ErrServer
	default:
		return Message{}, fmt.Errorf("pickup_message_failed_status:%d", status)
	}
}

func (client *Client) DeleteMessage(ctx context.Context, queueName string, guid string) error {
	if err := validateQueueName(queueName); err != nil {
		return err
	}
	if strings.TrimSpace(guid) == "" {
		return errors.New("guid is required")
	}
	payload := writeString(queueName)
	payload = append(payload, writeString(guid)...)
	response, err := client.roundTrip(ctx, commandDeleteMessage, payload)
	if err != nil {
		return err
	}
	status, _, err := readStatus(response)
	if err != nil {
		return err
	}
	switch status {
	case statusOK:
		return nil
	case statusNotFound:
		return ErrNotFound
	case statusInternalError:
		return ErrServer
	default:
		return fmt.Errorf("delete_message_failed_status:%d", status)
	}
}

func (client *Client) roundTrip(ctx context.Context, command byte, payload []byte) ([]byte, error) {
	ctx = contextOrBackground(ctx)
	conn, err := client.dial(ctx, "tcp", client.address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := writeFrame(conn, command, payload); err != nil {
		return nil, err
	}
	_, response, err := readFrame(conn)
	return response, err
}

type Adapter struct {
	client *Client
}

func NewAdapter(address string) (*Adapter, error) {
	client, err := NewClient(address)
	if err != nil {
		return nil, err
	}
	return &Adapter{client: client}, nil
}

func NewAdapterWithClient(client *Client) (*Adapter, error) {
	if client == nil {
		return nil, errors.New("client is required")
	}
	return &Adapter{client: client}, nil
}

func (adapter *Adapter) EnsureQueue(ctx context.Context, queueName string) error {
	if err := adapter.client.EnsureQueue(ctx, queueName); err != nil {
		return &OperationError{Operation: "ensure_queue", Queue: queueName, Err: err}
	}
	return nil
}

func (adapter *Adapter) PublishJSON(ctx context.Context, queueName string, payload map[string]any) (string, error) {
	if payload == nil {
		return "", &OperationError{Operation: "publish", Queue: queueName, Err: errors.New("payload is required")}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", &OperationError{Operation: "publish", Queue: queueName, Err: err}
	}
	if err := adapter.client.EnsureQueue(ctx, queueName); err != nil {
		return "", &OperationError{Operation: "publish", Queue: queueName, Err: err}
	}
	guid, err := adapter.client.AddMessage(ctx, queueName, string(encoded))
	if err != nil {
		return "", &OperationError{Operation: "publish", Queue: queueName, Err: err}
	}
	return guid, nil
}

func (adapter *Adapter) PickupJSON(ctx context.Context, queueName string, timeoutSeconds int, waitSeconds int) (*PickedMessage, error) {
	message, err := adapter.client.PickupMessage(ctx, queueName, timeoutSeconds, waitSeconds)
	if errors.Is(err, ErrQueueEmpty) {
		return nil, nil
	}
	if err != nil {
		return nil, &OperationError{Operation: "pickup", Queue: queueName, Err: err}
	}
	var payload any
	if err := json.Unmarshal([]byte(message.Content), &payload); err != nil {
		return nil, &OperationError{Operation: "pickup_payload_invalid_json", Queue: queueName, Err: err}
	}
	objectPayload, ok := payload.(map[string]any)
	if !ok {
		return nil, &OperationError{Operation: "pickup_payload_invalid_type", Queue: queueName, Err: errors.New("object_required")}
	}
	return &PickedMessage{GUID: message.GUID, Payload: objectPayload}, nil
}

func (adapter *Adapter) Ack(ctx context.Context, queueName string, guid string) error {
	if err := adapter.client.DeleteMessage(ctx, queueName, guid); err != nil {
		return &OperationError{Operation: "ack", Queue: queueName, GUID: guid, Err: err}
	}
	return nil
}

func normalizeAddress(address string) string {
	address = strings.TrimSpace(address)
	if _, _, err := net.SplitHostPort(address); err == nil {
		return address
	}
	return net.JoinHostPort(address, strconv.Itoa(9876))
}

func validateQueueName(queueName string) error {
	if strings.TrimSpace(queueName) == "" {
		return errors.New("queue_name is required")
	}
	return nil
}

func checksum(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func writeFrame(writer io.Writer, command byte, payload []byte) error {
	length := uint32(len(payload) + 1)
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], length)
	header[4] = command
	if err := writeAll(writer, header); err != nil {
		return err
	}
	return writeAll(writer, payload)
}

func readFrame(reader io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length == 0 {
		return 0, nil, errors.New("invalid frame length")
	}
	payload := make([]byte, int(length)-1)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	return header[4], payload, nil
}

func readStatus(payload []byte) (byte, int, error) {
	if len(payload) < 1 {
		return 0, 0, errors.New("missing response status")
	}
	return payload[0], 1, nil
}

func writeString(value string) []byte {
	encoded := []byte(value)
	result := make([]byte, 4, 4+len(encoded))
	binary.BigEndian.PutUint32(result, uint32(len(encoded)))
	return append(result, encoded...)
}

func readString(data []byte, offset int) (string, int, error) {
	if offset+4 > len(data) {
		return "", offset, errors.New("unexpected end of data")
	}
	length := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if offset+length > len(data) {
		return "", offset, errors.New("unexpected end of data")
	}
	value := string(data[offset : offset+length])
	return value, offset + length, nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
