package heartbeat

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/EdwardSalkeld/chatting/go/handler/internal/contracts"
)

const (
	InternalSource           = "internal"
	InternalReplyChannelType = "internal"
	InternalHeartbeatTarget  = "heartbeat"
)

type NowFunc func() time.Time

type Connector struct {
	now      NowFunc
	sequence int
}

func New(now NowFunc) *Connector {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Connector{now: now}
}

func (connector *Connector) Poll(ctx context.Context) ([]contracts.TaskEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connector.sequence++
	return []contracts.TaskEnvelope{
		BuildEnvelope(connector.sequence, connector.now()),
	}, nil
}

func BuildEnvelope(sequence int, now time.Time) contracts.TaskEnvelope {
	currentTime := now.UTC()
	heartbeatID := "internal-heartbeat:" + pythonUTCISO(currentTime) + ":" + strconv.Itoa(sequence)
	actor := "message-handler"
	return contracts.TaskEnvelope{
		SchemaVersion: contracts.SchemaVersion,
		ID:            heartbeatID,
		Source:        InternalSource,
		ReceivedAt:    contracts.NewTimestamp(currentTime),
		Actor:         &actor,
		Content:       "internal heartbeat ping",
		Attachments:   []contracts.AttachmentRef{},
		ContextRefs:   []string{},
		ReplyChannel: contracts.ReplyChannel{
			Type:   InternalReplyChannelType,
			Target: InternalHeartbeatTarget,
		},
		DedupeKey: heartbeatID,
	}
}

func IsEnvelope(envelope contracts.TaskEnvelope) bool {
	return envelope.Source == InternalSource &&
		envelope.ReplyChannel.Type == InternalReplyChannelType &&
		envelope.ReplyChannel.Target == InternalHeartbeatTarget
}

func IsLogPong(message contracts.OutboundMessage, envelope contracts.TaskEnvelope) bool {
	return IsEnvelope(envelope) &&
		message.Channel == "log" &&
		message.Target == InternalHeartbeatTarget
}

func pythonUTCISO(value time.Time) string {
	return strings.TrimSuffix(value.Format(time.RFC3339Nano), "Z") + "+00:00"
}
