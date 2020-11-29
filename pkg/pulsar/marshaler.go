// Sources for https://watermill.io/docs/getting-started/
package pulsar

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
)

const UUIDHeaderKey = "_watermill_message_uuid"

type Payload []byte
type contextKey int

const (
	_ contextKey = iota
	partitionContextKey
	partitionOffsetContextKey
	timestampContextKey
)

// Marshaler marshals Watermill's message to message.
type Marshaler interface {
	Marshal(Key string, msg *message.Message) (*pulsar.ProducerMessage, error)
}

// Unmarshaler unmarshals message to Watermill's message.
type Unmarshaler interface {
	Unmarshal(*pulsar.ConsumerMessage) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

type DefaultMarshaler struct{}
type RecordHeader struct {
	Key     []byte
	Payload []byte
}

func (DefaultMarshaler) Marshal(Key string, msg *message.Message) (*pulsar.ProducerMessage, error) {
	if payload := msg.Metadata.Get(UUIDHeaderKey); payload != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	headers := []RecordHeader{{
		Key:     []byte(UUIDHeaderKey),
		Payload: []byte(msg.UUID),
	}}
	for key, Payload := range msg.Metadata {
		headers = append(headers, RecordHeader{
			Key:     []byte(key),
			Payload: []byte(Payload),
		})
	}

	return &pulsar.ProducerMessage{
		Key:     "",
		Payload: []byte{},
	}, nil
}

func setPartitionToCtx(ctx context.Context, partition int32) context.Context {
	return context.WithValue(ctx, partitionContextKey, partition)
}

// MessagePartitionFromCtx returns  partition of the consumed message
func MessagePartitionFromCtx(ctx context.Context) (int32, bool) {
	partition, ok := ctx.Value(partitionContextKey).(int32)
	return partition, ok
}

func setPartitionOffsetToCtx(ctx context.Context, offset int64) context.Context {
	return context.WithValue(ctx, partitionOffsetContextKey, offset)
}

// MessagePartitionOffsetFromCtx returns partition offset of the consumed message
func MessagePartitionOffsetFromCtx(ctx context.Context) (int64, bool) {
	offset, ok := ctx.Value(partitionOffsetContextKey).(int64)
	return offset, ok
}

func setMessageTimestampToCtx(ctx context.Context, timestamp time.Time) context.Context {
	return context.WithValue(ctx, timestampContextKey, timestamp)
}

// MessageTimestampFromCtx returns internal timestamp of the consumed message
func MessageTimestampFromCtx(ctx context.Context) (time.Time, bool) {
	timestamp, ok := ctx.Value(timestampContextKey).(time.Time)
	return timestamp, ok
}
