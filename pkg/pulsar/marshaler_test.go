// Sources for https://watermill.io/docs/getting-started/
package pulsar

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
	//"github.com/apache/pulsar-client-go/pulsar"
)

func TestDefaultMarshaler_MarshalUnmarshal(t *testing.T) {
	m := DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	_, err := m.Marshal("topic", msg)
	require.NoError(t, err)

}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	for i := 0; i < b.N; i++ {
		m.Marshal("foo", msg)
	}
}
