// Sources for https://watermill.io/docs/getting-started/
package pulsar

import (
	//"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

//Getenv retrieves the value of the environment variable named by the key.
//pulsar is the broker, 127.0.0.1: localhost (the IP address), 6650: port broker

func PulsarBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_PULSAR_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, "pulsar://127.0.0.1:6650")
	}
	return []string{"pulsar://127.0.0.1:6650"}
}
func TestConnectionPulsar(t *testing.T) {

	PulsarBrokers()

	t.Logf("Test Connection pulsar Ok\n")

}

func newPubSub(t *testing.T, consumerGroup string) (*Publisher, *Subscriber) {
	var marshaler MarshalerUnmarshaler
	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher *Publisher

	retriesLeft := 5
	for {
		publisher, err = NewPublisher(PublisherConfig{
			Brokers:   PulsarBrokers(),
			Marshaler: marshaler,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot createPublisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	var subscriber *Subscriber

	retriesLeft = 5
	for {
		subscriber, err = NewSubscriber(
			SubscriberConfig{
				Brokers:     PulsarBrokers(),
				Unmarshaler: marshaler,

				ConsumerGroup: consumerGroup,
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) (*Publisher, *Subscriber) {
	return newPubSub(t, consumerGroup)
}

func createPubSub(t *testing.T) (*Publisher, *Subscriber) {
	return createPubSubWithConsumerGrup(t, "test")
}

// func TestPublishSubscribe(t *testing.T) {
// 	features := tests.Features{
// 		ConsumerGroups:      true,
// 		ExactlyOnceDelivery: false,
// 		GuaranteedOrder:     false,
// 		Persistent:          true,
// 	}
// 	fmt.Printf("features %+v \n", features)
// }

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		nil, /*createPartitionedPubSub*/
		createPubSubWithConsumerGrup,
	)
}
func BenchmarkSubscriber(b *testing.B) {
	tests.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := NewPublisher(PublisherConfig{
			Brokers:   PulsarBrokers(),
			Marshaler: DefaultMarshaler{},
		}, logger)
		if err != nil {
			panic(err)
		}
		return publisher, nil
	})
}
