// Sources for https://watermill.io/docs/getting-started/
package pulsar

import (
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/pkg/errors"
)

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed bool
}

// NewSubscriber creates a new pulsar Subscriber.
func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": watermill.NewShortUUID(),
	})

	return &Subscriber{
		config: config,
		logger: logger,

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	Brokers                []string
	Unmarshaler            Unmarshaler
	ConsumerGroup          string
	NackResendSleep        time.Duration
	ReconnectRetrySleep    time.Duration
	InitializeTopicDetails *pulsar.ReaderOptions
}

func (c *SubscriberConfig) setDefaults() {

	if c.NackResendSleep == 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Unmarshaler == nil {
		return errors.New("missing unmarshaler")
	}

	return nil
}
