// Sources for https://watermill.io/docs/getting-started/
package pulsar

import (
	"time"

	log "github.com/sirupsen/logrus"

	"unsafe"

	"github.com/ThreeDotsLabs/watermill"

	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/pulsar-client-go/pulsar"
)

type Publisher struct {
	config     PublisherConfig
	myproducer pulsar.Producer
	logger     watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new Publisher.
func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://127.0.0.1:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Pulsar client: %v", err)
	}
	myproducer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-partitioned-topic",
	})

	return &Publisher{
		config:     config,
		myproducer: myproducer,
		logger:     logger,
	}, nil
}

type PublisherConfig struct {
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into format.
	Marshaler Marshaler

	OverwritePulsarConfig *pulsar.ProducerOptions
}

func (c *PublisherConfig) setDefaults() {
	if c.OverwritePulsarConfig == nil {

	}

}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

	return nil
}

func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range msgs {
		if msg != nil {
			logFields["message_uuid"] = msg.UUID
			p.logger.Trace("Sending message to pulsar", logFields)

			_, err := p.config.Marshaler.Marshal(topic, msg)
			if err != nil {
				return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
			}

			p.logger.Trace("Message sent to pulsar", logFields)
		}

	}
	return nil
}

type producer struct {
	sync.RWMutex
	client        *pulsar.Client
	options       *pulsar.ProducerOptions
	topic         string
	producers     []pulsar.Producer
	producersPtr  unsafe.Pointer
	numPartitions uint32
	messageRouter func(*pulsar.ProducerMessage, pulsar.TopicMetadata) int
	ticker        *time.Ticker
	tickerStop    chan struct{}

	log *log.Entry
}

func (p *producer) getPartition(msg *pulsar.ProducerMessage) pulsar.Producer {

	partition := p.numPartitions
	producers := *(*[]pulsar.Producer)(atomic.LoadPointer(&p.producersPtr))

	return producers[partition]
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	p.myproducer.Close()

	return nil
}
