package kafka

import (
	"context"
	"github.com/aminvici/bds/common/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client         *kgo.Client
	brokerList     string
	messageChannel chan *Message
	errorChannel   chan *MessageError
	stopChannel    chan bool
	ctx            context.Context
	cancel         context.CancelFunc
	partitionNum   int
}

type ConsumerConfig struct {
	ClientID         string
	BrokerList       string
	BufferSize       int
	ConsumerNum      int
	FlushMessages    int
	FlushFrequency   int
	FlushMaxMessages int
	Timeout          int
	ReturnErrors     bool
}

func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	consumer := new(Consumer)
	consumer.brokerList = cfg.BrokerList
	consumer.messageChannel = make(chan *Message, cfg.BufferSize)
	consumer.errorChannel = make(chan *MessageError, cfg.BufferSize)
	consumer.stopChannel = make(chan bool)
	consumer.ctx, consumer.cancel = context.WithCancel(context.Background())

	return consumer, nil
}

func (c *Consumer) MessageChannel() chan *Message {
	return c.messageChannel
}

func (c *Consumer) ErrorChannel() <-chan *MessageError {
	return c.errorChannel
}

func (c *Consumer) Start(topic string) error {
	// Get broker list from a temporary client to fetch metadata
	tempOpts := []kgo.Opt{
		kgo.SeedBrokers(c.brokerList),
		kgo.RequestRetries(3),
	}
	tempClient, err := kgo.NewClient(tempOpts...)
	if err != nil {
		return err
	}

	// Get partition count
	admClient := kadm.NewClient(tempClient)
	metadata, err := admClient.Metadata(c.ctx)
	if err != nil {
		tempClient.Close()
		return err
	}

	if topicMeta, ok := metadata.Topics[topic]; ok {
		c.partitionNum = len(topicMeta.Partitions)
	}
	tempClient.Close()

	log.Debug("kafka: topic %s partitions %d", topic, c.partitionNum)

	// Create the actual consumer client
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.brokerList),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.RequestRetries(3),
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	c.client = client

	go c.receiveMessages()
	return nil
}

func (c *Consumer) Stop() {
	c.cancel()
	if c.client != nil {
		c.client.Close()
	}
	for i := 0; i < c.partitionNum; i++ {
		c.stopChannel <- true
	}
}

func (c *Consumer) receiveMessages() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.stopChannel:
			return
		default:
			fetches := c.client.PollFetches(c.ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					log.Debug("kafka: topic %s partition %d receive error %s", err.Topic, err.Partition, err.Err.Error())
					c.errorChannel <- &MessageError{
						Error:     err.Err,
						Topic:     err.Topic,
						Partition: err.Partition,
					}
				}
			}

			fetches.EachRecord(func(record *kgo.Record) {
				log.Debug("kafka: topic %s receive data length %d", record.Topic, len(record.Value))
				c.messageChannel <- &Message{
					Topic:     record.Topic,
					Partition: record.Partition,
					Offset:    record.Offset,
					Key:       record.Key,
					Data:      record.Value,
				}
			})
		}
	}
}
