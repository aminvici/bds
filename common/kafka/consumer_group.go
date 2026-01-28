package kafka

import (
	"context"
	"github.com/aminvici/bds/common/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumerGroup struct {
	client         *kgo.Client
	brokerList     string
	groupID        string
	messageChannel chan *Message
	errorChannel   chan *MessageError
	stopChannel    chan bool
	ctx            context.Context
	cancel         context.CancelFunc
}

type ConsumerGroupConfig struct {
	BrokerList   string
	BufferSize   int
	ClientID     string
	GroupID      string
	ReturnErrors bool
}

func NewConsumerGroup(cfg *ConsumerGroupConfig) (*ConsumerGroup, error) {
	consumerGroup := new(ConsumerGroup)
	consumerGroup.brokerList = cfg.BrokerList
	consumerGroup.groupID = cfg.GroupID
	consumerGroup.messageChannel = make(chan *Message, cfg.BufferSize)
	consumerGroup.errorChannel = make(chan *MessageError, cfg.BufferSize)
	consumerGroup.stopChannel = make(chan bool)
	consumerGroup.ctx, consumerGroup.cancel = context.WithCancel(context.Background())

	return consumerGroup, nil
}

func (c *ConsumerGroup) MessageChannel() chan *Message {
	return c.messageChannel
}

func (c *ConsumerGroup) ErrorChannel() <-chan *MessageError {
	return c.errorChannel
}

func (c *ConsumerGroup) Start(topic string) error {
	log.Debug("kafka: topic %s", topic)

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.brokerList),
		kgo.ConsumerGroup(c.groupID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
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

func (c *ConsumerGroup) Stop() {
	c.cancel()
	if c.client != nil {
		c.client.Close()
	}
	c.stopChannel <- true
}

func (c *ConsumerGroup) MarkOffset(msg *Message) {
	if c.client == nil {
		return
	}

	log.Debug("kafka: marking offset for topic %s partition %d offset %d",
		msg.Topic, msg.Partition, msg.Offset)

	// Create a record to mark the offset
	record := &kgo.Record{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}

	c.client.MarkCommitRecords(record)
}

func (c *ConsumerGroup) receiveMessages() {
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
					log.Debug("kafka: receive error %s", err.Err.Error())
					c.errorChannel <- &MessageError{
						Error: err.Err,
						Topic: err.Topic,
					}
				}
			}

			fetches.EachRecord(func(record *kgo.Record) {
				log.Debug("kafka: topic %s receive data on partition %d offset %d length %d",
					record.Topic, record.Partition, record.Offset, len(record.Value))
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
