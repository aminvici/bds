package kafka

import (
	"context"
	"github.com/aminvici/bds/common/log"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"
)

type ConsumerGroup struct {
	reader         *kafka.Reader
	brokerList     []string
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
	consumerGroup.brokerList = strings.Split(cfg.BrokerList, ",")
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

	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokerList,
		GroupID:        c.groupID,
		Topic:          topic,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		SessionTimeout: 6 * time.Second,
	})

	go c.receiveMessages()
	return nil
}

func (c *ConsumerGroup) Stop() {
	c.cancel()
	if c.reader != nil {
		_ = c.reader.Close()
	}
	close(c.stopChannel)
}

func (c *ConsumerGroup) MarkOffset(msg *Message) {
	// segmentio/kafka-go handles offset management automatically
	// when CommitInterval is set in ReaderConfig
	log.Debug("kafka: marking offset for topic %s partition %d offset %d",
		msg.Topic, msg.Partition, msg.Offset)
}

func (c *ConsumerGroup) receiveMessages() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := c.reader.ReadMessage(c.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Debug("kafka: receive error %v", err)
				c.errorChannel <- &MessageError{
					Error: err,
					Topic: msg.Topic,
				}
				continue
			}

			log.Debug("kafka: topic %s receive data on partition %d offset %d length %d",
				msg.Topic, msg.Partition, msg.Offset, len(msg.Value))
			c.messageChannel <- &Message{
				Topic:     msg.Topic,
				Partition: int32(msg.Partition),
				Offset:    msg.Offset,
				Key:       msg.Key,
				Data:      msg.Value,
			}
		}
	}
}
