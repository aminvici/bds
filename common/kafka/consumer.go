package kafka

import (
	"context"
	"fmt"
	"github.com/aminvici/bds/common/log"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"
)

type Consumer struct {
	reader         *kafka.Reader
	messageChannel chan *Message
	errorChannel   chan *MessageError
	stopChannel    chan bool
	ctx            context.Context
	cancel         context.CancelFunc
	partitionNum   int
	brokers        []string
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
	consumer.brokers = strings.Split(cfg.BrokerList, ",")

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
	// Get broker list from context or configuration
	// We'll need to store it in the struct
	cfg := c.getReaderConfig(topic)
	c.reader = kafka.NewReader(cfg)

	// Get partition count for the topic
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to dial broker: %v", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return fmt.Errorf("failed to read partitions: %v", err)
	}

	c.partitionNum = len(partitions)
	log.Debug("kafka: topic %s partitions %d", topic, c.partitionNum)

	go c.receiveMessages()
	return nil
}

func (c *Consumer) getReaderConfig(topic string) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:        c.brokers,
		Topic:          topic,
		StartOffset:    kafka.LastOffset,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	}
}

func (c *Consumer) Stop() {
	c.cancel()
	if c.reader != nil {
		_ = c.reader.Close()
	}
	close(c.stopChannel)
}

func (c *Consumer) receiveMessages() {
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

			log.Debug("kafka: topic %s receive data length %d", msg.Topic, len(msg.Value))
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
