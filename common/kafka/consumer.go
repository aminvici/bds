package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/aminvici/bds/common/log"
	"strings"
)

type Consumer struct {
	kafkaConfig    *sarama.Config
	consumer       sarama.Consumer
	messageChannel chan *Message
	errorChannel   chan *MessageError
	stopChannel    chan bool
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
	brokers := cfg.BrokerList
	consumer.kafkaConfig = sarama.NewConfig()
	consumer.kafkaConfig.Consumer.Return.Errors = cfg.ReturnErrors
	consumer.kafkaConfig.ClientID = cfg.ClientID
	c, err := sarama.NewConsumer(strings.Split(brokers, ","), consumer.kafkaConfig)
	if err != nil {
		return nil, err
	}

	consumer.messageChannel = make(chan *Message, cfg.BufferSize)
	consumer.errorChannel = make(chan *MessageError, cfg.BufferSize)
	consumer.stopChannel = make(chan bool)
	consumer.consumer = c

	return consumer, nil
}

func (c *Consumer) MessageChannel() chan *Message {
	return c.messageChannel
}

func (c *Consumer) ErrorChannel() <-chan *MessageError {
	return c.errorChannel
}

func (c *Consumer) Start(topic string) error {
	partitions, err := c.consumer.Partitions(topic)
	log.Debug("kafka: topic %s partitions %v", topic, partitions)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		pc, err := c.consumer.ConsumePartition(topic, p, sarama.OffsetNewest)
		if err != nil {
			return err
		}
		go c.receiveMessages(pc)
		if c.kafkaConfig.Consumer.Return.Errors {
			go c.receiveErrors(pc)
		}
	}
	c.partitionNum = len(partitions)
	return nil
}

func (c *Consumer) Stop() {
	_ = c.consumer.Close()
	for i := 0; i < c.partitionNum; i++ {
		c.stopChannel <- true
		if c.kafkaConfig.Consumer.Return.Errors {
			c.stopChannel <- true
		}
	}
}

func (c *Consumer) receiveMessages(consumer sarama.PartitionConsumer) {
	for {
		select {
		case msg := <-consumer.Messages():
			log.Debug("kafka: topic %s receive data length %d", msg.Topic, len(msg.Value))
			c.messageChannel <- &Message{
				Topic: msg.Topic,
				Data:  msg.Value,
			}
		case stop := <-c.stopChannel:
			if stop {
				return
			}
		}
	}
}

func (c *Consumer) receiveErrors(consumer sarama.PartitionConsumer) {
	for {
		select {
		case msg := <-consumer.Errors():
			log.Debug("kafka: topic %s partition %d receive error %s", msg.Topic, msg.Partition, msg.Err.Error())
			c.errorChannel <- &MessageError{
				Error:     msg.Err,
				Topic:     msg.Topic,
				Partition: msg.Partition,
			}
		case stop := <-c.stopChannel:
			if stop {
				return
			}
		}
	}
}
