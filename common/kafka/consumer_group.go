package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/aminvici/bds/common/log"
	"strings"
)

type ConsumerGroup struct {
	clusterConfig  *cluster.Config
	brokerList     []string
	groupID        string
	consumer       *cluster.Consumer
	messageChannel chan *Message
	errorChannel   chan *MessageError
	stopChannel    chan bool
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
	consumerGroup.clusterConfig = cluster.NewConfig()
	consumerGroup.clusterConfig.Consumer.Return.Errors = cfg.ReturnErrors
	consumerGroup.clusterConfig.ClientID = cfg.ClientID

	consumerGroup.brokerList = strings.Split(cfg.BrokerList, ",")
	consumerGroup.groupID = cfg.GroupID
	consumerGroup.messageChannel = make(chan *Message, cfg.BufferSize)
	consumerGroup.errorChannel = make(chan *MessageError, cfg.BufferSize)
	consumerGroup.stopChannel = make(chan bool)

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
	var err error
	c.consumer, err = cluster.NewConsumer(c.brokerList, c.groupID, []string{topic}, c.clusterConfig)
	if err != nil {
		return err
	}

	go c.receiveMessages()
	if c.clusterConfig.Consumer.Return.Errors {
		go c.receiveErrors()
	}
	return nil
}

func (c *ConsumerGroup) Stop() {
	_ = c.consumer.Close()
	c.stopChannel <- true
	if c.clusterConfig.Consumer.Return.Errors {
		c.stopChannel <- true
	}
}

func (c *ConsumerGroup) MarkOffset(msg *Message) {
	c.consumer.MarkOffset(
		&sarama.ConsumerMessage{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}, "")
}

func (c *ConsumerGroup) receiveMessages() {
	for {
		select {
		case msg := <-c.consumer.Messages():
			log.Debug("kafka: topic %s receive data on partition %d offset %d length %d",
				msg.Topic, msg.Partition, msg.Offset, len(msg.Value))
			c.messageChannel <- &Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Data:      msg.Value,
			}
		case stop := <-c.stopChannel:
			if stop {
				return
			}
		}
	}
}

func (c *ConsumerGroup) receiveErrors() {
	for {
		select {
		case msg := <-c.consumer.Errors():
			log.Debug("kafka: receive error %s", msg.Error())
			c.errorChannel <- &MessageError{
				Error: msg,
			}
		case stop := <-c.stopChannel:
			if stop {
				return
			}
		}
	}
}
