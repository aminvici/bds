package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
)

type Producer struct {
	producer       *kafka.Producer
	messageChannel chan *Message
	errorChannel   chan *MessageError
	stopChannel    chan bool
}

type ProducerConfig struct {
	BrokerList       string
	BufferSize       int
	ProducerNum      int
	FlushMessages    int
	FlushFrequency   int
	FlushMaxMessages int
	Timeout          int
	ReturnErrors     bool
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Data      []byte
}

type MessageError struct {
	Error     error
	Topic     string
	Timestamp time.Time
	Partition int32
	Offset    int64
	Metadata  interface{}
}

func NewProducer(cfg *ProducerConfig) (*Producer, error) {
	producer := new(Producer)
	producer.messageChannel = make(chan *Message, cfg.BufferSize)
	producer.errorChannel = make(chan *MessageError, cfg.BufferSize)
	producer.stopChannel = make(chan bool, 1)

	configMap := kafka.ConfigMap{
		"bootstrap.servers": cfg.BrokerList,
		"acks": "local",
		"linger.ms": cfg.FlushFrequency,
	}

	p, err := kafka.NewProducer(&configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %v", err)
	}

	producer.producer = p
	return producer, nil
}

func (p *Producer) MessageChannel() chan *Message {
	return p.messageChannel
}

func (p *Producer) ErrorChannel() <-chan *MessageError {
	return p.errorChannel
}

func (p *Producer) Start() {
	go p.receiveMessages()
	go p.receiveDeliveryReports()
}

func (p *Producer) Stop() {
	p.stopChannel <- true
	// Flush any remaining messages
	remaining := p.producer.Flush(10000)
	if remaining > 0 {
		fmt.Printf("Warning: %d messages in queue were not delivered\n", remaining)
	}
	p.producer.Close()
}

func (p *Producer) receiveMessages() {
	for {
		select {
		case msg := <-p.messageChannel:
			partition := kafka.PartitionAny
			if msg.Partition != 0 {
				partition = msg.Partition
			}
			
			kmsg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &msg.Topic,
					Partition: partition,
				},
				Key:   msg.Key,
				Value: msg.Data,
			}
			
			p.producer.ProduceChannel() <- kmsg
		case stop := <-p.stopChannel:
			if stop {
				return
			}
		}
	}
}

func (p *Producer) receiveDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.errorChannel <- &MessageError{
					Error:     ev.TopicPartition.Error,
					Topic:     *ev.TopicPartition.Topic,
					Timestamp: ev.Timestamp,
					Partition: ev.TopicPartition.Partition,
					Offset:    int64(ev.TopicPartition.Offset),
				}
			}
		case kafka.Error:
			p.errorChannel <- &MessageError{
				Error: ev,
			}
		}
	}
}

func (p *Producer) Send(topic string, data []byte) {
	msg := &Message{
		Topic: topic,
		Data:  data,
	}
	p.messageChannel <- msg
}
