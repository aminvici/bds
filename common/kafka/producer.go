package kafka

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type Producer struct {
	clients        []*kgo.Client
	messageChannel chan *Message
	errorChannel   chan *MessageError
	stopChannel    chan bool
	ctx            context.Context
	cancel         context.CancelFunc
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
	producer.clients = make([]*kgo.Client, 0)
	producer.messageChannel = make(chan *Message, cfg.BufferSize)
	producer.errorChannel = make(chan *MessageError, cfg.BufferSize)
	producerNum := 1
	if cfg.ProducerNum > 1 {
		producerNum = cfg.ProducerNum
	}

	producer.stopChannel = make(chan bool, producerNum)
	producer.ctx, producer.cancel = context.WithCancel(context.Background())

	for i := 0; i < producerNum; i++ {
		opts := []kgo.Opt{
			kgo.SeedBrokers(cfg.BrokerList),
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.ProducerLinger(time.Millisecond * time.Duration(cfg.FlushFrequency)),
			kgo.ProducerBatchMaxBytes(int32(cfg.FlushMaxMessages)),
			kgo.RequestTimeoutOverhead(time.Millisecond * time.Duration(cfg.Timeout)),
			kgo.RequestRetries(3),
		}

		client, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, err
		}
		producer.clients = append(producer.clients, client)
	}

	return producer, nil
}

func (p *Producer) MessageChannel() chan *Message {
	return p.messageChannel
}

func (p *Producer) ErrorChannel() <-chan *MessageError {
	return p.errorChannel
}

func (p *Producer) Start() {
	for _, client := range p.clients {
		go p.receiveMessages(client)
	}
}

func (p *Producer) Stop() {
	for i := 0; i < len(p.clients); i++ {
		p.stopChannel <- true
	}
	p.cancel()
	for _, client := range p.clients {
		client.Flush(p.ctx)
		client.Close()
	}
}

func (p *Producer) receiveMessages(client *kgo.Client) {
	for {
		select {
		case msg := <-p.messageChannel:
			record := &kgo.Record{
				Topic: msg.Topic,
				Value: msg.Data,
			}
			if msg.Key != nil {
				record.Key = msg.Key
			}

			client.Produce(p.ctx, record, func(r *kgo.Record, err error) {
				if err != nil {
					p.errorChannel <- &MessageError{
						Error:     err,
						Topic:     r.Topic,
						Timestamp: r.Timestamp,
						Partition: r.Partition,
						Offset:    r.Offset,
					}
				}
			})
		case stop := <-p.stopChannel:
			if stop {
				return
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
