package event

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

// EventProducer publishes encoded event messages to a topic.
type EventProducer interface {
	// Publish sends one message with its key and mirrored envelope headers.
	Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error
	// Close flushes pending messages and releases the broker connections.
	Close() error
}

// SaramaProducer is the EventProducer implementation backed by a sync sarama
// producer with idempotent, all-replica-acknowledged writes.
type SaramaProducer struct {
	producer sarama.SyncProducer
}

// NewSaramaProducer builds a producer for brokers.
func NewSaramaProducer(brokers []string) (*SaramaProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("event: create producer: %w", err)
	}
	return &SaramaProducer{producer: producer}, nil
}

// NewSaramaProducerFrom builds a SaramaProducer over an existing sync producer.
func NewSaramaProducerFrom(producer sarama.SyncProducer) *SaramaProducer {
	return &SaramaProducer{producer: producer}
}

// Publish implements EventProducer.
func (p *SaramaProducer) Publish(_ context.Context, topic string, key []byte, value []byte, headers map[string]string) error {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	for name, headerValue := range headers {
		message.Headers = append(message.Headers, sarama.RecordHeader{
			Key:   []byte(name),
			Value: []byte(headerValue),
		})
	}
	if _, _, err := p.producer.SendMessage(message); err != nil {
		return fmt.Errorf("event: publish to %s: %w", topic, err)
	}
	return nil
}

// Close implements EventProducer.
func (p *SaramaProducer) Close() error {
	if p.producer == nil {
		return nil
	}
	return p.producer.Close()
}

var _ EventProducer = (*SaramaProducer)(nil)
