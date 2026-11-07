// Package event wires the Kafka consumer group, the EVT-011 producer and the
// transactional-outbox publisher of notification_service.
package event

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// MessageHandler processes one consumed message. A nil return commits the
// offset; a non-nil return retries the message (the offset is not committed).
type MessageHandler interface {
	HandleMessage(ctx context.Context, topic string, raw []byte) error
}

// Consumer reads every consumed topic in a single consumer group with explicit
// offset commits after successful processing.
type Consumer struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler MessageHandler
	logger  *zap.Logger
}

// NewConsumer builds a consumer group over brokers. The group id is shared by
// all replicas; every topic is consumed by the same group.
func NewConsumer(brokers []string, groupID string, topics []string, handler MessageHandler, logger *zap.Logger) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	// Explicit commits keep the at-least-once semantics of the pipeline: an
	// offset is committed only after the local transaction (or no-op drop).
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("event: create consumer group: %w", err)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Consumer{group: group, topics: topics, handler: handler, logger: logger}, nil
}

// NewConsumerFromGroup builds a Consumer over an existing sarama consumer group.
// It is used by tests with a fake group.
func NewConsumerFromGroup(group sarama.ConsumerGroup, topics []string, handler MessageHandler, logger *zap.Logger) *Consumer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Consumer{group: group, topics: topics, handler: handler, logger: logger}
}

// Run consumes until ctx is cancelled, re-joining the group after every
// rebalance.
func (c *Consumer) Run(ctx context.Context) error {
	handler := &groupHandler{handler: c.handler, logger: c.logger}
	for {
		if err := c.group.Consume(ctx, c.topics, handler); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			c.logger.Error("consumer group session failed",
				zap.String("service_name", "notification_service"),
				zap.String("operation", "consume_events"),
				zap.Error(err),
			)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Close closes the consumer group, releasing the group membership.
func (c *Consumer) Close() error {
	if c.group == nil {
		return nil
	}
	return c.group.Close()
}

type groupHandler struct {
	handler MessageHandler
	logger  *zap.Logger
}

// Setup implements sarama.ConsumerGroupHandler.
func (g *groupHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (g *groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim implements sarama.ConsumerGroupHandler. Offsets are committed
// only after the handler accepted the message.
func (g *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := g.handle(session.Context(), message); err != nil {
				// Do not commit: end the session so the message is redelivered.
				g.logger.Error("event processing failed, offset not committed",
					zap.String("service_name", "notification_service"),
					zap.String("operation", "consume_event"),
					zap.String("topic", message.Topic),
					zap.Int32("partition", message.Partition),
					zap.Int64("offset", message.Offset),
					zap.Error(err),
				)
				return err
			}
			session.MarkMessage(message, "")
			session.Commit()
		}
	}
}

func (g *groupHandler) handle(ctx context.Context, message *sarama.ConsumerMessage) error {
	return g.handler.HandleMessage(ctx, message.Topic, message.Value)
}
