package event_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/event"
	"notification_service/internal/models"
)

// recordingHandler records the messages handed to the consumer pipeline.
type recordingHandler struct {
	mu       sync.Mutex
	messages []string
	err      error
}

func (h *recordingHandler) HandleMessage(_ context.Context, topic string, raw []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, topic+":"+string(raw))
	return h.err
}

func TestConsumerConsumesAllTopicsInOneGroup(t *testing.T) {
	t.Parallel()
	group := &fakeConsumerGroup{}
	handler := &recordingHandler{}
	consumer := event.NewConsumerFromGroup(group, models.ConsumedTopics, handler, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group.onConsume = cancel

	require.NoError(t, consumer.Run(ctx))
	require.Equal(t, 1, group.sessionCount())
	require.Equal(t, []string{"identity_events", "moderation_events", "social_events", "message_events"}, group.topics)
}

func TestConsumerCommitsOffsetAfterSuccessfulProcessing(t *testing.T) {
	t.Parallel()
	group := &fakeConsumerGroup{}
	handler := &recordingHandler{}
	consumer := event.NewConsumerFromGroup(group, models.ConsumedTopics, handler, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx))

	session := newFakeSession(context.Background())
	claim := newFakeClaim(models.TopicSocialEvents,
		&sarama.ConsumerMessage{Topic: models.TopicSocialEvents, Value: []byte("first"), Offset: 7},
		&sarama.ConsumerMessage{Topic: models.TopicSocialEvents, Value: []byte("second"), Offset: 8},
	)
	claim.close()

	require.NoError(t, group.handler.ConsumeClaim(session, claim))
	require.Len(t, handler.messages, 2)
	require.Equal(t, 2, session.commits, "each accepted message is committed")
	require.Len(t, session.committed, 2)
	require.Equal(t, int64(7), session.committed[0].Offset)
	require.Equal(t, int64(8), session.committed[1].Offset)
}

func TestConsumerDoesNotCommitFailedMessages(t *testing.T) {
	t.Parallel()
	group := &fakeConsumerGroup{}
	handler := &recordingHandler{err: errors.New("postgres is down")}
	consumer := event.NewConsumerFromGroup(group, models.ConsumedTopics, handler, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx))

	session := newFakeSession(context.Background())
	claim := newFakeClaim(models.TopicMessageEvents,
		&sarama.ConsumerMessage{Topic: models.TopicMessageEvents, Value: []byte("poison"), Offset: 3},
	)
	claim.close()

	err := group.handler.ConsumeClaim(session, claim)
	require.Error(t, err, "a failed message ends the session so it is redelivered")
	require.Zero(t, session.commits)
	require.Empty(t, session.committed)
}

func TestConsumerStopsOnContextCancellation(t *testing.T) {
	t.Parallel()
	group := &fakeConsumerGroup{}
	consumer := event.NewConsumerFromGroup(group, models.ConsumedTopics, &recordingHandler{}, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx))

	cancelled, cancelSession := context.WithCancel(context.Background())
	cancelSession()
	session := newFakeSession(cancelled)
	claim := newFakeClaim(models.TopicIdentityEvents)
	claim.close()
	require.NoError(t, group.handler.ConsumeClaim(session, claim))
	require.Zero(t, session.commits)
}

func TestConsumerClose(t *testing.T) {
	t.Parallel()
	group := &fakeConsumerGroup{}
	consumer := event.NewConsumerFromGroup(group, models.ConsumedTopics, &recordingHandler{}, nil)
	require.NoError(t, consumer.Close())
	require.True(t, group.closeCalled)

	empty := event.NewConsumerFromGroup(nil, nil, &recordingHandler{}, nil)
	require.NoError(t, empty.Close())
}

func TestConsumerRejoinsAfterSessionError(t *testing.T) {
	t.Parallel()
	var (
		mu       sync.Mutex
		sessions int
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group := &fakeConsumerGroup{consumeErr: errors.New("rebalance")}
	group.onConsume = func() {
		mu.Lock()
		sessions++
		enough := sessions >= 3
		mu.Unlock()
		if enough {
			cancel()
		}
	}
	consumer := event.NewConsumerFromGroup(group, models.ConsumedTopics, &recordingHandler{}, zap.NewNop())

	require.NoError(t, consumer.Run(ctx))
	require.GreaterOrEqual(t, group.sessionCount(), 3, "the consumer rejoins the group after a failed session")
}

func TestGroupHandlerSetupAndCleanup(t *testing.T) {
	t.Parallel()
	group := &fakeConsumerGroup{}
	consumer := event.NewConsumerFromGroup(group, models.ConsumedTopics, &recordingHandler{}, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group.onConsume = cancel
	require.NoError(t, consumer.Run(ctx))

	session := newFakeSession(context.Background())
	require.NoError(t, group.handler.Setup(session))
	require.NoError(t, group.handler.Cleanup(session))
}
