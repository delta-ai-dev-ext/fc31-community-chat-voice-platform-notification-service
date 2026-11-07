package event_test

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
)

// fakeConsumerGroup replays a session and records the topics it consumed.
type fakeConsumerGroup struct {
	handler     sarama.ConsumerGroupHandler
	sessions    int
	topics      []string
	mu          sync.Mutex
	closeCalled bool
	consumeErr  error
	// onConsume runs after every session (tests use it to cancel the context so
	// the re-join loop terminates).
	onConsume func()
}

func (g *fakeConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	g.mu.Lock()
	g.sessions++
	g.topics = topics
	g.mu.Unlock()
	g.handler = handler
	if g.onConsume != nil {
		g.onConsume()
	}
	if g.consumeErr != nil {
		return g.consumeErr
	}
	return nil
}

// sessionCount returns how often Consume was called.
func (g *fakeConsumerGroup) sessionCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sessions
}

func (g *fakeConsumerGroup) Errors() <-chan error { return make(chan error) }

func (g *fakeConsumerGroup) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeCalled = true
	return nil
}

func (g *fakeConsumerGroup) Pause(map[string][]int32)  {}
func (g *fakeConsumerGroup) Resume(map[string][]int32) {}
func (g *fakeConsumerGroup) PauseAll()                 {}
func (g *fakeConsumerGroup) ResumeAll()                {}

// fakeSession implements sarama.ConsumerGroupSession.
type fakeSession struct {
	ctx       context.Context
	marked    []*sarama.ConsumerMessage
	commits   int
	committed []*sarama.ConsumerMessage
}

func newFakeSession(ctx context.Context) *fakeSession {
	return &fakeSession{ctx: ctx}
}

func (s *fakeSession) Claims() map[string][]int32 { return map[string][]int32{"social_events": {0}} }

func (s *fakeSession) MemberID() string { return "member-1" }

func (s *fakeSession) GenerationID() int32 { return 1 }

func (s *fakeSession) MarkOffset(string, int32, int64, string) {}

func (s *fakeSession) Commit() {
	s.commits++
	if len(s.marked) > len(s.committed) {
		s.committed = append(s.committed, s.marked[len(s.committed):]...)
	}
}

func (s *fakeSession) ResetOffset(string, int32, int64, string) {}

func (s *fakeSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, message)
}

func (s *fakeSession) Context() context.Context { return s.ctx }

// fakeClaim implements sarama.ConsumerGroupClaim.
type fakeClaim struct {
	topic     string
	messages  chan *sarama.ConsumerMessage
	highWater int64
}

func newFakeClaim(topic string, messages ...*sarama.ConsumerMessage) *fakeClaim {
	claim := &fakeClaim{topic: topic, messages: make(chan *sarama.ConsumerMessage, len(messages)+1)}
	for _, message := range messages {
		claim.messages <- message
	}
	return claim
}

func (c *fakeClaim) Topic() string                            { return c.topic }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return c.highWater }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func (c *fakeClaim) close() { close(c.messages) }
