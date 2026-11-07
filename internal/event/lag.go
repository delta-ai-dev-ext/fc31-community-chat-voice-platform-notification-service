package event

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// LagSource reports the total consumer lag per topic.
type LagSource interface {
	// Lag returns the pending message count per consumed topic.
	Lag(ctx context.Context) (map[string]int64, error)
}

// LagReporter receives the refreshed lag values.
type LagReporter interface {
	// SetConsumerLag publishes the lag of one topic.
	SetConsumerLag(topic, consumerGroup string, lag float64)
}

// LagRefresher periodically refreshes kafka_consumer_lag{topic,consumer_group}.
type LagRefresher struct {
	source   LagSource
	reporter LagReporter
	group    string
	topics   []string
	logger   *zap.Logger
}

// NewLagRefresher builds a LagRefresher for the consumed topics.
func NewLagRefresher(source LagSource, reporter LagReporter, group string, topics []string, logger *zap.Logger) *LagRefresher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &LagRefresher{source: source, reporter: reporter, group: group, topics: topics, logger: logger}
}

// Run refreshes the lag until ctx is cancelled.
func (r *LagRefresher) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r.refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refresh(ctx)
		}
	}
}

func (r *LagRefresher) refresh(ctx context.Context) {
	lags, err := r.source.Lag(ctx)
	if err != nil {
		r.logger.Warn("consumer lag refresh failed",
			zap.String("service_name", "notification_service"),
			zap.String("operation", "refresh_consumer_lag"),
			zap.Error(err),
		)
		return
	}
	for _, topic := range r.topics {
		r.reporter.SetConsumerLag(topic, r.group, float64(lags[topic]))
	}
}

// SaramaLagSource computes lag from a sarama client and the group's committed
// offsets.
type SaramaLagSource struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
	group  string
	topics []string

	mu  sync.Mutex
	now func() time.Time
}

// NewSaramaLagSource builds a LagSource for the consumer group over topics.
func NewSaramaLagSource(client sarama.Client, admin sarama.ClusterAdmin, group string, topics []string) *SaramaLagSource {
	return &SaramaLagSource{client: client, admin: admin, group: group, topics: topics, now: func() time.Time { return time.Now().UTC() }}
}

// Lag implements LagSource: lag = newest offset - committed offset, floored at
// zero, summed over the topic's partitions.
func (s *SaramaLagSource) Lag(_ context.Context) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := make(map[string][]int32, len(s.topics))
	for _, topic := range s.topics {
		found, err := s.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		partitions[topic] = found
	}

	committed := make(map[string]map[int32]int64, len(s.topics))
	response, err := s.admin.ListConsumerGroupOffsets(s.group, partitions)
	if err != nil {
		return nil, err
	}
	if response != nil {
		for topic, blocks := range response.Blocks {
			committed[topic] = make(map[int32]int64, len(blocks))
			for partition, block := range blocks {
				if block == nil {
					continue
				}
				committed[topic][partition] = block.Offset
			}
		}
	}

	lags := make(map[string]int64, len(s.topics))
	for topic, topicPartitions := range partitions {
		var total int64
		for _, partition := range topicPartitions {
			newest, err := s.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}
			offset, ok := committed[topic][partition]
			if !ok || offset < 0 {
				// Nothing committed yet: the whole partition is pending.
				total += newest
				continue
			}
			if lag := newest - offset; lag > 0 {
				total += lag
			}
		}
		lags[topic] = total
	}
	return lags, nil
}
