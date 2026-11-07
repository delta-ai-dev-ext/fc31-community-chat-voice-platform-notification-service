package event_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/event"
	"notification_service/internal/models"
)

// fakeLagSource returns canned lag values.
type fakeLagSource struct {
	lags map[string]int64
	err  error
}

func (s fakeLagSource) Lag(context.Context) (map[string]int64, error) {
	return s.lags, s.err
}

// recordingLagReporter captures lag reports.
type recordingLagReporter struct {
	mu      sync.Mutex
	reports map[string]float64
}

func (r *recordingLagReporter) SetConsumerLag(topic, consumerGroup string, lag float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports[topic+"/"+consumerGroup] = lag
}

func TestLagRefresherReportsEveryTopic(t *testing.T) {
	t.Parallel()
	reporter := &recordingLagReporter{reports: map[string]float64{}}
	source := fakeLagSource{lags: map[string]int64{
		models.TopicIdentityEvents:   1,
		models.TopicModerationEvents: 2,
		models.TopicSocialEvents:     3,
		models.TopicMessageEvents:    4,
	}}

	refresher := event.NewLagRefresher(source, reporter, "notification_service", models.ConsumedTopics, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	refresher.Run(ctx, 5*time.Millisecond)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	require.Len(t, reporter.reports, len(models.ConsumedTopics))
	require.Equal(t, float64(4), reporter.reports[models.TopicMessageEvents+"/notification_service"])
}

func TestLagRefresherSurvivesSourceErrors(t *testing.T) {
	t.Parallel()
	reporter := &recordingLagReporter{reports: map[string]float64{}}
	source := fakeLagSource{err: errors.New("broker unavailable")}

	refresher := event.NewLagRefresher(source, reporter, "group", models.ConsumedTopics, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(15 * time.Millisecond)
		cancel()
	}()
	refresher.Run(ctx, 0)

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	require.Empty(t, reporter.reports, "a failed refresh reports nothing rather than a wrong lag")
}
