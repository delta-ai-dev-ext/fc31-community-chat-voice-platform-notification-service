package event_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"notification_service/internal/event"
	"notification_service/internal/models"
	"notification_service/tests/mocks"
)

// countingDrainer records outbox drains.
type countingDrainer struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (d *countingDrainer) Drain(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	return d.err
}

func (d *countingDrainer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// countingRetrier records email retry sweeps.
type countingRetrier struct {
	mu       sync.Mutex
	calls    int
	lastSize int
	err      error
}

func (r *countingRetrier) RetryDue(_ context.Context, limit int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.lastSize = limit
	return r.err
}

func (r *countingRetrier) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func runWithCancel(t *testing.T, run func(context.Context), wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx)
	}()
	time.Sleep(wait)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance loop did not stop after cancellation")
	}
}

func TestMaintenanceRunsEveryLoop(t *testing.T) {
	t.Parallel()
	drainer := &countingDrainer{}
	retrier := &countingRetrier{}
	processed := &mocks.ProcessedEventRepository{}
	processed.On("Prune", mock.Anything, mock.Anything).Return(int64(3), nil).Maybe()

	maintenance := event.NewMaintenance(event.MaintenanceOptions{
		Outbox:      drainer,
		Emails:      retrier,
		Processed:   processed,
		Logger:      zap.NewNop(),
		OutboxEvery: 5 * time.Millisecond,
		EmailEvery:  5 * time.Millisecond,
		PruneEvery:  5 * time.Millisecond,
		EmailBatch:  17,
	})

	runWithCancel(t, maintenance.Run, 40*time.Millisecond)

	require.GreaterOrEqual(t, drainer.count(), 2, "the outbox is drained periodically")
	require.GreaterOrEqual(t, retrier.count(), 2, "pending emails are retried periodically")
	processed.AssertCalled(t, "Prune", mock.Anything, mock.Anything)

	retrier.mu.Lock()
	defer retrier.mu.Unlock()
	require.Equal(t, 17, retrier.lastSize)
}

func TestMaintenanceAppliesDefaults(t *testing.T) {
	t.Parallel()
	maintenance := event.NewMaintenance(event.MaintenanceOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		maintenance.Run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance must stop when the context is already cancelled")
	}
}

func TestMaintenanceToleratesFailures(t *testing.T) {
	t.Parallel()
	drainer := &countingDrainer{err: errors.New("broker unavailable")}
	retrier := &countingRetrier{err: errors.New("db down")}
	processed := &mocks.ProcessedEventRepository{}
	processed.On("Prune", mock.Anything, mock.Anything).Return(int64(0), errors.New("db down")).Maybe()

	maintenance := event.NewMaintenance(event.MaintenanceOptions{
		Outbox:      drainer,
		Emails:      retrier,
		Processed:   processed,
		Logger:      zap.NewNop(),
		OutboxEvery: 5 * time.Millisecond,
		EmailEvery:  5 * time.Millisecond,
		PruneEvery:  5 * time.Millisecond,
	})

	runWithCancel(t, maintenance.Run, 40*time.Millisecond)
	require.GreaterOrEqual(t, drainer.count(), 2, "a failed sweep must not stop the loop")
}

func TestMaintenanceWithoutDependencies(t *testing.T) {
	t.Parallel()
	maintenance := event.NewMaintenance(event.MaintenanceOptions{
		Logger:      zap.NewNop(),
		OutboxEvery: 5 * time.Millisecond,
		EmailEvery:  5 * time.Millisecond,
		PruneEvery:  5 * time.Millisecond,
	})
	runWithCancel(t, maintenance.Run, 20*time.Millisecond)
}

func TestMaintenanceDefaultsAreSane(t *testing.T) {
	t.Parallel()
	maintenance := event.NewMaintenance(event.MaintenanceOptions{
		Logger: zap.NewNop(),
	})
	require.NotNil(t, maintenance)
	_ = models.NewUUID()
}
