package event

import (
	"context"
	"time"

	"go.uber.org/zap"

	"notification_service/internal/repository"
)

// EmailRetrier retries pending notification email deliveries.
type EmailRetrier interface {
	RetryDue(ctx context.Context, limit int) error
}

// OutboxDrainer publishes pending EVT-011 outbox rows.
type OutboxDrainer interface {
	Drain(ctx context.Context) error
}

// MaintenanceOptions are the dependencies of the periodic maintenance loops.
type MaintenanceOptions struct {
	Outbox      OutboxDrainer
	Emails      EmailRetrier
	Processed   repository.ProcessedEventRepository
	Logger      *zap.Logger
	OutboxEvery time.Duration
	EmailEvery  time.Duration
	PruneEvery  time.Duration
	PruneBefore time.Duration
	EmailBatch  int
}

// Maintenance runs the periodic background jobs of the service: outbox drain,
// email retry sweep and processed-event retention.
type Maintenance struct {
	outbox      OutboxDrainer
	emails      EmailRetrier
	processed   repository.ProcessedEventRepository
	logger      *zap.Logger
	outboxEvery time.Duration
	emailEvery  time.Duration
	pruneEvery  time.Duration
	pruneBefore time.Duration
	emailBatch  int
}

// Default intervals of the maintenance loops.
const (
	DefaultOutboxInterval     = time.Second
	DefaultEmailInterval      = 10 * time.Second
	DefaultPruneInterval      = time.Hour
	DefaultProcessedRetention = 7 * 24 * time.Hour
)

// NewMaintenance builds the maintenance runner.
func NewMaintenance(opts MaintenanceOptions) *Maintenance {
	maintenance := &Maintenance{
		outbox:      opts.Outbox,
		emails:      opts.Emails,
		processed:   opts.Processed,
		logger:      opts.Logger,
		outboxEvery: opts.OutboxEvery,
		emailEvery:  opts.EmailEvery,
		pruneEvery:  opts.PruneEvery,
		pruneBefore: opts.PruneBefore,
		emailBatch:  opts.EmailBatch,
	}
	if maintenance.logger == nil {
		maintenance.logger = zap.NewNop()
	}
	if maintenance.outboxEvery <= 0 {
		maintenance.outboxEvery = DefaultOutboxInterval
	}
	if maintenance.emailEvery <= 0 {
		maintenance.emailEvery = DefaultEmailInterval
	}
	if maintenance.pruneEvery <= 0 {
		maintenance.pruneEvery = DefaultPruneInterval
	}
	if maintenance.pruneBefore <= 0 {
		maintenance.pruneBefore = DefaultProcessedRetention
	}
	if maintenance.emailBatch <= 0 {
		maintenance.emailBatch = 50
	}
	return maintenance
}

// Run starts every loop and blocks until ctx is cancelled.
func (m *Maintenance) Run(ctx context.Context) {
	done := make(chan struct{}, 3)
	go func() {
		m.loop(ctx, m.outboxEvery, m.drainOutbox)
		done <- struct{}{}
	}()
	go func() {
		m.loop(ctx, m.emailEvery, m.retryEmails)
		done <- struct{}{}
	}()
	go func() {
		m.loop(ctx, m.pruneEvery, m.pruneProcessed)
		done <- struct{}{}
	}()
	for i := 0; i < 3; i++ {
		<-done
	}
}

func (m *Maintenance) loop(ctx context.Context, interval time.Duration, job func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job(ctx)
		}
	}
}

func (m *Maintenance) drainOutbox(ctx context.Context) {
	if m.outbox == nil {
		return
	}
	if err := m.outbox.Drain(ctx); err != nil {
		m.logger.Warn("outbox drain failed",
			zap.String("service_name", "notification_service"),
			zap.String("operation", "drain_notification_outbox"),
			zap.Error(err),
		)
	}
}

func (m *Maintenance) retryEmails(ctx context.Context) {
	if m.emails == nil {
		return
	}
	if err := m.emails.RetryDue(ctx, m.emailBatch); err != nil {
		m.logger.Warn("email retry sweep failed",
			zap.String("service_name", "notification_service"),
			zap.String("operation", "retry_notification_emails"),
			zap.Error(err),
		)
	}
}

func (m *Maintenance) pruneProcessed(ctx context.Context) {
	if m.processed == nil {
		return
	}
	removed, err := m.processed.Prune(ctx, time.Now().UTC().Add(-m.pruneBefore))
	if err != nil {
		m.logger.Warn("processed-event pruning failed",
			zap.String("service_name", "notification_service"),
			zap.String("operation", "prune_processed_events"),
			zap.Error(err),
		)
		return
	}
	if removed > 0 {
		m.logger.Info("processed-event pruning completed",
			zap.String("service_name", "notification_service"),
			zap.String("operation", "prune_processed_events"),
			zap.Int64("removed", removed),
		)
	}
}
