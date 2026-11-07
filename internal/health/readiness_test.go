package health_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"notification_service/internal/health"
)

func TestReadinessAllHealthy(t *testing.T) {
	t.Parallel()
	readiness := health.New(
		health.Check{Name: "postgres", Ping: func(context.Context) error { return nil }},
		health.Check{Name: "kafka", Ping: func(context.Context) error { return nil }},
		health.Check{Name: "skipped"},
	)
	require.NoError(t, readiness.Ready(context.Background()))
	require.False(t, readiness.ShuttingDown())
}

func TestReadinessReportsDependencyFailures(t *testing.T) {
	t.Parallel()
	readiness := health.New(
		health.Check{Name: "postgres", Ping: func(context.Context) error { return errors.New("connection refused") }},
		health.Check{Name: "kafka", Ping: func(context.Context) error { return errors.New("no brokers") }},
	)
	err := readiness.Ready(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "postgres")
	require.Contains(t, err.Error(), "kafka")
}

func TestReadinessFailsAfterShutdownStarts(t *testing.T) {
	t.Parallel()
	readiness := health.New(health.Check{Name: "postgres", Ping: func(context.Context) error { return nil }})
	readiness.BeginShutdown()
	require.True(t, readiness.ShuttingDown())
	require.ErrorIs(t, readiness.Ready(context.Background()), health.ErrShuttingDown)
}
