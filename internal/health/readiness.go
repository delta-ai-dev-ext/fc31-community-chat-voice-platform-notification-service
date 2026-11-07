// Package health implements the readiness semantics of GET /readyz: dependency
// connectivity plus the shutdown latch.
package health

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// ErrShuttingDown is reported by Ready once graceful shutdown has started.
var ErrShuttingDown = errors.New("service is shutting down")

// Check is one named dependency readiness probe.
type Check struct {
	// Name identifies the dependency in the readyz error detail.
	Name string
	// Ping returns nil when the dependency is usable.
	Ping func(ctx context.Context) error
}

// Readiness aggregates dependency checks and a shutdown latch.
type Readiness struct {
	checks       []Check
	shuttingDown atomic.Bool
}

// New builds a Readiness over the given checks (evaluated in order).
func New(checks ...Check) *Readiness {
	return &Readiness{checks: checks}
}

// BeginShutdown flips the readiness latch so the load balancer stops routing.
func (r *Readiness) BeginShutdown() { r.shuttingDown.Store(true) }

// ShuttingDown reports whether shutdown has started.
func (r *Readiness) ShuttingDown() bool { return r.shuttingDown.Load() }

// Ready implements handler.ReadyChecker.
func (r *Readiness) Ready(ctx context.Context) error {
	if r.shuttingDown.Load() {
		return ErrShuttingDown
	}
	var failures []string
	for _, check := range r.checks {
		if check.Ping == nil {
			continue
		}
		if err := check.Ping(ctx); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", check.Name, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("dependency check failed: %s", strings.Join(failures, "; "))
	}
	return nil
}
