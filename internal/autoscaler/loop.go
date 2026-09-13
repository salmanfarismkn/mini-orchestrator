package autoscaler

import (
	"context"
	"log/slog"
	"time"

	"mini-orchestrator/internal/store"
)

type Loop struct {
	store      *store.Postgres
	autoscaler *Autoscaler
	interval   time.Duration
}

func NewLoop(
	store *store.Postgres,
	autoscaler *Autoscaler,
	interval time.Duration,
) *Loop {
	return &Loop{
		store:      store,
		autoscaler: autoscaler,
		interval:   interval,
	}
}

func (l *Loop) Run(ctx context.Context) {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			l.reconcile(ctx)
		}
	}
}

func (l *Loop) reconcile(ctx context.Context) {
	services, err := l.store.ListServices(ctx)
	if err != nil {
		slog.Error(
			"autoscaler failed to list services",
			"error", err,
		)
		return
	}

	for _, service := range services {
		if err := l.autoscaler.ReconcileService(
			ctx,
			service,
		); err != nil {
			slog.Error(
				"autoscaling failed",
				"service_id", service.ID,
				"error", err,
			)
		}
	}
}
