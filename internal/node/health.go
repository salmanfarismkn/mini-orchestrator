package node

import (
	"context"
	"log/slog"
	"time"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type HealthChecker struct {
	store    *store.Postgres
	interval time.Duration
	timeout  time.Duration
}

func NewHealthChecker(
	store *store.Postgres,
	interval time.Duration,
	timeout time.Duration,
) *HealthChecker {
	return &HealthChecker{
		store:    store,
		interval: interval,
		timeout:  timeout,
	}
}

func (h *HealthChecker) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		h.check(ctx)

		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
		}
	}
}

func (h *HealthChecker) check(ctx context.Context) {
	nodes, err := h.store.ListNodes(ctx)
	if err != nil {
		slog.Error("node health check failed", "error", err)
		return
	}

	now := time.Now().UTC()
	heartbeatCutoff := now.Add(-h.timeout)

	for _, node := range nodes {
		if node.Status == model.NodeDraining {
			continue
		}
		if !node.LastHeartbeat.IsZero() &&
			now.Sub(node.LastHeartbeat) <= h.timeout {
			continue
		}

		markedUnhealthy, err := h.store.MarkNodeUnhealthyIfHeartbeatBefore(
			ctx,
			node.ID,
			heartbeatCutoff,
		)
		if err != nil {
			slog.Error(
				"failed to mark node unhealthy",
				"node_id", node.ID,
				"error", err,
			)
			continue
		}
		if markedUnhealthy && node.Status != model.NodeUnhealthy {
			slog.Warn(
				"node marked unhealthy",
				"node_id", node.ID,
				"state", model.NodeUnhealthy,
			)
		}
	}
}
