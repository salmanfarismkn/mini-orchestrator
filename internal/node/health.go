package node

import (
	"context"
	"log"
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
		log.Printf("node health check failed: %v", err)
		return
	}

	now := time.Now().UTC()

	for _, node := range nodes {
		if node.LastHeartbeat.IsZero() {
			continue
		}

		if now.Sub(node.LastHeartbeat) > h.timeout {
			if node.Status != model.NodeUnhealthy {
				log.Printf(
					"node %s marked unhealthy",
					node.ID,
				)

				if err := h.store.SetNodeStatus(
					ctx,
					node.ID,
					model.NodeUnhealthy,
				); err != nil {
					log.Printf(
						"failed to mark node %s unhealthy: %v",
						node.ID,
						err,
					)
				}
			}
		}
	}
}
