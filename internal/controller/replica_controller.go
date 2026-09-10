package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type ReplicaController struct {
	store *store.Postgres
}

func NewReplicaController(store *store.Postgres) *ReplicaController {
	return &ReplicaController{
		store: store,
	}
}

// ReconcileService makes the number of active workloads
// match the service's desired replica count.
func (c *ReplicaController) ReconcileService(
	ctx context.Context,
	service model.Service,
) error {
	workloads, err := c.store.ListWorkloadsByService(ctx, service.ID)
	if err != nil {
		return fmt.Errorf("list workloads for service %q: %w", service.ID, err)
	}

	activeReplicas := countActiveReplicas(workloads)

	if activeReplicas >= service.DesiredReplicas {
		return nil
	}

	missing := service.DesiredReplicas - activeReplicas

	for i := 0; i < missing; i++ {
		workload := model.Workload{
			ID:                 newWorkloadID(),
			ServiceID:          service.ID,
			Image:              service.Image,
			CPURequestMillis:   service.CPURequestMillis,
			MemoryRequestMB:    service.MemoryRequestMB,
			DesiredState:       model.WorkloadPending,
			ActualState:        model.WorkloadPending,
		}

		if err := c.store.CreatePendingWorkload(ctx, workload); err != nil {
			return fmt.Errorf(
				"create pending workload for service %q: %w",
				service.ID,
				err,
			)
		}
	}

	return nil
}

func countActiveReplicas(workloads []model.Workload) int {
	count := 0

	for _, workload := range workloads {
		switch workload.ActualState {
		case model.WorkloadPending,
			model.WorkloadScheduled,
			model.WorkloadRunning:
			count++
		}
	}

	return count
}

func newWorkloadID() string {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("generate workload ID: %v", err))
	}

	return "wl-" + hex.EncodeToString(b[:])
}