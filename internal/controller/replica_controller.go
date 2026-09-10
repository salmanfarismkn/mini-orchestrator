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


func (c *ReplicaController) ReconcileService(
	ctx context.Context,
	service model.Service,
) error {
	workloads, err := c.store.ListWorkloadsByService(ctx, service.ID)
	if err != nil {
		return fmt.Errorf(
			"list workloads for service %q: %w",
			service.ID,
			err,
		)
	}

	activeReplicas := countActiveReplicas(workloads)

	// Scale up.
	if activeReplicas < service.DesiredReplicas {
		missing := service.DesiredReplicas - activeReplicas

		for i := 0; i < missing; i++ {
			workload := model.Workload{
				ID:               newWorkloadID(),
				ServiceID:        service.ID,
				Image:            service.Image,
				CPURequestMillis: service.CPURequestMillis,
				MemoryRequestMB: service.MemoryRequestMB,
				DeploymentVersion: service.DeploymentVersion,
				DesiredState:     model.WorkloadPending,
				ActualState:      model.WorkloadPending,
			}

			if err := c.store.CreatePendingWorkload(
				ctx,
				workload,
			); err != nil {
				return fmt.Errorf(
					"create pending workload: %w",
					err,
				)
			}
		}

		return nil
	}

	// Desired count already satisfied.
	if activeReplicas == service.DesiredReplicas {
		return nil
	}

	// Scale down.
	excess := activeReplicas - service.DesiredReplicas

	candidates := selectScaleDownCandidates(
		workloads,
		excess,
	)

	for _, workload := range candidates {
		if err := c.store.UpdateWorkloadDesiredState(
			ctx,
			workload.ID,
			model.WorkloadStopped,
		); err != nil {
			return fmt.Errorf(
				"mark workload %q for shutdown: %w",
				workload.ID,
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

func selectScaleDownCandidates(
	workloads []model.Workload,
	count int,
) []model.Workload {
	var candidates []model.Workload

	// Work backwards so newer workloads are removed first.
	for i := len(workloads) - 1; i >= 0 && len(candidates) < count; i-- {
		workload := workloads[i]

		switch workload.ActualState {
		case model.WorkloadPending,
			model.WorkloadScheduled,
			model.WorkloadRunning:

			if workload.DesiredState != model.WorkloadStopped {
				candidates = append(candidates, workload)
			}
		}
	}

	return candidates
}