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
	if service.Status != model.ServiceActive {
		return nil
	}

	workloads, err := c.store.ListWorkloadsByService(ctx, service.ID)
	if err != nil {
		return fmt.Errorf("list workloads for service %q: %w", service.ID, err)
	}

	activeReplicas := countActiveReplicas(workloads, service.DeploymentVersion)

	// Deployment-in-progress check
	if hasOldActiveWorkloads(workloads, service.DeploymentVersion) {
		return nil
	}

	// Scale up
	if activeReplicas < service.DesiredReplicas {
		missing := service.DesiredReplicas - activeReplicas

		for i := 0; i < missing; i++ {
			workload := model.Workload{
				ID:                newWorkloadID(),
				ServiceID:         service.ID,
				Image:             service.Image,
				CPURequestMillis:  service.CPURequestMillis,
				MemoryRequestMB:   service.MemoryRequestMB,
				DeploymentVersion: service.DeploymentVersion,
				DesiredState:      model.WorkloadPending,
				ActualState:       model.WorkloadPending,
			}

			if err := c.store.CreatePendingWorkload(ctx, workload); err != nil {
				return fmt.Errorf("create pending workload: %w", err)
			}
		}
		return nil
	}

	// Desired count already satisfied
	if activeReplicas == service.DesiredReplicas {
		return nil
	}

	// Scale down
	excess := activeReplicas - service.DesiredReplicas
	candidates := selectScaleDownCandidates(workloads, excess)

	// Count how many new workloads are already running
	newRunning := 0
	for _, w := range workloads {
		if w.DeploymentVersion == service.DeploymentVersion &&
			w.ActualState == model.WorkloadRunning {
			newRunning++
		}
	}

	// Enforce maxUnavailable = 0
	if newRunning == 0 {
		return nil
	}

	for _, workload := range candidates {
		if err := c.store.UpdateWorkloadDesiredState(
			ctx,
			workload.ID,
			model.WorkloadStopped,
		); err != nil {
			return fmt.Errorf("mark workload %q for shutdown: %w", workload.ID, err)
		}
	}

	return nil
}

func countActiveReplicas(
	workloads []model.Workload,
	currentVersion int,
) int {
	count := 0

	for _, workload := range workloads {
		if workload.DeploymentVersion != 0 &&
			workload.DeploymentVersion != currentVersion {
			continue
		}

		switch workload.ActualState {
		case model.WorkloadPending,
			model.WorkloadScheduled,
			model.WorkloadRunning:

			if workload.DesiredState != model.WorkloadStopped {
				count++
			}
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

func hasOldActiveWorkloads(
	workloads []model.Workload,
	currentVersion int,
) bool {
	for _, workload := range workloads {
		if workload.DeploymentVersion < currentVersion &&
			isActive(workload) {
			return true
		}
	}

	return false
}
