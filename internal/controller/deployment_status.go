package controller

import (
	"context"
	"fmt"
	"time"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type DeploymentStatusController struct {
	store   *store.Postgres
	timeout time.Duration
}

func NewDeploymentStatusController(
	store *store.Postgres,
	timeout time.Duration,
) *DeploymentStatusController {
	return &DeploymentStatusController{
		store:   store,
		timeout: timeout,
	}
}

func (c *DeploymentStatusController) ReconcileService(
	ctx context.Context,
	service model.Service,
) error {
	workloads, err := c.store.ListWorkloadsByService(
		ctx,
		service.ID,
	)
	if err != nil {
		return fmt.Errorf(
			"list workloads: %w",
			err,
		)
	}

	oldRunning := 0
	newRunning := 0
	newFailed := 0

	for _, workload := range workloads {
		if workload.DeploymentVersion < service.DeploymentVersion {
			if workload.ActualState == model.WorkloadRunning {
				oldRunning++
			}

			continue
		}

		if workload.DeploymentVersion > service.DeploymentVersion {
			continue
		}

		switch workload.ActualState {
		case model.WorkloadRunning:
			newRunning++

		case model.WorkloadFailed:
			newFailed++
		}
	}

	// Rollout is complete when every desired replica
	// is running on the current version and no old replicas remain.
	if newRunning == service.DesiredReplicas &&
		oldRunning == 0 {

		return c.store.UpdateDeploymentStatus(
			ctx,
			service.ID,
			model.DeploymentAvailable,
		)
	}

	// A current-version workload failing means the rollout
	// cannot currently reach the desired state.
	if newFailed > 0 {
		return c.store.UpdateDeploymentStatus(
			ctx,
			service.ID,
			model.DeploymentFailed,
		)
	}

	// Check for rollout timeout.
	if service.DeploymentStartedAt != nil &&
		time.Since(*service.DeploymentStartedAt) > c.timeout {

		return c.store.UpdateDeploymentStatus(
			ctx,
			service.ID,
			model.DeploymentFailed,
		)
	}

	return c.store.UpdateDeploymentStatus(
		ctx,
		service.ID,
		model.DeploymentProgressing,
	)
}
