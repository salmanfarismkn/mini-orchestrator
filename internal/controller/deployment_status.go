package controller

import (
	"context"
	"fmt"
	"log/slog"
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
	if service.DeploymentStatus == model.DeploymentFailed {
		return nil
	}

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

	oldActive := 0
	newRunning := 0
	newFailed := 0

	for _, workload := range workloads {
		if workload.DeploymentVersion < service.DeploymentVersion {
			if isActive(workload) {
				oldActive++
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
		oldActive == 0 {
		if err := c.store.UpdateDeploymentStatus(
			ctx,
			service.ID,
			model.DeploymentAvailable,
		); err != nil {
			return err
		}
		if service.DeploymentStatus != model.DeploymentAvailable {
			slog.Info(
				"deployment completed",
				"service_id", service.ID,
				"deployment_version", service.DeploymentVersion,
				"state", model.DeploymentAvailable,
			)
		}
		return nil
	}

	// A current-version workload failing means the rollout
	// cannot currently reach the desired state.
	if newFailed > 0 {
		if service.DeploymentStatus != model.DeploymentFailed {
			slog.Error(
				"deployment failed",
				"service_id", service.ID,
				"deployment_version", service.DeploymentVersion,
				"state", model.DeploymentFailed,
				"error", "current-version workload failed",
			)
		}
		return c.store.UpdateDeploymentStatus(
			ctx,
			service.ID,
			model.DeploymentFailed,
		)
	}

	// Check for rollout timeout.
	if service.DeploymentStartedAt != nil &&
		time.Since(*service.DeploymentStartedAt) > c.timeout {
		if service.DeploymentStatus != model.DeploymentFailed {
			slog.Error(
				"deployment failed",
				"service_id", service.ID,
				"deployment_version", service.DeploymentVersion,
				"state", model.DeploymentFailed,
				"error", "deployment timed out",
			)
		}

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
