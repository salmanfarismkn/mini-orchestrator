package controller

import (
	"context"
	"fmt"
	"log/slog"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type DeploymentController struct {
	store *store.Postgres
}

func NewDeploymentController(
	store *store.Postgres,
) *DeploymentController {
	return &DeploymentController{
		store: store,
	}
}

func (c *DeploymentController) ReconcileService(
	ctx context.Context,
	service model.Service,
) error {
	workloads, err := c.store.ListWorkloadsByService(
		ctx,
		service.ID,
	)
	if err != nil {
		return fmt.Errorf("list service workloads: %w", err)
	}

	oldWorkloads := make([]model.Workload, 0)

	for _, workload := range workloads {
		if workload.DeploymentVersion < service.DeploymentVersion &&
			workload.ActualState != model.WorkloadStopped &&
			workload.ActualState != model.WorkloadFailed {

			oldWorkloads = append(oldWorkloads, workload)
		}
	}

	if len(oldWorkloads) == 0 {
		return nil
	}

	// Replace only one replica per reconciliation cycle.
	workload := oldWorkloads[0]

	if err := c.store.UpdateWorkloadDesiredState(
		ctx,
		workload.ID,
		model.WorkloadStopped,
	); err != nil {
		return fmt.Errorf(
			"stop old workload %q: %w",
			workload.ID,
			err,
		)
	}

	slog.Info(
		"rolling deployment replacing workload",
		"service_id", service.ID,
		"workload_id", workload.ID,
		"old_version", workload.DeploymentVersion,
		"new_version", service.DeploymentVersion,
	)

	return nil
}