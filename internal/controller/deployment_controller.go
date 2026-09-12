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

	oldWorkloads := oldActiveWorkloads(
		workloads,
		service.DeploymentVersion,
	)

	if len(oldWorkloads) == 0 {
		return nil
	}

	newWorkloads := newActiveWorkloads(
		workloads,
		service.DeploymentVersion,
	)

	totalActive := len(oldWorkloads) + len(newWorkloads)

	maxTotal := service.DesiredReplicas + service.MaxSurge

	// First create a replacement if we have surge capacity.
	if totalActive < maxTotal {
		if err := c.createReplacementWorkload(
			ctx,
			service,
		); err != nil {
			return err
		}

		slog.Info(
			"rolling deployment created replacement",
			"service_id", service.ID,
			"deployment_version", service.DeploymentVersion,
		)

		return nil
	}

	// We have enough new capacity. Now remove one old replica.
	old := oldWorkloads[0]

	if err := c.store.UpdateWorkloadDesiredState(
		ctx,
		old.ID,
		model.WorkloadStopped,
	); err != nil {
		return fmt.Errorf(
			"stop old workload %q: %w",
			old.ID,
			err,
		)
	}

	slog.Info(
		"rolling deployment stopping old workload",
		"service_id", service.ID,
		"workload_id", old.ID,
		"old_version", old.DeploymentVersion,
		"new_version", service.DeploymentVersion,
	)

	return nil
}

func oldActiveWorkloads(
	workloads []model.Workload,
	currentVersion int,
) []model.Workload {
	var result []model.Workload

	for _, workload := range workloads {
		if workload.DeploymentVersion >= currentVersion {
			continue
		}

		if isActive(workload) {
			result = append(result, workload)
		}
	}

	return result
}

func newActiveWorkloads(
	workloads []model.Workload,
	currentVersion int,
) []model.Workload {
	var result []model.Workload

	for _, workload := range workloads {
		if workload.DeploymentVersion != currentVersion {
			continue
		}

		if isActive(workload) {
			result = append(result, workload)
		}
	}

	return result
}

func isActive(workload model.Workload) bool {
	switch workload.ActualState {
	case model.WorkloadPending,
		model.WorkloadScheduled,
		model.WorkloadRunning:
		return workload.DesiredState != model.WorkloadStopped

	default:
		return false
	}
}

func (c *DeploymentController) createReplacementWorkload(
	ctx context.Context,
	service model.Service,
) error {
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

	if err := c.store.CreatePendingWorkload(
		ctx,
		workload,
	); err != nil {
		return fmt.Errorf(
			"create replacement workload: %w",
			err,
		)
	}

	return nil
}