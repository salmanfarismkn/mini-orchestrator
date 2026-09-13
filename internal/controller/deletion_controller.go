package controller

import (
	"context"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type DeletionController struct {
	store *store.Postgres
}

func NewDeletionController(s *store.Postgres) *DeletionController {
	return &DeletionController{
		store: s,
	}
}

func (c *DeletionController) ReconcileService(
	ctx context.Context,
	service model.Service,
) error {
	if service.Status != model.ServiceDeleting {
		return nil
	}

	workloads, err := c.store.ListWorkloadsByService(ctx, service.ID)
	if err != nil {
		return err
	}

	allFinished := true

	for _, workload := range workloads {
		switch workload.ActualState {
		case model.WorkloadRunning,
			model.WorkloadScheduled,
			model.WorkloadPending:

			allFinished = false

			if workload.DesiredState != model.WorkloadStopped {
				if err := c.store.UpdateWorkloadDesiredState(
					ctx,
					workload.ID,
					model.WorkloadStopped,
				); err != nil {
					return err
				}
			}

		case model.WorkloadStopped,
			model.WorkloadFailed:
			// Nothing to do.
		}
	}

	if !allFinished {
		return nil
	}

	return c.store.MarkServiceDeleted(ctx, service.ID)
}
