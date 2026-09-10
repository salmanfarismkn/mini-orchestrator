package reconciler

import (
	"context"
	"log/slog"
	"time"

	"mini-orchestrator/internal/controller"
	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/scheduler"
	"mini-orchestrator/internal/store"
)

type Reconciler struct {
	store             *store.Postgres
	replicaController *controller.ReplicaController
	schedulerService  *scheduler.Service
	interval          time.Duration
}

func New(
	store *store.Postgres,
	replicaController *controller.ReplicaController,
	schedulerService *scheduler.Service,
	interval time.Duration,
) *Reconciler {
	return &Reconciler{
		store:             store,
		replicaController: replicaController,
		schedulerService:  schedulerService,
		interval:          interval,
	}
}

func (r *Reconciler) listServices(ctx context.Context) ([]model.Service, error) {
	return r.store.ListServices(ctx)
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Reconcile immediately when the control plane starts.
	r.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

func (r *Reconciler) reconcile(ctx context.Context) {
	services, err := r.listServices(ctx)
	if err != nil {
		slog.Error("reconciliation failed to list services", "error", err)
		return
	}

	for _, service := range services {
		if err := r.replicaController.ReconcileService(ctx, service); err != nil {
			slog.Error(
				"replica reconciliation failed",
				"service_id", service.ID,
				"error", err,
			)
			continue
		}

		if err := r.schedulePendingWorkloads(ctx, service); err != nil {
			slog.Error(
				"workload scheduling failed",
				"service_id", service.ID,
				"error", err,
			)
		}
	}
}

func (r *Reconciler) schedulePendingWorkloads(
	ctx context.Context,
	service model.Service,
) error {
	workloads, err := r.store.ListWorkloadsByService(ctx, service.ID)
	if err != nil {
		return err
	}

	for _, workload := range workloads {
		if workload.ActualState != model.WorkloadPending {
			continue
		}

		if err := r.schedulerService.ScheduleWorkload(ctx, workload); err != nil {
			// A workload may legitimately remain pending if
			// no node currently has enough capacity.
			slog.Warn(
				"workload remains pending",
				"workload_id", workload.ID,
				"service_id", service.ID,
				"error", err,
			)
		}
	}

	return nil
}