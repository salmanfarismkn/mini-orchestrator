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
	executor          *WorkloadExecutor
	runtimeReconciler *RuntimeReconciler
	interval          time.Duration
}

func New(
	store *store.Postgres,
	replicaController *controller.ReplicaController,
	schedulerService *scheduler.Service,
	executor *WorkloadExecutor,
	runtimeReconciler *RuntimeReconciler,
	interval time.Duration,
) *Reconciler {
	return &Reconciler{
		store:             store,
		replicaController: replicaController,
		schedulerService:  schedulerService,
		executor:          executor,
		runtimeReconciler: runtimeReconciler,
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

    nodes, err := r.store.ListNodes(ctx)
    if err != nil {
        slog.Error("failed to list nodes", "error", err)
        return
    }

    for _, node := range nodes {
        if node.Status == model.NodeUnhealthy {
            if err := r.store.RecoverWorkloadsFromNode(ctx, node.ID); err != nil {
                slog.Error(
                    "failed to recover workloads from node",
                    "node_id", node.ID,
                    "error", err,
                )
            }
            continue
        }

        if node.Status != model.NodeReady {
            continue
        }

        if err := r.runtimeReconciler.ReconcileNode(ctx, node); err != nil {
            slog.Error(
                "runtime reconciliation failed",
                "node_id", node.ID,
                "error", err,
            )
        }
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

		if err := r.executeScheduledWorkloads(ctx, service); err != nil {
			slog.Error(
				"workload execution failed",
				"service_id", service.ID,
				"error", err,
			)
		}
	}

	return nil
}

func (r *Reconciler) executeScheduledWorkloads(
	ctx context.Context,
	service model.Service,
) error {
	workloads, err := r.store.ListWorkloadsByService(ctx, service.ID)
	if err != nil {
		return err
	}

	for _, workload := range workloads {
		if workload.ActualState != model.WorkloadScheduled {
			continue
		}

		if workload.NodeID == nil {
			slog.Error(
				"scheduled workload has no node",
				"workload_id", workload.ID,
			)
			continue
		}

		node, err := r.store.GetNode(ctx, *workload.NodeID)
		if err != nil {
			slog.Error(
				"failed to get workload node",
				"workload_id", workload.ID,
				"node_id", *workload.NodeID,
				"error", err,
			)
			continue
		}

		if err := r.executor.Execute(ctx, workload, node); err != nil {
			slog.Error(
				"workload execution failed",
				"workload_id", workload.ID,
				"node_id", node.ID,
				"error", err,
			)
		}
	}

	return nil
}