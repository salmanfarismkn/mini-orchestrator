package reconciler

import (
	"context"
	"log/slog"
	"time"

	"mini-orchestrator/internal/controller"
	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/observability"
	"mini-orchestrator/internal/scheduler"
	"mini-orchestrator/internal/store"
)

type Reconciler struct {
	store                *store.Postgres
	replicaController    *controller.ReplicaController
	deploymentController *controller.DeploymentController
	deploymentStatus     *controller.DeploymentStatusController
	schedulerService     *scheduler.Service
	executor             *WorkloadExecutor
	runtimeReconciler    *RuntimeReconciler
	interval             time.Duration
	deletionController   *controller.DeletionController
	metrics              *observability.Metrics
}

func New(
	store *store.Postgres,
	replicaController *controller.ReplicaController,
	deploymentController *controller.DeploymentController,
	deploymentStatus *controller.DeploymentStatusController,
	schedulerService *scheduler.Service,
	executor *WorkloadExecutor,
	runtimeReconciler *RuntimeReconciler,
	interval time.Duration,
	metrics *observability.Metrics,
) *Reconciler {
	return &Reconciler{
		store:                store,
		replicaController:    replicaController,
		deploymentController: deploymentController,
		deploymentStatus:     deploymentStatus,
		schedulerService:     schedulerService,
		executor:             executor,
		runtimeReconciler:    runtimeReconciler,
		interval:             interval,
		deletionController:   controller.NewDeletionController(store),
		metrics:              metrics,
	}
}

func (r *Reconciler) listServices(ctx context.Context) ([]model.Service, error) {
	return r.store.ListServices(ctx)
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	if err := r.reconcile(ctx); err != nil {
		r.metrics.ReconcileFailed()
		slog.Error(
			"reconciliation cycle failed",
			"error",
			err,
		)
	} else {
		r.metrics.ReconcileSucceeded()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := r.reconcile(ctx); err != nil {
				r.metrics.ReconcileFailed()
				slog.Error(
					"reconciliation cycle failed",
					"error",
					err,
				)
			} else {
				r.metrics.ReconcileSucceeded()
			}
		}
	}
}

func (r *Reconciler) reconcile(ctx context.Context) error {
	services, err := r.listServices(ctx)
	if err != nil {
		slog.Error("reconciliation failed to list services", "error", err)
		return err
	}

	nodes, err := r.store.ListNodes(ctx)
	if err != nil {
		slog.Error("failed to list nodes", "error", err)
		return err
	}

	for _, node := range nodes {
		if node.Status == model.NodeUnhealthy {
			if err := r.store.RecoverWorkloadsFromNode(ctx, node.ID); err != nil {
				slog.Error("failed to recover workloads from node",
					"node_id", node.ID, "error", err)
			}
			continue
		}
		if node.Status != model.NodeReady {
			continue
		}
		if err := r.runtimeReconciler.ReconcileNode(ctx, node); err != nil {
			slog.Error("runtime reconciliation failed",
				"node_id", node.ID, "error", err)
		}
	}

	for _, service := range services {
		if service.Status == model.ServiceDeleting {
			if err := r.deletionController.ReconcileService(ctx, service); err != nil {
				slog.Error("service deletion reconciliation failed",
					"service_id", service.ID, "error", err)
			}
			continue
		}

		if service.Status != model.ServiceActive {
			continue
		}

		if err := r.deploymentController.ReconcileService(ctx, service); err != nil {
			slog.Error("deployment reconciliation failed",
				"service_id", service.ID,
				"deployment_version", service.DeploymentVersion,
				"error", err)
			continue
		}

		if err := r.replicaController.ReconcileService(ctx, service); err != nil {
			slog.Error("replica reconciliation failed",
				"service_id", service.ID,
				"deployment_version", service.DeploymentVersion,
				"error", err)
			continue
		}

		if err := r.schedulePendingWorkloads(ctx, service); err != nil {
			slog.Error("workload scheduling failed",
				"service_id", service.ID,
				"deployment_version", service.DeploymentVersion,
				"error", err)
		}

		if err := r.deploymentStatus.ReconcileService(ctx, service); err != nil {
			slog.Error("deployment status reconciliation failed",
				"service_id", service.ID,
				"deployment_version", service.DeploymentVersion,
				"error", err)
		}
	}

	return nil
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
				"workload_id", workload.ID,
				"service_id", service.ID,
				"deployment_version", workload.DeploymentVersion,
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
		switch {
		case workload.DesiredState == model.WorkloadStopped:
			if workload.NodeID == nil {
				if err := r.executor.Stop(
					ctx,
					workload,
					model.Node{},
				); err != nil {
					slog.Error(
						"failed to stop pending workload",
						"workload_id", workload.ID,
						"service_id", workload.ServiceID,
						"state", workload.ActualState,
						"error", err,
					)
				}
				continue
			}

		case workload.ActualState != model.WorkloadScheduled:
			continue
		}

		if workload.NodeID == nil {
			continue
		}

		node, err := r.store.GetNode(ctx, *workload.NodeID)
		if err != nil {
			slog.Error(
				"failed to get workload node",
				"workload_id", workload.ID,
				"service_id", workload.ServiceID,
				"node_id", *workload.NodeID,
				"error", err,
			)
			continue
		}

		if workload.DesiredState == model.WorkloadStopped {
			if err := r.executor.Stop(
				ctx,
				workload,
				node,
			); err != nil {
				slog.Error(
					"failed to stop workload",
					"workload_id", workload.ID,
					"service_id", workload.ServiceID,
					"node_id", node.ID,
					"container_id", workload.ContainerID,
					"state", workload.ActualState,
					"error", err,
				)
			}

			continue
		}

		if err := r.executor.Execute(
			ctx,
			workload,
			node,
		); err != nil {
			slog.Error(
				"workload execution failed",
				"workload_id", workload.ID,
				"service_id", workload.ServiceID,
				"node_id", node.ID,
				"deployment_version", workload.DeploymentVersion,
				"error", err,
			)
		}
	}

	return nil
}
