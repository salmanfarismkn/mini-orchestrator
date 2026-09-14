package reconciler

import (
	"context"
	"log/slog"
	"fmt"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/store"
)

type RuntimeReconciler struct {
	store      *store.Postgres
	nodeClient *node.Client
	logger     *slog.Logger
}

func NewRuntimeReconciler(
	store *store.Postgres,
	nodeClient *node.Client,
) *RuntimeReconciler {
	return &RuntimeReconciler{
		store:      store,
		nodeClient: nodeClient,
		logger:     slog.Default(),
	}
}

func (r *RuntimeReconciler) ReconcileNode(
    ctx context.Context,
    node model.Node,
) error {
    // 1. List all containers currently running on the node
    containers, err := r.nodeClient.ListContainers(ctx, node.Address)
    if err != nil {
        return err
    }

    // 2. Build a set of existing container IDs
    existingContainers := make(map[string]struct{}, len(containers))
    for _, container := range containers {
        existingContainers[container.ID] = struct{}{}
    }

    // 3. Reconcile each container → workload mapping
    for _, container := range containers {
        workload, err := r.store.GetWorkloadByContainerID(ctx, container.ID)
        if err != nil {
            // Container may belong to something outside our orchestrator
            r.logger.Warn("container not tracked by orchestrator",
                "container_id", container.ID,
                "node_id", node.ID,
            )
            continue
        }

        // Handle stop requests
        if workload.DesiredState == model.WorkloadStopped &&
            workload.ActualState != model.WorkloadStopped {
            r.logger.Info("stopping workload container",
                "workload_id", workload.ID,
                "node_id", node.ID,
            )

            if err := r.nodeClient.StopContainer(ctx, node.Address, container.ID); err != nil {
                r.logger.Error("failed to stop container",
                    "workload_id", workload.ID,
                    "node_id", node.ID,
                    "error", err,
                )
                continue
            }

            if err := r.store.UpdateWorkloadActualState(ctx, workload.ID, model.WorkloadStopped); err != nil {
                return err
            }
            continue
        }

        // Otherwise reconcile based on container status
        state := containerState(container.Status)
        if workload.ActualState == state {
            continue
        }

        if state == model.WorkloadFailed {
            if err := r.store.MarkWorkloadFailedAndReleaseResources(ctx, workload.ID); err != nil {
                return err
            }
        } else {
            if err := r.store.UpdateWorkloadActualState(ctx, workload.ID, state); err != nil {
                return err
            }
        }

        r.logger.Info("workload state reconciled",
            "workload_id", workload.ID,
            "node_id", node.ID,
            "state", state,
        )
    }

    // 4. Reconcile workloads assigned to this node but missing containers
    workloads, err := r.store.ListWorkloadsByNode(ctx, node.ID)
    if err != nil {
        return err
    }

    for _, workload := range workloads {
        if workload.ContainerID == nil {
            continue
        }

        if workload.ActualState != model.WorkloadRunning &&
            workload.ActualState != model.WorkloadScheduled {
            continue
        }

        if _, exists := existingContainers[*workload.ContainerID]; exists {
            continue
        }

        r.logger.Warn("tracked container missing from node",
            "workload_id", workload.ID,
            "container_id", *workload.ContainerID,
            "node_id", node.ID,
        )

        if err := r.store.MarkWorkloadFailedAndReleaseResources(ctx, workload.ID); err != nil {
            return fmt.Errorf("mark missing workload failed: %w", err)
        }
    }

    return nil
}



func containerState(status string) model.WorkloadState {
	switch status {
	case "running":
		return model.WorkloadRunning

	case "created":
		return model.WorkloadScheduled

	case "exited", "dead":
		return model.WorkloadFailed

	default:
		return model.WorkloadFailed
	}
}
