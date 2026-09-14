package reconciler

import (
	"context"
	"log/slog"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/store"
)

type RuntimeReconciler struct {
	store      *store.Postgres
	nodeClient *node.Client
}

func NewRuntimeReconciler(
	store *store.Postgres,
	nodeClient *node.Client,
) *RuntimeReconciler {
	return &RuntimeReconciler{
		store:      store,
		nodeClient: nodeClient,
	}
}

func (r *RuntimeReconciler) ReconcileNode(
    ctx context.Context,
    node model.Node,
) error {
    containers, err := r.nodeClient.ListContainers(ctx, node.Address)
    if err != nil {
        return err
    }

    for _, container := range containers {
        workload, err := r.store.GetWorkloadByContainerID(ctx, container.ID)
        if err != nil {

            slog.Warn("container not tracked by orchestrator",
                "container_id", container.ID,
                "node_id", node.ID,
            )
            continue
        }


        if workload.DesiredState == model.WorkloadStopped &&
            workload.ActualState != model.WorkloadStopped {
            slog.Info("stopping workload container",
                "workload_id", workload.ID,
                "node_id", node.ID,
            )


            if err := r.nodeClient.StopContainer(ctx, node.Address, container.ID); err != nil {
                slog.Error("failed to stop container",
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

        slog.Info("workload state reconciled",
            "workload_id", workload.ID,
            "node_id", node.ID,
            "state", state,
        )
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
