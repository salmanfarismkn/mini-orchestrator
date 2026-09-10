package reconciler

import (
	"context"
	"fmt"
	"log/slog"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/runtime"
	"mini-orchestrator/internal/store"
)

type WorkloadExecutor struct {
	store       *store.Postgres
	nodeClient  *node.Client
}

func NewWorkloadExecutor(
	store *store.Postgres,
	nodeClient *node.Client,
) *WorkloadExecutor {
	return &WorkloadExecutor{
		store:      store,
		nodeClient: nodeClient,
	}
}

func (e *WorkloadExecutor) Execute(
	ctx context.Context,
	workload model.Workload,
	node model.Node,
) error {
	if workload.DesiredState != model.WorkloadScheduled {
		return nil
	}

	containerName := "workload-" + workload.ID

	container, err := e.nodeClient.CreateContainer(
		ctx,
		node.Address,
		runtime.ContainerConfig{
			Name:      containerName,
			Image:     workload.Image,
			CPUMillis: workload.CPURequestMillis,
			MemoryMB:  workload.MemoryRequestMB,
		},
	)
	if err != nil {
		return fmt.Errorf("create container: %w", err)
	}

	if err := e.nodeClient.StartContainer(
		ctx,
		node.Address,
		container.ID,
	); err != nil {
		slog.Error(
			"container created but failed to start",
			"workload_id", workload.ID,
			"container_id", container.ID,
			"node_id", node.ID,
			"error", err,
		)

		_ = e.store.MarkWorkloadFailed(ctx, workload.ID)

		return fmt.Errorf("start container: %w", err)
	}

	if err := e.store.MarkWorkloadRunning(
		ctx,
		workload.ID,
		container.ID,
	); err != nil {
		return fmt.Errorf("persist running workload: %w", err)
	}

	slog.Info(
		"workload started",
		"workload_id", workload.ID,
		"container_id", container.ID,
		"node_id", node.ID,
	)

	return nil
}

func (e *WorkloadExecutor) Stop(
	ctx context.Context,
	workload model.Workload,
	node model.Node,
) error {
	if workload.DesiredState != model.WorkloadStopped {
		return nil
	}

	// Pending workloads don't have a container.
	if workload.ContainerID == nil {
		return e.store.MarkWorkloadStoppedAndReleaseResources(
			ctx,
			workload.ID,
		)
	}

	containerID := *workload.ContainerID

	if err := e.nodeClient.StopContainer(
		ctx,
		node.Address,
		containerID,
	); err != nil {
		return fmt.Errorf(
			"stop container %q: %w",
			containerID,
			err,
		)
	}

	if err := e.nodeClient.DeleteContainer(
		ctx,
		node.Address,
		containerID,
	); err != nil {
		return fmt.Errorf(
			"delete container %q: %w",
			containerID,
			err,
		)
	}

	if err := e.store.MarkWorkloadStoppedAndReleaseResources(
		ctx,
		workload.ID,
	); err != nil {
		return fmt.Errorf(
			"persist stopped workload: %w",
			err,
		)
	}

	slog.Info(
		"workload stopped",
		"workload_id", workload.ID,
		"container_id", containerID,
		"node_id", node.ID,
	)

	return nil
}