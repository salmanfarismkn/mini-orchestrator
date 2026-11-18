package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/runtime"
	"mini-orchestrator/internal/store"
)

type workloadExecutionStore interface {
	ClaimExecution(context.Context, string) (bool, error)
	PersistContainerID(context.Context, string, string) error
	SetExecutionError(context.Context, string, model.ExecutionState, string) error
	MarkWorkloadRunning(context.Context, string, string) error
	MarkWorkloadFailedAndReleaseResources(context.Context, string) error
	MarkWorkloadStoppedAndReleaseResources(context.Context, string) error
}

type workloadNodeClient interface {
	CreateContainer(context.Context, string, runtime.ContainerConfig) (runtime.Container, error)
	StartContainer(context.Context, string, string) error
	StopContainer(context.Context, string, string) error
	DeleteContainer(context.Context, string, string) error
	GetContainer(context.Context, string, string) (runtime.Container, error)
}

type WorkloadExecutor struct {
	store      workloadExecutionStore
	nodeClient workloadNodeClient
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
	if workload.DesiredState != model.WorkloadScheduled ||
		workload.ExecutionState == model.ExecutionFailed ||
		workload.ExecutionState == model.ExecutionRunning {
		return nil
	}

	containerID := ""
	if workload.ContainerID != nil {
		containerID = *workload.ContainerID
	}

	if workload.ExecutionState == model.ExecutionIdle || workload.ExecutionState == "" {
		claimed, err := e.store.ClaimExecution(ctx, workload.ID)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
	}

	if containerID == "" {
		containerName := "workload-" + workload.ID
		container, err := e.nodeClient.CreateContainer(
			ctx,
			node.Address,
			runtime.ContainerConfig{
				Name:       containerName,
				WorkloadID: workload.ID,
				Image:      workload.Image,
				CPUMillis:  workload.CPURequestMillis,
				MemoryMB:   workload.MemoryRequestMB,
			},
		)
		if err != nil {
			if errors.Is(err, runtime.ErrContainerConflict) {
				conflictErr := fmt.Sprintf("container identity conflict: %v", err)
				if dbErr := e.store.SetExecutionError(ctx, workload.ID, model.ExecutionFailed, conflictErr); dbErr == nil {
					_ = e.store.MarkWorkloadFailedAndReleaseResources(ctx, workload.ID)
				}
				return err
			}
			_ = e.store.SetExecutionError(ctx, workload.ID, model.ExecutionStarting, err.Error())
			return fmt.Errorf("create container: %w", err)
		}
		containerID = container.ID
		if err := e.store.PersistContainerID(ctx, workload.ID, containerID); err != nil {
			return fmt.Errorf("persist created container ID: %w", err)
		}
	}

	if err := e.nodeClient.StartContainer(ctx, node.Address, containerID); err != nil {
		_ = e.store.SetExecutionError(ctx, workload.ID, model.ExecutionCreated, err.Error())
		return fmt.Errorf("start container: %w", err)
	}

	container, err := e.nodeClient.GetContainer(ctx, node.Address, containerID)
	if err != nil {
		_ = e.store.SetExecutionError(ctx, workload.ID, model.ExecutionCreated, err.Error())
		return fmt.Errorf("inspect started container: %w", err)
	}
	if container.Status != "running" {
		message := fmt.Sprintf("container %q is not running; observed status %q", containerID, container.Status)
		_ = e.store.SetExecutionError(ctx, workload.ID, model.ExecutionCreated, message)
		return errors.New(message)
	}

	if err := e.store.MarkWorkloadRunning(ctx, workload.ID, containerID); err != nil {
		return fmt.Errorf("persist running workload: %w", err)
	}

	slog.Info("workload started", "workload_id", workload.ID, "container_id", containerID, "node_id", node.ID)
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
	if workload.ActualState == model.WorkloadStopped {
		return nil
	}

	// Pending workloads don't have a container.
	if workload.ContainerID == nil {
		if err := e.store.MarkWorkloadStoppedAndReleaseResources(
			ctx,
			workload.ID,
		); err != nil {
			return err
		}
		slog.Info(
			"replica removed",
			"workload_id", workload.ID,
			"service_id", workload.ServiceID,
			"node_id", node.ID,
			"deployment_version", workload.DeploymentVersion,
			"state", model.WorkloadStopped,
		)
		return nil
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
		"replica removed",
		"workload_id", workload.ID,
		"service_id", workload.ServiceID,
		"container_id", containerID,
		"node_id", node.ID,
		"deployment_version", workload.DeploymentVersion,
		"state", model.WorkloadStopped,
	)

	return nil
}
