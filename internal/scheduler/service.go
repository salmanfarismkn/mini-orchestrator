package scheduler

import (
	"context"
	"fmt"
	"log/slog"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type Service struct {
	store     *store.Postgres
	scheduler *Scheduler
}

func NewService(
	store *store.Postgres,
	scheduler *Scheduler,
) *Service {
	return &Service{
		store:     store,
		scheduler: scheduler,
	}
}

func (s *Service) ScheduleWorkload(
	ctx context.Context,
	workload model.Workload,
) error {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		slog.Error("workload scheduling failed", "workload_id", workload.ID, "service_id", workload.ServiceID, "error", err)
		return fmt.Errorf("list nodes: %w", err)
	}

	selectedNode, err := s.scheduler.Schedule(
		workload,
		nodes,
	)
	if err != nil {
		slog.Error("workload scheduling failed", "workload_id", workload.ID, "service_id", workload.ServiceID, "error", err)
		return err
	}

	if err := s.store.ReserveNodeForWorkload(
		ctx,
		workload.ID,
		selectedNode.ID,
		workload.CPURequestMillis,
		workload.MemoryRequestMB,
	); err != nil {
		slog.Error("workload scheduling failed", "workload_id", workload.ID, "service_id", workload.ServiceID, "node_id", selectedNode.ID, "error", err)
		return fmt.Errorf(
			"reserve selected node %q: %w",
			selectedNode.ID,
			err,
		)
	}

	slog.Info(
		"workload scheduled",
		"workload_id", workload.ID,
		"service_id", workload.ServiceID,
		"node_id", selectedNode.ID,
		"deployment_version", workload.DeploymentVersion,
		"state", model.WorkloadScheduled,
	)

	return nil
}
