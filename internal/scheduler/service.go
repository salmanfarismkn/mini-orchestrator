package scheduler

import (
	"context"
	"fmt"

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
		return fmt.Errorf("list nodes: %w", err)
	}

	selectedNode, err := s.scheduler.Schedule(
		workload,
		nodes,
	)
	if err != nil {
		return err
	}

	if err := s.store.ReserveNodeForWorkload(
		ctx,
		workload.ID,
		selectedNode.ID,
		workload.CPURequestMillis,
		workload.MemoryRequestMB,
	); err != nil {
		return fmt.Errorf(
			"reserve selected node %q: %w",
			selectedNode.ID,
			err,
		)
	}

	return nil
}
