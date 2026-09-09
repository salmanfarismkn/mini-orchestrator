package scheduler

import (
	"fmt"

	"mini-orchestrator/internal/model"
)

type Scheduler struct{}

func New() *Scheduler {
	return &Scheduler{}
}

func (s *Scheduler) Schedule(
	workload model.Workload,
	nodes []model.Node,
) (model.Node, error) {
	candidates := FilterNodes(workload, nodes)

	if len(candidates) == 0 {
		return model.Node{}, fmt.Errorf(
			"no suitable node found for workload %q",
			workload.ID,
		)
	}

	return SelectBestNode(workload, candidates), nil
}
