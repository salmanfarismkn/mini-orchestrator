package scheduler

import (
	"mini-orchestrator/internal/model"
)

func FilterNodes(
	workload model.Workload,
	nodes []model.Node,
) []model.Node {
	candidates := make([]model.Node, 0, len(nodes))

	for _, node := range nodes {
		if node.Status != model.NodeReady {
			continue
		}

		availableCPU := node.CPUCapacity - node.CPUAllocated
		availableMemory := node.MemoryCapacity - node.MemoryAllocated

		if availableCPU < workload.CPURequestMillis {
			continue
		}

		if availableMemory < workload.MemoryRequestMB {
			continue
		}

		candidates = append(candidates, node)
	}

	return candidates
}
