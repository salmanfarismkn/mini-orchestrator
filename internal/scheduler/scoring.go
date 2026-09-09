package scheduler

import (
	"math"

	"mini-orchestrator/internal/model"
)

func SelectBestNode(
	workload model.Workload,
	nodes []model.Node,
) model.Node {
	best := nodes[0]
	bestScore := math.Inf(-1)

	for _, node := range nodes {
		score := scoreNode(workload, node)

		if score > bestScore {
			bestScore = score
			best = node
		}
	}

	return best
}

func scoreNode(
	workload model.Workload,
	node model.Node,
) float64 {
	availableCPU :=
		node.CPUCapacity -
			node.CPUAllocated -
			workload.CPURequestMillis

	availableMemory :=
		node.MemoryCapacity -
			node.MemoryAllocated -
			workload.MemoryRequestMB

	cpuRatio :=
		float64(availableCPU) /
			float64(node.CPUCapacity)

	memoryRatio :=
		float64(availableMemory) /
			float64(node.MemoryCapacity)

	return cpuRatio + memoryRatio
}
