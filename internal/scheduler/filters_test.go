package scheduler

import (
	"testing"

	"mini-orchestrator/internal/model"
)

func TestFilterNodesExcludesDrainingNodes(t *testing.T) {
	workload := model.Workload{CPURequestMillis: 100, MemoryRequestMB: 128}
	nodes := []model.Node{
		{ID: "ready", Status: model.NodeReady, CPUCapacity: 1000, MemoryCapacity: 1024},
		{ID: "draining", Status: model.NodeDraining, CPUCapacity: 1000, MemoryCapacity: 1024},
	}

	got := FilterNodes(workload, nodes)
	if len(got) != 1 || got[0].ID != "ready" {
		t.Fatalf("candidates = %#v; want only ready node", got)
	}
}
