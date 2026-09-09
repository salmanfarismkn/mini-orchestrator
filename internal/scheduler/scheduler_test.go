package scheduler

import (
	"testing"

	"mini-orchestrator/internal/model"
)

func TestSchedulerSelectsFeasibleNode(t *testing.T) {
	workload := model.Workload{
		ID:               "workload-1",
		CPURequestMillis: 500,
		MemoryRequestMB:  512,
	}

	nodes := []model.Node{
		{
			ID:              "node-1",
			Status:          model.NodeReady,
			CPUCapacity:     4000,
			CPUAllocated:    1000,
			MemoryCapacity:  8192,
			MemoryAllocated: 1024,
		},
		{
			ID:              "node-2",
			Status:          model.NodeReady,
			CPUCapacity:     4000,
			CPUAllocated:    3800,
			MemoryCapacity:  8192,
			MemoryAllocated: 1024,
		},
	}

	scheduler := New()

	selected, err := scheduler.Schedule(
		workload,
		nodes,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if selected.ID != "node-1" {
		t.Fatalf(
			"expected node-1, got %s",
			selected.ID,
		)
	}
}

func TestSchedulerRejectsInsufficientResources(t *testing.T) {
	workload := model.Workload{
		ID:               "workload-1",
		CPURequestMillis: 2000,
		MemoryRequestMB:  4096,
	}

	nodes := []model.Node{
		{
			ID:              "node-1",
			Status:          model.NodeReady,
			CPUCapacity:     4000,
			CPUAllocated:    3000,
			MemoryCapacity:  8192,
			MemoryAllocated: 6144,
		},
	}

	scheduler := New()

	_, err := scheduler.Schedule(
		workload,
		nodes,
	)

	if err == nil {
		t.Fatal("expected scheduling to fail")
	}
}

func TestSchedulerIgnoresUnhealthyNodes(t *testing.T) {
	workload := model.Workload{
		ID:               "workload-1",
		CPURequestMillis: 500,
		MemoryRequestMB:  512,
	}

	nodes := []model.Node{
		{
			ID:             "unhealthy-node",
			Status:         model.NodeUnhealthy,
			CPUCapacity:    8000,
			MemoryCapacity: 16384,
		},
		{
			ID:             "ready-node",
			Status:         model.NodeReady,
			CPUCapacity:    2000,
			MemoryCapacity: 4096,
		},
	}

	scheduler := New()

	selected, err := scheduler.Schedule(
		workload,
		nodes,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if selected.ID != "ready-node" {
		t.Fatalf(
			"expected ready-node, got %s",
			selected.ID,
		)
	}
}
func TestSchedulerRejectsInsufficientMemory(t *testing.T) {
	workload := model.Workload{
		ID:               "memory-heavy",
		CPURequestMillis: 500,
		MemoryRequestMB:  4096,
	}

	nodes := []model.Node{
		{
			ID:              "node-1",
			Status:          model.NodeReady,
			CPUCapacity:     4000,
			CPUAllocated:    500,
			MemoryCapacity:  4096,
			MemoryAllocated: 1024,
		},
	}

	scheduler := New()

	_, err := scheduler.Schedule(
		workload,
		nodes,
	)

	if err == nil {
		t.Fatal("expected scheduling to fail")
	}
}
