package controller

import (
	"testing"

	"mini-orchestrator/internal/model"
)

func TestCountActiveReplicas(t *testing.T) {
	workloads := []model.Workload{
		{ActualState: model.WorkloadPending},
		{ActualState: model.WorkloadScheduled},
		{ActualState: model.WorkloadRunning},
		{ActualState: model.WorkloadRunning},
		{ActualState: model.WorkloadFailed},
		{ActualState: model.WorkloadStopped},
	}

	got := countActiveReplicas(workloads, 4)

	if got != 4 {
		t.Fatalf("expected 4 active replicas, got %d", got)
	}
}
