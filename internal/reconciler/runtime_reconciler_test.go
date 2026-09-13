package reconciler

import (
	"testing"

	"mini-orchestrator/internal/model"
)

func TestContainerState(t *testing.T) {
	tests := []struct {
		status string
		want   model.WorkloadState
	}{
		{
			status: "running",
			want:   model.WorkloadRunning,
		},
		{
			status: "created",
			want:   model.WorkloadScheduled,
		},
		{
			status: "exited",
			want:   model.WorkloadFailed,
		},
		{
			status: "dead",
			want:   model.WorkloadFailed,
		},
	}

	for _, tt := range tests {
		got := containerState(tt.status)

		if got != tt.want {
			t.Fatalf(
				"status %q: expected %q, got %q",
				tt.status,
				tt.want,
				got,
			)
		}
	}
}
