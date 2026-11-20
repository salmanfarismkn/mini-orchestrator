package store

import (
	"testing"

	"mini-orchestrator/internal/model"
)

func TestValidDeploymentTransition(t *testing.T) {
	tests := []struct {
		name string
		from model.DeploymentStatus
		to   model.DeploymentStatus
		want bool
	}{
		{name: "pending to progressing", from: model.DeploymentPending, to: model.DeploymentProgressing, want: true},
		{name: "progressing to available", from: model.DeploymentProgressing, to: model.DeploymentAvailable, want: true},
		{name: "progressing to failed", from: model.DeploymentProgressing, to: model.DeploymentFailed, want: true},
		{name: "available to progressing", from: model.DeploymentAvailable, to: model.DeploymentProgressing, want: true},
		{name: "failed is terminal", from: model.DeploymentFailed, to: model.DeploymentProgressing, want: false},
		{name: "available to pending is invalid", from: model.DeploymentAvailable, to: model.DeploymentPending, want: false},
		{name: "same state is idempotent", from: model.DeploymentAvailable, to: model.DeploymentAvailable, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validDeploymentTransition(test.from, test.to); got != test.want {
				t.Fatalf("validDeploymentTransition(%q, %q) = %t; want %t",
					test.from, test.to, got, test.want)
			}
		})
	}
}
