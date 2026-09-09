package scheduler

import (
	"context"
	"testing"
)

// Dummy config for now
type fakeRuntime struct{}

func TestSchedulerService_Compiles(t *testing.T) {
	ctx := context.Background()
	_ = ctx

	// Instantiate the scheduler service with nil dependencies for this compile-only test.
	svc := NewService(nil, nil)
	if svc == nil {
		t.Fatal("expected scheduler service")
	}
}
