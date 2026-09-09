package runtime

import (
	"context"
	"testing"
)

func TestDockerRuntime(t *testing.T) {
	runtime, err := NewDockerRuntime()
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}

	ctx := context.Background()

	container, err := runtime.Create(ctx, ContainerConfig{
		Name:      "mini-orchestrator-test",
		Image:     "nginx:alpine",
		CPUMillis: 100,
		MemoryMB:  128,
	})
	if err != nil {
		t.Fatalf("create container: %v", err)
	}

	t.Cleanup(func() {
		_ = runtime.Remove(ctx, container.ID)
	})

	if err := runtime.Start(ctx, container.ID); err != nil {
		t.Fatalf("start container: %v", err)
	}

	if err := runtime.Start(ctx, container.ID); err != nil {
		t.Fatalf("start already-running container: %v", err)
	}

	info, err := runtime.Inspect(ctx, container.ID)
	if err != nil {
		t.Fatalf("inspect container: %v", err)
	}

	if info.ID != container.ID {
		t.Fatalf(
			"expected container ID %q, got %q",
			container.ID,
			info.ID,
		)
	}

	if info.Status != "running" {
		t.Fatalf(
			"expected running container, got %q",
			info.Status,
		)
	}
}
