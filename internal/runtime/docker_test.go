package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"
)

func TestDockerRuntime(t *testing.T) {
	runtime, err := NewDockerRuntime()
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var randomName [16]byte
	if _, err := rand.Read(randomName[:]); err != nil {
		t.Fatalf("generate unique test container name: %v", err)
	}

	container, err := runtime.Create(ctx, ContainerConfig{
		Name:      "mini-orchestrator-test-" + hex.EncodeToString(randomName[:]),
		Image:     "nginx:alpine",
		CPUMillis: 100,
		MemoryMB:  128,
	})
	if err != nil {
		t.Fatalf("create container: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := runtime.Remove(cleanupCtx, container.ID); err != nil {
			t.Errorf("remove test-created container: %v", err)
		}
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
	containers, err := runtime.List(ctx)
	if err != nil {
		t.Fatalf("list orchestrator containers: %v", err)
	}
	listedRunning := false
	for _, listed := range containers {
		if listed.ID == container.ID && listed.Status == "running" {
			listedRunning = true
			break
		}
	}
	if !listedRunning {
		t.Fatalf("runtime list did not report created container %q as running", container.ID)
	}

	stats, err := runtime.Stats(ctx, container.ID)
	if err != nil {
		t.Fatalf("get container stats: %v", err)
	}

	if stats.CPUUsageMillis < 0 {
		t.Fatalf("expected non-negative CPU usage, got %d", stats.CPUUsageMillis)
	}
}
