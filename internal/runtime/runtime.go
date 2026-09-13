package runtime

import "context"

type ContainerConfig struct {
	Name      string
	Image     string
	CPUMillis int
	MemoryMB  int
}

type Container struct {
	ID             string
	Name           string
	Image          string
	Status         string
	CPUUsageMillis int
}

type ContainerStats struct {
	CPUUsageMillis int
}

type Runtime interface {
	Create(ctx context.Context, config ContainerConfig) (Container, error)
	Start(ctx context.Context, containerID string) error
	Stop(ctx context.Context, containerID string) error
	Remove(ctx context.Context, containerID string) error
	Inspect(ctx context.Context, containerID string) (Container, error)
	List(ctx context.Context) ([]Container, error)
	Stats(ctx context.Context, containerID string) (ContainerStats, error)
}
