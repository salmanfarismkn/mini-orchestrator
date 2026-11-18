package runtime

import (
	"context"
	"errors"
)

type ContainerConfig struct {
	Name       string
	WorkloadID string
	NodeID     string
	Image      string
	CPUMillis  int
	MemoryMB   int
}

type Container struct {
	ID             string
	Name           string
	Image          string
	Status         string
	CPUUsageMillis int
	WorkloadID     string `json:"workload_id"`
	NodeID         string `json:"node_id"`
}

type ContainerStats struct {
	CPUUsageMillis int
}

var ErrContainerNotOwned = errors.New("container is not owned by node")

type Runtime interface {
	Create(ctx context.Context, config ContainerConfig) (Container, error)
	Start(ctx context.Context, containerID, nodeID string) error
	Stop(ctx context.Context, containerID, nodeID string) error
	Remove(ctx context.Context, containerID, nodeID string) error
	Inspect(ctx context.Context, containerID, nodeID string) (Container, error)
	List(ctx context.Context, nodeID string) ([]Container, error)
	Stats(ctx context.Context, containerID, nodeID string) (ContainerStats, error)
}
