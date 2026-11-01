package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var ErrContainerNotFound = errors.New("container not found")
var ErrContainerConflict = errors.New("container conflicts with workload configuration")

const workloadLabel = "mini-orchestrator.workload-id"

type dockerClient interface {
	ImageInspectWithRaw(ctx context.Context, imageID string) (types.ImageInspect, []byte, error)
	ImagePull(ctx context.Context, refStr string, options types.ImagePullOptions) (io.ReadCloser, error)
	ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	ContainerInspect(ctx context.Context, containerID string) (types.ContainerJSON, error)
	ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error
	ContainerList(ctx context.Context, options container.ListOptions) ([]types.Container, error)
	ContainerStats(ctx context.Context, containerID string, stream bool) (types.ContainerStats, error)
}

type DockerRuntime struct {
	client dockerClient
}

func (d *DockerRuntime) Close() error {
	if closer, ok := d.client.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func NewDockerRuntime() (*DockerRuntime, error) {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &DockerRuntime{
		client: dockerClient,
	}, nil
}

func (d *DockerRuntime) Create(
	ctx context.Context,
	config ContainerConfig,
) (Container, error) {
	if config.WorkloadID == "" {
		return Container{}, fmt.Errorf("workload ID is required")
	}

	info, err := d.client.ContainerInspect(ctx, config.Name)
	if err == nil {
		return d.adoptContainer(info, config)
	}
	if !errdefs.IsNotFound(err) {
		return Container{}, fmt.Errorf("inspect container %q: %w", config.Name, err)
	}

	_, _, err = d.client.ImageInspectWithRaw(ctx, config.Image)
	if err != nil {
		reader, pullErr := d.client.ImagePull(ctx, config.Image, types.ImagePullOptions{})
		if pullErr != nil {
			return Container{}, fmt.Errorf("pull image %q: %w", config.Image, pullErr)
		}
		defer reader.Close()
		if _, copyErr := io.Copy(io.Discard, reader); copyErr != nil {
			return Container{}, fmt.Errorf("pull image %q: %w", config.Image, copyErr)
		}
	}

	resp, err := d.client.ContainerCreate(
		ctx,
		&container.Config{
			Image: config.Image,
			Labels: map[string]string{
				"mini-orchestrator": "true",
				workloadLabel:       config.WorkloadID,
			},
		},
		&container.HostConfig{Resources: container.Resources{
			NanoCPUs: int64(config.CPUMillis) * 1_000_000,
			Memory:   int64(config.MemoryMB) * 1024 * 1024,
		}},
		nil,
		nil,
		config.Name,
	)
	if err != nil {
		if errdefs.IsConflict(err) {
			info, inspectErr := d.client.ContainerInspect(ctx, config.Name)
			if inspectErr == nil {
				container, adoptErr := d.adoptContainer(info, config)
				if adoptErr == nil {
					return container, nil
				}
				return Container{}, adoptErr
			}
			return Container{}, fmt.Errorf("%w: name %q is in use and could not be inspected: %v", ErrContainerConflict, config.Name, inspectErr)
		}
		return Container{}, fmt.Errorf("create container: %w", err)
	}

	return Container{ID: resp.ID, Name: config.Name, Image: config.Image, Status: "created"}, nil
}

func (d *DockerRuntime) adoptContainer(info types.ContainerJSON, config ContainerConfig) (Container, error) {
	if info.Config == nil {
		return Container{}, fmt.Errorf("%w: container %q has no configuration", ErrContainerConflict, config.Name)
	}
	if info.Config.Labels["mini-orchestrator"] != "true" {
		return Container{}, fmt.Errorf("%w: container %q is not orchestrator-managed", ErrContainerConflict, config.Name)
	}
	if actual := info.Config.Labels[workloadLabel]; actual != config.WorkloadID {
		return Container{}, fmt.Errorf("%w: container %q workload label is %q, want %q", ErrContainerConflict, config.Name, actual, config.WorkloadID)
	}
	if info.Config.Image != config.Image {
		return Container{}, fmt.Errorf("%w: container %q image is %q, want %q", ErrContainerConflict, config.Name, info.Config.Image, config.Image)
	}
	if info.HostConfig == nil {
		return Container{}, fmt.Errorf("%w: container %q has no host configuration", ErrContainerConflict, config.Name)
	}
	wantNanoCPUs := int64(config.CPUMillis) * 1_000_000
	wantMemory := int64(config.MemoryMB) * 1024 * 1024
	if info.HostConfig.NanoCPUs != wantNanoCPUs || info.HostConfig.Memory != wantMemory {
		return Container{}, fmt.Errorf("%w: container %q CPU/memory limits do not match workload", ErrContainerConflict, config.Name)
	}

	name := strings.TrimPrefix(info.Name, "/")
	if name != config.Name {
		return Container{}, fmt.Errorf("%w: inspected container name is %q, want %q", ErrContainerConflict, name, config.Name)
	}

	status := "unknown"
	if info.State != nil {
		status = info.State.Status
	}
	return Container{ID: info.ID, Name: config.Name, Image: config.Image, Status: status}, nil
}

func (d *DockerRuntime) Start(
	ctx context.Context,
	containerID string,
) error {
	info, err := d.client.ContainerInspect(ctx, containerID)
	if err == nil && info.State != nil && info.State.Status == "running" {
		return nil
	}

	if err := d.client.ContainerStart(
		ctx,
		containerID,
		container.StartOptions{},
	); err != nil {
		if errdefs.IsConflict(err) || errdefs.IsNotModified(err) {
			info, inspectErr := d.client.ContainerInspect(ctx, containerID)
			if inspectErr == nil && info.State != nil && info.State.Status == "running" {
				return nil
			}
		}

		return fmt.Errorf(
			"start container %q: %w",
			containerID,
			err,
		)
	}

	return nil
}

func (d *DockerRuntime) Stop(
	ctx context.Context,
	containerID string,
) error {
	if err := d.client.ContainerStop(
		ctx,
		containerID,
		container.StopOptions{},
	); err != nil {
		return containerOperationError("stop", containerID, err)
	}

	return nil
}

func (d *DockerRuntime) Remove(
	ctx context.Context,
	containerID string,
) error {
	if err := d.client.ContainerRemove(
		ctx,
		containerID,
		container.RemoveOptions{
			Force: true,
		},
	); err != nil {
		return containerOperationError("remove", containerID, err)
	}

	return nil
}

func (d *DockerRuntime) Inspect(
	ctx context.Context,
	containerID string,
) (Container, error) {
	info, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return Container{}, containerOperationError("inspect", containerID, err)
	}

	status := "unknown"

	if info.State != nil {
		status = info.State.Status
	}

	name := info.Name

	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}

	return Container{
		ID:     info.ID,
		Name:   name,
		Image:  info.Config.Image,
		Status: status,
	}, nil
}

func containerOperationError(operation, containerID string, err error) error {
	if errdefs.IsNotFound(err) {
		return fmt.Errorf("%s container %q: %w: %w",
			operation, containerID, ErrContainerNotFound, err)
	}
	return fmt.Errorf("%s container %q: %w", operation, containerID, err)
}

func isOrchestratorManagedContainer(c types.Container) bool {
	if c.Labels != nil {
		if value, ok := c.Labels["mini-orchestrator"]; ok && value == "true" {
			return true
		}
	}

	for _, name := range c.Names {
		if strings.HasPrefix(name, "/workload-") {
			return true
		}
	}

	return false
}

func (d *DockerRuntime) List(
	ctx context.Context,
) ([]Container, error) {
	containers, err := d.client.ContainerList(
		ctx,
		container.ListOptions{
			All:     true,
			Filters: filters.NewArgs(filters.Arg("label", "mini-orchestrator=true")),
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list containers: %w",
			err,
		)
	}

	result := make([]Container, 0, len(containers))

	for _, c := range containers {
		if !isOrchestratorManagedContainer(c) {
			continue
		}

		name := ""

		if len(c.Names) > 0 {
			name = c.Names[0]

			if len(name) > 0 && name[0] == '/' {
				name = name[1:]
			}
		}

		result = append(result, Container{
			ID:     c.ID,
			Name:   name,
			Image:  c.Image,
			Status: c.State,
		})
	}

	return result, nil
}

func calculateCPUUsage(
	previousCPU uint64,
	currentCPU uint64,
	previousSystem uint64,
	currentSystem uint64,
	numCPUs uint64,
) int {
	cpuDelta := currentCPU - previousCPU
	systemDelta := currentSystem - previousSystem

	if systemDelta == 0 {
		return 0
	}

	usage := float64(cpuDelta) /
		float64(systemDelta) *
		float64(numCPUs) *
		1000

	return int(usage)
}

func (d *DockerRuntime) Stats(
	ctx context.Context,
	containerID string,
) (ContainerStats, error) {
	response, err := d.client.ContainerStats(
		ctx,
		containerID,
		false,
	)
	if err != nil {
		return ContainerStats{}, fmt.Errorf(
			"get container stats: %w",
			err,
		)
	}
	defer response.Body.Close()

	var stats types.StatsJSON
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil {
		if err == io.EOF {
			return ContainerStats{}, fmt.Errorf("docker stats stream ended")
		}
		return ContainerStats{}, fmt.Errorf(
			"decode container stats: %w",
			err,
		)
	}

	cpuDelta := stats.CPUStats.CPUUsage.TotalUsage -
		stats.PreCPUStats.CPUUsage.TotalUsage
	if stats.PreCPUStats.SystemUsage == 0 {
		return ContainerStats{}, nil
	}

	systemDelta := stats.CPUStats.SystemUsage -
		stats.PreCPUStats.SystemUsage
	if systemDelta == 0 {
		return ContainerStats{}, nil
	}

	numCPUs := stats.CPUStats.OnlineCPUs
	if numCPUs == 0 {
		numCPUs = uint32(len(stats.CPUStats.CPUUsage.PercpuUsage))
	}
	if numCPUs == 0 {
		return ContainerStats{}, nil
	}

	cpuMillis := float64(cpuDelta) /
		float64(systemDelta) *
		float64(numCPUs) *
		1000

	return ContainerStats{
		CPUUsageMillis: int(cpuMillis),
	}, nil
}
