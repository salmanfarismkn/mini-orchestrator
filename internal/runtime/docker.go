package runtime

import (
    "context"
    "fmt"

    "github.com/docker/docker/api/types"
    "github.com/docker/docker/api/types/container"
    "github.com/docker/docker/client"
)


type DockerRuntime struct {
	client *client.Client
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
	_, _, err := d.client.ImageInspectWithRaw(ctx, config.Image)

	if err != nil {
		_, err = d.client.ImagePull(
			ctx,
			config.Image,
			types.ImagePullOptions{},
		)
		if err != nil {
			return Container{}, fmt.Errorf(
				"pull image %q: %w",
				config.Image,
				err,
			)
		}
	}

	resp, err := d.client.ContainerCreate(
		ctx,
		&container.Config{
			Image: config.Image,
		},
		&container.HostConfig{
			Resources: container.Resources{
				NanoCPUs: int64(config.CPUMillis) * 1_000_000,
				Memory:   int64(config.MemoryMB) * 1024 * 1024,
			},
		},
		nil,
		nil,
		config.Name,
	)

	if err != nil {
		return Container{}, fmt.Errorf(
			"create container: %w",
			err,
		)
	}

	return Container{
		ID:     resp.ID,
		Name:   config.Name,
		Image:  config.Image,
		Status: "created",
	}, nil
}

func (d *DockerRuntime) Start(
	ctx context.Context,
	containerID string,
) error {
	if err := d.client.ContainerStart(
		ctx,
		containerID,
		container.StartOptions{},
	); err != nil {
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
		return fmt.Errorf(
			"stop container %q: %w",
			containerID,
			err,
		)
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
		return fmt.Errorf(
			"remove container %q: %w",
			containerID,
			err,
		)
	}

	return nil
}

func (d *DockerRuntime) Inspect(
	ctx context.Context,
	containerID string,
) (Container, error) {
	info, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return Container{}, fmt.Errorf(
			"inspect container %q: %w",
			containerID,
			err,
		)
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

func (d *DockerRuntime) List(
	ctx context.Context,
) ([]Container, error) {
	containers, err := d.client.ContainerList(
		ctx,
		container.ListOptions{
			All: true,
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
			Status: c.Status,
		})
	}

	return result, nil
}
