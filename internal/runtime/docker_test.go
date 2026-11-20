package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
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
		Name:       "mini-orchestrator-test-" + hex.EncodeToString(randomName[:]),
		WorkloadID: "test-" + hex.EncodeToString(randomName[:]),
		NodeID:     "runtime-test-node",
		Image:      "nginx:alpine",
		CPUMillis:  100,
		MemoryMB:   128,
	})
	if err != nil {
		t.Fatalf("create container: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := runtime.Remove(cleanupCtx, container.ID, "runtime-test-node"); err != nil {
			t.Errorf("remove test-created container: %v", err)
		}
	})

	if err := runtime.Start(ctx, container.ID, "runtime-test-node"); err != nil {
		t.Fatalf("start container: %v", err)
	}

	if err := runtime.Start(ctx, container.ID, "runtime-test-node"); err != nil {
		t.Fatalf("start already-running container: %v", err)
	}

	info, err := runtime.Inspect(ctx, container.ID, "runtime-test-node")
	if err != nil {
		t.Fatalf("inspect container: %v", err)
	}

	if info.NodeID != "runtime-test-node" || info.WorkloadID != "test-"+hex.EncodeToString(randomName[:]) {
		t.Fatalf("container ownership labels = node %q, workload %q", info.NodeID, info.WorkloadID)
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
	containers, err := runtime.List(ctx, "runtime-test-node")
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

	stats, err := runtime.Stats(ctx, container.ID, "runtime-test-node")
	if err != nil {
		t.Fatalf("get container stats: %v", err)
	}

	if stats.CPUUsageMillis < 0 {
		t.Fatalf("expected non-negative CPU usage, got %d", stats.CPUUsageMillis)
	}
}

func TestDockerRuntimeRejectsLegacyContainerWithoutMutation(t *testing.T) {
	runtime, err := NewDockerRuntime()
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var randomName [16]byte
	if _, err := rand.Read(randomName[:]); err != nil {
		t.Fatalf("generate unique legacy container name: %v", err)
	}
	name := "mini-orchestrator-legacy-test-" + hex.EncodeToString(randomName[:])
	image := "nginx:alpine"

	if _, _, err := runtime.client.ImageInspectWithRaw(ctx, image); err != nil {
		if !errdefs.IsNotFound(err) {
			t.Fatalf("inspect test image: %v", err)
		}
		reader, err := runtime.client.ImagePull(ctx, image, types.ImagePullOptions{})
		if err != nil {
			t.Fatalf("pull test image: %v", err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			reader.Close()
			t.Fatalf("read test image pull: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("close test image pull: %v", err)
		}
	}

	created, err := runtime.client.ContainerCreate(
		ctx,
		&container.Config{Image: image},
		&container.HostConfig{Resources: container.Resources{
			NanoCPUs: 100_000_000,
			Memory:   128 * 1024 * 1024,
		}},
		nil,
		nil,
		name,
	)
	if err != nil {
		t.Fatalf("create legacy container fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := runtime.client.ContainerRemove(cleanupCtx, created.ID, container.RemoveOptions{Force: true}); err != nil &&
			!errdefs.IsNotFound(err) {
			t.Errorf("remove legacy test fixture: %v", err)
		}
	})

	before, err := runtime.client.ContainerInspect(ctx, created.ID)
	if err != nil {
		t.Fatalf("inspect legacy fixture before rejection: %v", err)
	}
	config := ContainerConfig{
		Name:       name,
		WorkloadID: "legacy-" + hex.EncodeToString(randomName[:]),
		NodeID:     "runtime-test-node",
		Image:      image,
		CPUMillis:  100,
		MemoryMB:   128,
	}
	if _, err := runtime.Create(ctx, config); !errors.Is(err, ErrContainerConflict) {
		t.Fatalf("Create() error = %v, want ErrContainerConflict", err)
	}
	if err := runtime.Start(ctx, created.ID, config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Start() error = %v, want ErrContainerNotOwned", err)
	}
	if err := runtime.Stop(ctx, created.ID, config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Stop() error = %v, want ErrContainerNotOwned", err)
	}
	if err := runtime.Remove(ctx, created.ID, config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Remove() error = %v, want ErrContainerNotOwned", err)
	}
	if _, err := runtime.Inspect(ctx, created.ID, config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Inspect() error = %v, want ErrContainerNotOwned", err)
	}
	if _, err := runtime.Stats(ctx, created.ID, config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Stats() error = %v, want ErrContainerNotOwned", err)
	}
	listed, err := runtime.List(ctx, config.NodeID)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	for _, item := range listed {
		if item.ID == created.ID {
			t.Fatalf("List() exposed unlabeled legacy container: %+v", item)
		}
	}

	after, err := runtime.client.ContainerInspect(ctx, created.ID)
	if err != nil {
		t.Fatalf("legacy fixture disappeared after rejected operations: %v", err)
	}
	if after.ID != before.ID || after.State.Status != before.State.Status ||
		after.Config.Labels["mini-orchestrator"] != "" ||
		after.Config.Labels[workloadLabel] != "" ||
		after.Config.Labels[nodeLabel] != "" {
		t.Fatalf("legacy fixture changed after rejected operations: before=%+v after=%+v", before, after)
	}
}

type fakeDockerClient struct {
	containers        map[string]types.ContainerJSON
	listed            []types.Container
	created           container.CreateResponse
	createErr         error
	createdConfig     *container.Config
	createdHost       *container.HostConfig
	createdName       string
	conflictContainer *types.ContainerJSON
	createCalls       int
	startCalls        []string
	stopCalls         []string
	removeCalls       []string
}

func (f *fakeDockerClient) ImageInspectWithRaw(context.Context, string) (types.ImageInspect, []byte, error) {
	return types.ImageInspect{}, nil, nil
}
func (f *fakeDockerClient) ImagePull(context.Context, string, types.ImagePullOptions) (io.ReadCloser, error) {
	return io.NopCloser(nilReader{}), nil
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }

func (f *fakeDockerClient) ContainerCreate(_ context.Context, config *container.Config, hostConfig *container.HostConfig, _ *network.NetworkingConfig, _ *ocispec.Platform, name string) (container.CreateResponse, error) {
	f.createCalls++
	f.createdConfig = config
	f.createdHost = hostConfig
	f.createdName = name
	if f.createErr != nil {
		if f.conflictContainer != nil {
			f.containers[name] = *f.conflictContainer
		}
		return container.CreateResponse{}, f.createErr
	}
	f.containers[name] = fakeContainerJSON("created-id", name, config, hostConfig)
	return container.CreateResponse{ID: "created-id"}, nil
}
func (f *fakeDockerClient) ContainerInspect(_ context.Context, id string) (types.ContainerJSON, error) {
	if info, ok := f.containers[id]; ok {
		return info, nil
	}
	return types.ContainerJSON{}, errdefs.NotFound(errors.New("container not found"))
}
func (f *fakeDockerClient) ContainerStart(_ context.Context, id string, _ container.StartOptions) error {
	f.startCalls = append(f.startCalls, id)
	if info, ok := f.containers[id]; ok && info.State != nil {
		info.State.Status = "running"
		f.containers[id] = info
	}
	return nil
}
func (f *fakeDockerClient) ContainerStop(_ context.Context, id string, _ container.StopOptions) error {
	f.stopCalls = append(f.stopCalls, id)
	return nil
}
func (f *fakeDockerClient) ContainerRemove(_ context.Context, id string, _ container.RemoveOptions) error {
	f.removeCalls = append(f.removeCalls, id)
	delete(f.containers, id)
	return nil
}
func (f *fakeDockerClient) ContainerList(context.Context, container.ListOptions) ([]types.Container, error) {
	return f.listed, nil
}
func (f *fakeDockerClient) ContainerStats(context.Context, string, bool) (types.ContainerStats, error) {
	return types.ContainerStats{}, nil
}

func fakeContainerJSON(id, name string, config *container.Config, hostConfig *container.HostConfig) types.ContainerJSON {
	return types.ContainerJSON{
		ContainerJSONBase: &types.ContainerJSONBase{
			ID: id, Name: "/" + name,
			HostConfig: hostConfig,
			State:      &types.ContainerState{Status: "created"},
		},
		Config: config,
	}
}

func newCreateConfig() ContainerConfig {
	return ContainerConfig{Name: "workload-w1", WorkloadID: "w1", NodeID: "worker-01", Image: "nginx:alpine", CPUMillis: 100, MemoryMB: 128}
}

func TestDockerCreateWhenContainerDoesNotExist(t *testing.T) {
	fake := &fakeDockerClient{containers: make(map[string]types.ContainerJSON)}
	runtime := &DockerRuntime{client: fake}
	created, err := runtime.Create(context.Background(), newCreateConfig())
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID != "created-id" || fake.createCalls != 1 {
		t.Fatalf("created = %+v, create calls = %d", created, fake.createCalls)
	}
	if fake.createdConfig.Labels["mini-orchestrator"] != "true" ||
		fake.createdConfig.Labels[workloadLabel] != "w1" ||
		fake.createdConfig.Labels[nodeLabel] != "worker-01" {
		t.Fatalf("workload labels were not set: %v", fake.createdConfig.Labels)
	}
	if fake.createdHost.NanoCPUs != 100_000_000 || fake.createdHost.Memory != 128*1024*1024 {
		t.Fatalf("resource limits were not preserved: %+v", fake.createdHost.Resources)
	}
}

func TestDockerCreateRetryAdoptsExistingContainer(t *testing.T) {
	config := newCreateConfig()
	createdConfig := &container.Config{Image: config.Image, Labels: map[string]string{"mini-orchestrator": "true", workloadLabel: "w1", nodeLabel: config.NodeID}}
	createdHost := &container.HostConfig{Resources: container.Resources{NanoCPUs: 100_000_000, Memory: 128 * 1024 * 1024}}
	fake := &fakeDockerClient{containers: map[string]types.ContainerJSON{
		config.Name: fakeContainerJSON("existing-id", config.Name, createdConfig, createdHost),
	}}
	runtime := &DockerRuntime{client: fake}
	adopted, err := runtime.Create(context.Background(), config)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if adopted.ID != "existing-id" || fake.createCalls != 0 {
		t.Fatalf("adopted = %+v, create calls = %d", adopted, fake.createCalls)
	}
}

func TestDockerCreateRejectsConflictingContainers(t *testing.T) {
	config := newCreateConfig()
	tests := []struct {
		name       string
		workloadID string
		image      string
		cpu        int64
		memory     int64
	}{
		{name: "workload identity", workloadID: "other", image: config.Image, cpu: 100_000_000, memory: 128 * 1024 * 1024},
		{name: "image", workloadID: "w1", image: "redis:alpine", cpu: 100_000_000, memory: 128 * 1024 * 1024},
		{name: "resources", workloadID: "w1", image: config.Image, cpu: 200_000_000, memory: 128 * 1024 * 1024},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			createdConfig := &container.Config{Image: test.image, Labels: map[string]string{"mini-orchestrator": "true", workloadLabel: test.workloadID, nodeLabel: config.NodeID}}
			createdHost := &container.HostConfig{Resources: container.Resources{NanoCPUs: test.cpu, Memory: test.memory}}
			fake := &fakeDockerClient{containers: map[string]types.ContainerJSON{config.Name: fakeContainerJSON("conflict-id", config.Name, createdConfig, createdHost)}}
			_, err := (&DockerRuntime{client: fake}).Create(context.Background(), config)
			if !errors.Is(err, ErrContainerConflict) {
				t.Fatalf("Create() error = %v, want ErrContainerConflict", err)
			}
			if fake.createCalls != 0 || fake.containers[config.Name].ID != "conflict-id" {
				t.Fatalf("conflicting container was modified: create calls=%d container=%+v",
					fake.createCalls, fake.containers[config.Name])
			}
		})
	}
}

func TestDockerRejectsLegacyContainerWithoutMutation(t *testing.T) {
	config := newCreateConfig()
	legacy := fakeContainerJSON(
		"legacy-id",
		config.Name,
		&container.Config{Image: config.Image},
		&container.HostConfig{Resources: container.Resources{
			NanoCPUs: 100_000_000,
			Memory:   128 * 1024 * 1024,
		}},
	)
	fake := &fakeDockerClient{
		containers: map[string]types.ContainerJSON{
			config.Name: legacy,
			"legacy-id": legacy,
		},
	}
	runtime := &DockerRuntime{client: fake}

	if _, err := runtime.Create(context.Background(), config); !errors.Is(err, ErrContainerConflict) {
		t.Fatalf("Create() error = %v, want ErrContainerConflict", err)
	}
	if _, err := runtime.Inspect(context.Background(), "legacy-id", config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Inspect() error = %v, want ErrContainerNotOwned", err)
	}
	if err := runtime.Start(context.Background(), "legacy-id", config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Start() error = %v, want ErrContainerNotOwned", err)
	}
	if err := runtime.Stop(context.Background(), "legacy-id", config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Stop() error = %v, want ErrContainerNotOwned", err)
	}
	if err := runtime.Remove(context.Background(), "legacy-id", config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Remove() error = %v, want ErrContainerNotOwned", err)
	}
	if _, err := runtime.Stats(context.Background(), "legacy-id", config.NodeID); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Stats() error = %v, want ErrContainerNotOwned", err)
	}
	if fake.createCalls != 0 || len(fake.startCalls)+len(fake.stopCalls)+len(fake.removeCalls) != 0 {
		t.Fatalf("legacy container operations were executed: creates=%d starts=%v stops=%v removes=%v",
			fake.createCalls, fake.startCalls, fake.stopCalls, fake.removeCalls)
	}
	unchanged := fake.containers[config.Name]
	if unchanged.ID != legacy.ID || unchanged.State.Status != legacy.State.Status ||
		unchanged.Config.Labels["mini-orchestrator"] != "" ||
		unchanged.Config.Labels[workloadLabel] != "" ||
		unchanged.Config.Labels[nodeLabel] != "" {
		t.Fatalf("legacy container changed after rejected operations: %+v", unchanged)
	}
}

func TestDockerCreateNameConflictAdoptsMatchingContainer(t *testing.T) {
	config := newCreateConfig()
	matchingConfig := &container.Config{Image: config.Image, Labels: map[string]string{"mini-orchestrator": "true", workloadLabel: "w1", nodeLabel: config.NodeID}}
	matchingHost := &container.HostConfig{Resources: container.Resources{NanoCPUs: 100_000_000, Memory: 128 * 1024 * 1024}}
	conflict := fakeContainerJSON("existing-id", config.Name, matchingConfig, matchingHost)
	fake := &fakeDockerClient{
		containers:        make(map[string]types.ContainerJSON),
		createErr:         errdefs.Conflict(errors.New("name is already in use")),
		conflictContainer: &conflict,
	}
	adopted, err := (&DockerRuntime{client: fake}).Create(context.Background(), config)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if adopted.ID != "existing-id" {
		t.Fatalf("adopted ID = %q, want existing-id", adopted.ID)
	}
}

func TestDockerCreateNameConflictIsValidated(t *testing.T) {
	config := newCreateConfig()
	otherConfig := &container.Config{Image: config.Image, Labels: map[string]string{"mini-orchestrator": "true", workloadLabel: "other", nodeLabel: config.NodeID}}
	otherHost := &container.HostConfig{Resources: container.Resources{NanoCPUs: 100_000_000, Memory: 128 * 1024 * 1024}}
	conflict := fakeContainerJSON("other-id", config.Name, otherConfig, otherHost)
	fake := &fakeDockerClient{
		containers:        make(map[string]types.ContainerJSON),
		createErr:         errdefs.Conflict(errors.New("name is already in use")),
		conflictContainer: &conflict,
	}
	_, err := (&DockerRuntime{client: fake}).Create(context.Background(), config)
	if !errors.Is(err, ErrContainerConflict) {
		t.Fatalf("Create() error = %v, want ErrContainerConflict", err)
	}
}

func TestDockerStartAndStopBehaviorIsPreserved(t *testing.T) {
	fake := &fakeDockerClient{containers: make(map[string]types.ContainerJSON)}
	info := fakeContainerJSON("id", "workload-w1", &container.Config{Labels: map[string]string{"mini-orchestrator": "true", workloadLabel: "w1", nodeLabel: "worker-01"}}, &container.HostConfig{})
	fake.containers["id"] = info
	runtime := &DockerRuntime{client: fake}
	if err := runtime.Start(context.Background(), "id", "worker-01"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.Start(context.Background(), "id", "worker-01"); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	if len(fake.startCalls) != 1 {
		t.Fatalf("start calls = %d, want 1 because already-running container is not restarted", len(fake.startCalls))
	}
	if err := runtime.Stop(context.Background(), "id", "worker-01"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if len(fake.stopCalls) != 1 {
		t.Fatalf("stop calls = %d, want 1", len(fake.stopCalls))
	}
}

func TestDockerRuntimeRejectsContainersOwnedByAnotherNode(t *testing.T) {
	foreignConfig := &container.Config{
		Image: "nginx:alpine",
		Labels: map[string]string{
			"mini-orchestrator": "true",
			workloadLabel:       "workload-1",
			nodeLabel:           "worker-02",
		},
	}
	fake := &fakeDockerClient{
		containers: map[string]types.ContainerJSON{
			"foreign-container": fakeContainerJSON(
				"foreign-container",
				"workload-1",
				foreignConfig,
				&container.HostConfig{},
			),
		},
	}
	runtime := &DockerRuntime{client: fake}

	if _, err := runtime.Inspect(context.Background(), "foreign-container", "worker-01"); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Inspect() error = %v; want ErrContainerNotOwned", err)
	}
	if err := runtime.Start(context.Background(), "foreign-container", "worker-01"); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Start() error = %v; want ErrContainerNotOwned", err)
	}
	if err := runtime.Stop(context.Background(), "foreign-container", "worker-01"); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Stop() error = %v; want ErrContainerNotOwned", err)
	}
	if err := runtime.Remove(context.Background(), "foreign-container", "worker-01"); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Remove() error = %v; want ErrContainerNotOwned", err)
	}
	if _, err := runtime.Stats(context.Background(), "foreign-container", "worker-01"); !errors.Is(err, ErrContainerNotOwned) {
		t.Fatalf("Stats() error = %v; want ErrContainerNotOwned", err)
	}
	if len(fake.startCalls)+len(fake.stopCalls)+len(fake.removeCalls) != 0 {
		t.Fatalf("foreign container operations were executed: starts=%v stops=%v removes=%v",
			fake.startCalls, fake.stopCalls, fake.removeCalls)
	}

	fake.listed = []types.Container{{
		ID:     "foreign-container",
		Names:  []string{"/workload-1"},
		Image:  "nginx:alpine",
		State:  "running",
		Labels: foreignConfig.Labels,
	}}
	containers, err := runtime.List(context.Background(), "worker-01")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(containers) != 0 {
		t.Fatalf("List() returned foreign node containers: %+v", containers)
	}
}
