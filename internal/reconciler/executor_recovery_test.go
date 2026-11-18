package reconciler

import (
	"context"
	"errors"
	"testing"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/runtime"
)

type fakeExecutionStore struct {
	executionState model.ExecutionState
	containerID    string
	persistErr     error
	persistCalls   int
	createClaimed  bool
	failed         bool
}

func (s *fakeExecutionStore) ClaimExecution(context.Context, string) (bool, error) {
	s.createClaimed = true
	s.executionState = model.ExecutionStarting
	return true, nil
}
func (s *fakeExecutionStore) PersistContainerID(_ context.Context, _ string, containerID string) error {
	s.persistCalls++
	if s.persistErr != nil {
		return s.persistErr
	}
	s.containerID = containerID
	s.executionState = model.ExecutionCreated
	return nil
}
func (s *fakeExecutionStore) SetExecutionError(_ context.Context, _ string, state model.ExecutionState, _ string) error {
	s.executionState = state
	return nil
}
func (s *fakeExecutionStore) MarkWorkloadRunning(context.Context, string, string) error {
	s.executionState = model.ExecutionRunning
	return nil
}
func (s *fakeExecutionStore) MarkWorkloadFailedAndReleaseResources(context.Context, string) error {
	s.failed = true
	return nil
}
func (s *fakeExecutionStore) MarkWorkloadStoppedAndReleaseResources(context.Context, string) error {
	return nil
}

type fakeExecutionNode struct {
	container       runtime.Container
	createCalls     int
	createErr       error
	startErr        error
	inspectErr      error
	lastContainerID string
	startCallsCount int
}

func (n *fakeExecutionNode) CreateContainer(context.Context, string, runtime.ContainerConfig) (runtime.Container, error) {
	n.createCalls++
	if n.createErr != nil {
		return runtime.Container{}, n.createErr
	}
	return n.container, nil
}
func (n *fakeExecutionNode) StartContainer(_ context.Context, _ string, containerID string) error {
	n.startCallsCount++
	n.lastContainerID = containerID
	return n.startErr
}
func (n *fakeExecutionNode) StopContainer(context.Context, string, string) error { return nil }
func (n *fakeExecutionNode) DeleteContainer(context.Context, string, string) error { return nil }
func (n *fakeExecutionNode) GetContainer(context.Context, string, string) (runtime.Container, error) {
	if n.inspectErr != nil {
		return runtime.Container{}, n.inspectErr
	}
	return n.container, nil
}

func recoveryWorkload() model.Workload {
	return model.Workload{
		ID:                "w1",
		Image:             "nginx:alpine",
		CPURequestMillis:  100,
		MemoryRequestMB:   128,
		DesiredState:      model.WorkloadScheduled,
		ActualState:       model.WorkloadScheduled,
		ExecutionState:    model.ExecutionIdle,
	}
}

func TestExecutionRetriesCreationAndPersistsContainerID(t *testing.T) {
	store := &fakeExecutionStore{}
	node := &fakeExecutionNode{container: runtime.Container{ID: "container-1", Status: "running"}}
	executor := &WorkloadExecutor{store: store, nodeClient: node}
	workload := recoveryWorkload()

	if err := executor.Execute(context.Background(), workload, model.Node{ID: "node-1", Address: "node.test"}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !store.createClaimed || store.persistCalls != 1 || store.containerID != "container-1" {
		t.Fatalf("execution state was not persisted: %+v", store)
	}
	if node.createCalls != 1 || node.lastContainerID != "container-1" || store.executionState != model.ExecutionRunning {
		t.Fatalf("unexpected execution result: node=%+v store=%+v", node, store)
	}
}

func TestExecutionPersistsBeforeStartAndSurvivesPersistenceFailure(t *testing.T) {
	store := &fakeExecutionStore{executionState: model.ExecutionStarting, persistErr: errors.New("database unavailable")}
	node := &fakeExecutionNode{container: runtime.Container{ID: "container-1", Status: "running"}}
	executor := &WorkloadExecutor{store: store, nodeClient: node}
	workload := recoveryWorkload()
	workload.ExecutionState = model.ExecutionStarting

	if err := executor.Execute(context.Background(), workload, model.Node{ID: "node-1", Address: "node.test"}); err == nil {
		t.Fatal("Execute() succeeded despite container-ID persistence failure")
	}
	if node.startCallsCount != 0 {
		t.Fatal("start was called before container ID persistence succeeded")
	}

	store.persistErr = nil
	if err := executor.Execute(context.Background(), workload, model.Node{ID: "node-1", Address: "node.test"}); err != nil {
		t.Fatalf("retry Execute() error = %v", err)
	}
	if node.createCalls != 2 || store.persistCalls != 2 || store.executionState != model.ExecutionRunning {
		t.Fatalf("retry did not converge: node=%+v store=%+v", node, store)
	}
}

func TestExecutionStartFailureIsRecoverable(t *testing.T) {
	store := &fakeExecutionStore{}
	node := &fakeExecutionNode{
		container: runtime.Container{ID: "container-1", Status: "created"},
		startErr: errors.New("daemon unavailable"),
	}
	executor := &WorkloadExecutor{store: store, nodeClient: node}

	err := executor.Execute(context.Background(), recoveryWorkload(), model.Node{ID: "node-1", Address: "node.test"})
	if err == nil {
		t.Fatal("Execute() succeeded despite start failure")
	}
	if store.executionState != model.ExecutionCreated {
		t.Fatalf("execution state = %q, want CREATED", store.executionState)
	}
}

func TestExecutionConflictMarksExplicitFailure(t *testing.T) {
	store := &fakeExecutionStore{}
	node := &fakeExecutionNode{createErr: errors.Join(runtime.ErrContainerConflict, errors.New("name belongs to another workload"))}
	executor := &WorkloadExecutor{store: store, nodeClient: node}

	err := executor.Execute(context.Background(), recoveryWorkload(), model.Node{ID: "node-1", Address: "node.test"})
	if !errors.Is(err, runtime.ErrContainerConflict) {
		t.Fatalf("Execute() error = %v, want container conflict", err)
	}
	if store.executionState != model.ExecutionFailed || !store.failed {
		t.Fatalf("conflict was not marked terminal: %+v", store)
	}
}
