package reconciler

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"

	"mini-orchestrator/internal/api"
	"mini-orchestrator/internal/controller"
	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/observability"
	orchestratorRuntime "mini-orchestrator/internal/runtime"
	"mini-orchestrator/internal/scheduler"
	"mini-orchestrator/internal/store"
	"mini-orchestrator/internal/transporttls"
)

const (
	integrationOptInEnv = "MINI_ORCHESTRATOR_INTEGRATION"
	integrationTimeout  = 2 * time.Minute
)

type multiNodeEnvironment struct {
	database     *sql.DB
	store        *store.Postgres
	dsn          string
	controlPlane *httptest.Server
	agents       []*node.Agent
	nodes        []model.Node
	token        string
	clientTLS    *tls.Config
	tlsFiles     integrationTLSFiles
}

type integrationTLSFiles struct {
	certificate string
	privateKey  string
	ca          string
	nodes       map[string]integrationTLSFiles
}

type integrationContainer struct {
	config orchestratorRuntime.ContainerConfig
	state  string
}

type integrationRuntime struct {
	mu         sync.Mutex
	prefix     string
	nextID     int
	containers map[string]integrationContainer
}

func newIntegrationRuntime(prefix string) *integrationRuntime {
	return &integrationRuntime{
		prefix:     prefix,
		containers: make(map[string]integrationContainer),
	}
}

func (r *integrationRuntime) Create(
	_ context.Context,
	config orchestratorRuntime.ContainerConfig,
) (orchestratorRuntime.Container, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	id := fmt.Sprintf("%s-container-%d", r.prefix, r.nextID)
	r.containers[id] = integrationContainer{
		config: config,
		state:  "created",
	}

	return orchestratorRuntime.Container{
		ID:     id,
		Name:   config.Name,
		Image:  config.Image,
		Status: "created",
	}, nil
}

func (r *integrationRuntime) Start(
	_ context.Context,
	containerID string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	container, ok := r.containers[containerID]
	if !ok {
		return fmt.Errorf("container %q not found", containerID)
	}
	container.state = "running"
	r.containers[containerID] = container
	return nil
}

func (r *integrationRuntime) Stop(
	_ context.Context,
	containerID string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	container, ok := r.containers[containerID]
	if !ok {
		return fmt.Errorf("%w: %s", orchestratorRuntime.ErrContainerNotFound, containerID)
	}
	container.state = "exited"
	r.containers[containerID] = container
	return nil
}

func (r *integrationRuntime) Remove(
	_ context.Context,
	containerID string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.containers[containerID]; !ok {
		return fmt.Errorf("%w: %s", orchestratorRuntime.ErrContainerNotFound, containerID)
	}
	delete(r.containers, containerID)
	return nil
}

func (r *integrationRuntime) Inspect(
	_ context.Context,
	containerID string,
) (orchestratorRuntime.Container, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	container, ok := r.containers[containerID]
	if !ok {
		return orchestratorRuntime.Container{}, fmt.Errorf(
			"%w: %s",
			orchestratorRuntime.ErrContainerNotFound,
			containerID,
		)
	}
	return orchestratorRuntime.Container{
		ID:     containerID,
		Name:   container.config.Name,
		Image:  container.config.Image,
		Status: container.state,
	}, nil
}

func (r *integrationRuntime) List(
	_ context.Context,
) ([]orchestratorRuntime.Container, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]orchestratorRuntime.Container, 0, len(r.containers))
	for id, container := range r.containers {
		result = append(result, orchestratorRuntime.Container{
			ID:     id,
			Name:   container.config.Name,
			Image:  container.config.Image,
			Status: container.state,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (r *integrationRuntime) Stats(
	_ context.Context,
	_ string,
) (orchestratorRuntime.ContainerStats, error) {
	return orchestratorRuntime.ContainerStats{}, nil
}

func TestMultiNodeSchedulingIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	serviceID := integrationID(t, "service")
	drainingNodeID := integrationID(t, "draining")
	env.cleanupTestData(t, serviceID, drainingNodeID)

	drainingIdentity := "spiffe://mini-orchestrator/node/" + drainingNodeID
	if _, err := env.database.ExecContext(ctx, `
		INSERT INTO nodes (id, address, cpu_capacity_millis, memory_capacity_mb, status, certificate_identity)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, drainingNodeID, "127.0.0.1:1", 8000, 8192, model.NodeDraining, drainingIdentity); err != nil {
		t.Fatalf("enroll draining node: %v", err)
	}
	if _, err := env.store.RegisterNode(ctx, model.Node{
		ID:             drainingNodeID,
		Address:        "127.0.0.1:1",
		CPUCapacity:    8000,
		MemoryCapacity: 8192,
		Status:         model.NodeDraining,
	}, drainingIdentity, "integration-draining-"+drainingNodeID); err != nil {
		t.Fatalf("register draining node: %v", err)
	}

	service := model.Service{
		ID:                serviceID,
		Name:              serviceID,
		Image:             "integration/test:latest",
		DesiredReplicas:   2,
		CPURequestMillis:  500,
		MemoryRequestMB:   256,
		DeploymentVersion: 1,
		MaxSurge:          1,
		DeploymentStatus:  model.DeploymentAvailable,
		Status:            model.ServiceActive,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
	if err := env.store.CreateService(ctx, service); err != nil {
		t.Fatalf("create service: %v", err)
	}

	replicas := controller.NewReplicaController(env.store)
	if err := replicas.ReconcileService(ctx, service); err != nil {
		t.Fatalf("create service replicas: %v", err)
	}

	workloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list pending workloads: %v", err)
	}
	if len(workloads) != service.DesiredReplicas {
		t.Fatalf("created %d workloads; want %d", len(workloads), service.DesiredReplicas)
	}

	schedulerService := scheduler.NewService(env.store, scheduler.New())
	for _, workload := range workloads {
		if workload.ActualState != model.WorkloadPending {
			t.Fatalf("new workload %q state = %q; want PENDING", workload.ID, workload.ActualState)
		}
		if err := schedulerService.ScheduleWorkload(ctx, workload); err != nil {
			t.Fatalf("schedule workload %q: %v", workload.ID, err)
		}
	}

	executor := NewWorkloadExecutor(env.store, env.nodeClient())
	workloads, err = env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list scheduled workloads: %v", err)
	}

	eligibleNodes := map[string]struct{}{
		env.nodes[0].ID: {},
		env.nodes[1].ID: {},
	}
	usedNodes := make(map[string]struct{})
	for _, workload := range workloads {
		if workload.NodeID == nil {
			t.Fatalf("scheduled workload %q has no node assignment", workload.ID)
		}
		if _, ok := eligibleNodes[*workload.NodeID]; !ok {
			t.Errorf("workload %q assigned to ineligible node %q", workload.ID, *workload.NodeID)
			continue
		}
		usedNodes[*workload.NodeID] = struct{}{}

		node, err := env.store.GetNode(ctx, *workload.NodeID)
		if err != nil {
			t.Fatalf("get node %q: %v", *workload.NodeID, err)
		}
		if err := executor.Execute(ctx, workload, node); err != nil {
			t.Fatalf("execute workload %q on node %q: %v", workload.ID, node.ID, err)
		}
	}
	if len(usedNodes) != 2 {
		t.Errorf("workloads used %d eligible nodes; want both nodes", len(usedNodes))
	}

	for _, node := range env.nodes {
		updated, err := env.store.GetNode(ctx, node.ID)
		if err != nil {
			t.Fatalf("get updated node %q: %v", node.ID, err)
		}
		if updated.CPUAllocated > updated.CPUCapacity {
			t.Errorf("node %q CPU allocation %d exceeds capacity %d",
				node.ID, updated.CPUAllocated, updated.CPUCapacity)
		}
		if updated.MemoryAllocated > updated.MemoryCapacity {
			t.Errorf("node %q memory allocation %d exceeds capacity %d",
				node.ID, updated.MemoryAllocated, updated.MemoryCapacity)
		}
		if updated.CPUAllocated != 500 || updated.MemoryAllocated != 256 {
			t.Errorf("node %q allocations = CPU %d, memory %d; want CPU 500, memory 256",
				node.ID, updated.CPUAllocated, updated.MemoryAllocated)
		}
	}
	assertAllocationsMatchActiveWorkloads(t, ctx, env, serviceID)

	for _, request := range []struct {
		cpu    int
		memory int
	}{
		{cpu: 1001, memory: 128},
		{cpu: 100, memory: 513},
	} {
		workload := model.Workload{
			ID:                integrationID(t, "unschedulable"),
			ServiceID:         serviceID,
			Image:             service.Image,
			CPURequestMillis:  request.cpu,
			MemoryRequestMB:   request.memory,
			DeploymentVersion: service.DeploymentVersion,
		}
		if err := env.store.CreatePendingWorkload(ctx, workload); err != nil {
			t.Fatalf("create unschedulable workload: %v", err)
		}
		if err := schedulerService.ScheduleWorkload(ctx, workload); err == nil {
			t.Errorf("workload requesting CPU %d and memory %d unexpectedly scheduled",
				request.cpu, request.memory)
		}

		got, err := env.store.GetWorkload(ctx, workload.ID)
		if err != nil {
			t.Fatalf("get unschedulable workload: %v", err)
		}
		if got.ActualState != model.WorkloadPending ||
			got.DesiredState != model.WorkloadPending ||
			got.NodeID != nil {
			t.Errorf("unschedulable workload state = (%q, %q, node %v); want pending and unassigned",
				got.ActualState, got.DesiredState, got.NodeID)
		}
		assertAllocationsMatchActiveWorkloads(t, ctx, env, serviceID)
	}
}

func TestMissingContainerRecoveryAndMetricsIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	serviceID := integrationID(t, "missing-container-service")
	env.cleanupTestData(t, serviceID)
	service := model.Service{
		ID:                serviceID,
		Name:              serviceID,
		Image:             "integration/test:latest",
		DesiredReplicas:   1,
		CPURequestMillis:  500,
		MemoryRequestMB:   256,
		DeploymentVersion: 1,
		MaxSurge:          1,
		DeploymentStatus:  model.DeploymentAvailable,
		Status:            model.ServiceActive,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
	if err := env.store.CreateService(ctx, service); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := controller.NewReplicaController(env.store).ReconcileService(ctx, service); err != nil {
		t.Fatalf("create service replica: %v", err)
	}
	workloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list pending workloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("workload count = %d; want 1", len(workloads))
	}
	schedulerService := scheduler.NewService(env.store, scheduler.New())
	if err := schedulerService.ScheduleWorkload(ctx, workloads[0]); err != nil {
		t.Fatalf("schedule workload: %v", err)
	}
	workloads, err = env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list scheduled workload: %v", err)
	}
	workload := workloads[0]
	if workload.NodeID == nil {
		t.Fatal("scheduled workload has no node assignment")
	}
	assignedNode, err := env.store.GetNode(ctx, *workload.NodeID)
	if err != nil {
		t.Fatalf("get assigned node: %v", err)
	}
	metrics := &observability.Metrics{}
	client := env.nodeClient()
	executor := NewWorkloadExecutor(env.store, client)
	if err := executor.Execute(ctx, workload, assignedNode); err != nil {
		t.Fatalf("execute workload over mTLS agent endpoint: %v", err)
	}
	workloads, err = env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read running workload: %v", err)
	}
	workload = workloads[0]
	if workload.ActualState != model.WorkloadRunning || workload.ContainerID == nil {
		t.Fatalf("workload state = %q, container = %v; want running with container",
			workload.ActualState, workload.ContainerID)
	}

	var owningAgent *node.Agent
	for _, agent := range env.agents {
		if agent.ID == assignedNode.ID {
			owningAgent = agent
			break
		}
	}
	if owningAgent == nil {
		t.Fatalf("no integration agent for node %q", assignedNode.ID)
	}
	if err := owningAgent.Runtime.Remove(ctx, *workload.ContainerID); err != nil {
		t.Fatalf("remove test-created container: %v", err)
	}

	runtimeReconciler := NewRuntimeReconciler(env.store, client, metrics)
	if err := runtimeReconciler.ReconcileNode(ctx, assignedNode); err != nil {
		t.Fatalf("reconcile missing container: %v", err)
	}
	failed, err := env.store.GetWorkload(ctx, workload.ID)
	if err != nil {
		t.Fatalf("read failed workload: %v", err)
	}
	if failed.ActualState != model.WorkloadFailed {
		t.Fatalf("workload state = %q; want FAILED", failed.ActualState)
	}
	updatedNode, err := env.store.GetNode(ctx, assignedNode.ID)
	if err != nil {
		t.Fatalf("read node resource allocation: %v", err)
	}
	if updatedNode.CPUAllocated != 0 || updatedNode.MemoryAllocated != 0 {
		t.Fatalf("node allocation after failure = CPU %d, memory %d; want zero",
			updatedNode.CPUAllocated, updatedNode.MemoryAllocated)
	}

	if err := runtimeReconciler.ReconcileNode(ctx, assignedNode); err != nil {
		t.Fatalf("reconcile already-processed missing container: %v", err)
	}
	response := httptest.NewRecorder()
	metrics.Handler()(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		"orchestrator_missing_containers_total 1\n",
		"orchestrator_workloads_failed_total 1\n",
	} {
		if !strings.Contains(response.Body.String(), want) {
			t.Errorf("metrics response does not contain %q:\n%s", want, response.Body.String())
		}
	}
}

func TestMultiNodeServiceDeletionIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	serviceID := integrationID(t, "service")
	env.cleanupTestData(t, serviceID)

	service := model.Service{
		ID:                serviceID,
		Name:              serviceID,
		Image:             "integration/test:latest",
		DesiredReplicas:   1,
		CPURequestMillis:  500,
		MemoryRequestMB:   256,
		DeploymentVersion: 1,
		MaxSurge:          1,
		DeploymentStatus:  model.DeploymentAvailable,
		Status:            model.ServiceActive,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
	if err := env.store.CreateService(ctx, service); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := controller.NewReplicaController(env.store).ReconcileService(ctx, service); err != nil {
		t.Fatalf("create service replica: %v", err)
	}

	workloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list service workloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("created %d workloads; want 1", len(workloads))
	}

	schedulerService := scheduler.NewService(env.store, scheduler.New())
	if err := schedulerService.ScheduleWorkload(ctx, workloads[0]); err != nil {
		t.Fatalf("schedule workload: %v", err)
	}
	workloads, err = env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list scheduled workload: %v", err)
	}
	if workloads[0].NodeID == nil {
		t.Fatal("scheduled workload has no node")
	}

	executor := NewWorkloadExecutor(env.store, env.nodeClient())
	assignedNode, err := env.store.GetNode(ctx, *workloads[0].NodeID)
	if err != nil {
		t.Fatalf("get workload node: %v", err)
	}
	if err := executor.Execute(ctx, workloads[0], assignedNode); err != nil {
		t.Fatalf("execute workload: %v", err)
	}
	workloads, err = env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read running workload: %v", err)
	}
	if workloads[0].ActualState != model.WorkloadRunning || workloads[0].ContainerID == nil {
		t.Fatalf("workload state = %q, container = %v; want RUNNING with a container",
			workloads[0].ActualState, workloads[0].ContainerID)
	}
	staleRunningWorkload := workloads[0]
	var owningAgent *node.Agent
	for _, agent := range env.agents {
		if agent.ID == assignedNode.ID {
			owningAgent = agent
			break
		}
	}
	if owningAgent == nil {
		t.Fatalf("no integration agent owns node %q", assignedNode.ID)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		env.controlPlane.URL+"/services/"+serviceID,
		nil,
	)
	if err != nil {
		t.Fatalf("create service deletion request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+env.token)
	response, err := env.controlPlane.Client().Do(request)
	if err != nil {
		t.Fatalf("send service deletion request: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("DELETE service status = %d; want %d",
			response.StatusCode, http.StatusAccepted)
	}
	deletingService, err := env.store.GetService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read service deletion state: %v", err)
	}
	if err := controller.NewDeletionController(env.store).ReconcileService(ctx, deletingService); err != nil {
		t.Fatalf("mark workloads stopped before test container disappearance: %v", err)
	}
	if err := owningAgent.Runtime.Remove(ctx, *staleRunningWorkload.ContainerID); err != nil {
		t.Fatalf("remove test container immediately before stop: %v", err)
	}
	staleRunningWorkload.DesiredState = model.WorkloadStopped
	if err := executor.Stop(ctx, staleRunningWorkload, assignedNode); err != nil {
		t.Fatalf("stop workload after container disappeared: %v", err)
	}

	reconciler := New(
		env.store,
		controller.NewReplicaController(env.store),
		controller.NewDeploymentController(env.store),
		controller.NewDeploymentStatusController(env.store, time.Minute),
		schedulerService,
		executor,
		NewRuntimeReconciler(
			env.store,
			env.nodeClient(),
			&observability.Metrics{},
		),
		10*time.Millisecond,
		&observability.Metrics{},
	)
	pollUntil(t, ctx, 10*time.Second, func() (bool, error) {
		if err := reconciler.reconcile(ctx); err != nil {
			return false, err
		}
		updated, err := env.store.GetService(ctx, serviceID)
		if err != nil {
			return false, err
		}
		return updated.Status == model.ServiceDeleted, nil
	})

	history, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list workload history after deletion: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("workload history has %d rows after deletion; want 1", len(history))
	}
	if history[0].ActualState != model.WorkloadStopped {
		t.Errorf("deleted service workload state = %q; want STOPPED", history[0].ActualState)
	}
	if history[0].NodeID == nil {
		t.Fatal("stopped workload lost its node ID before history was checked")
	}
	if history[0].ContainerID == nil {
		t.Fatal("stopped workload lost its container ID before history was checked")
	}
	if err := executor.Stop(ctx, staleRunningWorkload, assignedNode); err != nil {
		t.Fatalf("repeat stop using stale running snapshot: %v", err)
	}
	agentFound := false
	for _, agent := range env.agents {
		if agent.ID != *history[0].NodeID {
			continue
		}
		agentFound = true
		if _, err := agent.Runtime.Inspect(ctx, *history[0].ContainerID); err == nil {
			t.Errorf("container %q still exists after service deletion", *history[0].ContainerID)
		}
	}
	if !agentFound {
		t.Errorf("no test node agent owns workload node %q", *history[0].NodeID)
	}

	assertAllocationsMatchActiveWorkloads(t, ctx, env, serviceID)
}

func TestResourceReservationIdempotencyIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	serviceID := integrationID(t, "reservation-service")
	nodeID := integrationID(t, "reservation-node")
	env.cleanupTestData(t, serviceID, nodeID)

	nodeIdentity := "spiffe://mini-orchestrator/node/" + nodeID
	if _, err := env.database.ExecContext(ctx, `
		INSERT INTO nodes (id, address, cpu_capacity_millis, memory_capacity_mb, status, certificate_identity)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, nodeID, "127.0.0.1:1", 1000, 1024, model.NodeReady, nodeIdentity); err != nil {
		t.Fatalf("enroll reservation node: %v", err)
	}
	if _, err := env.store.RegisterNode(ctx, model.Node{
		ID: nodeID, Address: "127.0.0.1:1", CPUCapacity: 1000, MemoryCapacity: 1024,
		Status: model.NodeReady, LastHeartbeat: time.Now().UTC(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}, nodeIdentity, "integration-reservation-"+nodeID); err != nil {
		t.Fatalf("register reservation node: %v", err)
	}
	service := model.Service{
		ID: serviceID, Name: serviceID, Image: "integration/test:latest", DesiredReplicas: 1,
		CPURequestMillis: 300, MemoryRequestMB: 256, DeploymentVersion: 1, MaxSurge: 1,
		DeploymentStatus: model.DeploymentAvailable, Status: model.ServiceActive,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := env.store.CreateService(ctx, service); err != nil {
		t.Fatalf("create reservation service: %v", err)
	}
	workload := model.Workload{
		ID: integrationID(t, "reservation-workload"), ServiceID: serviceID, Image: service.Image,
		CPURequestMillis: 300, MemoryRequestMB: 256, DeploymentVersion: 1,
		DesiredState: model.WorkloadPending, ActualState: model.WorkloadPending,
		ExecutionState: model.ExecutionIdle, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := env.store.CreatePendingWorkload(ctx, workload); err != nil {
		t.Fatalf("create pending workload: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- env.store.ReserveNodeForWorkload(ctx, workload.ID, nodeID, workload.CPURequestMillis, workload.MemoryRequestMB)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reservation failed: %v", err)
		}
	}

	stored, err := env.store.GetWorkload(ctx, workload.ID)
	if err != nil {
		t.Fatalf("get reserved workload: %v", err)
	}
	if !stored.ResourceReserved || stored.DesiredState != model.WorkloadScheduled || stored.NodeID == nil {
		t.Fatalf("unexpected reservation state: reserved=%v desired=%q node=%v", stored.ResourceReserved, stored.DesiredState, stored.NodeID)
	}
	node, err := env.store.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("get reservation node: %v", err)
	}
	if node.CPUAllocated != 300 || node.MemoryAllocated != 256 {
		t.Fatalf("allocated resources = (%d CPU, %d memory); want (300, 256)", node.CPUAllocated, node.MemoryAllocated)
	}

	if err := env.store.MarkWorkloadStoppedAndReleaseResources(ctx, workload.ID); err != nil {
		t.Fatalf("first stop release: %v", err)
	}
	if err := env.store.MarkWorkloadStoppedAndReleaseResources(ctx, workload.ID); err != nil {
		t.Fatalf("repeated stop release: %v", err)
	}
	stored, err = env.store.GetWorkload(ctx, workload.ID)
	if err != nil {
		t.Fatalf("get stopped workload: %v", err)
	}
	if stored.ResourceReserved || stored.ActualState != model.WorkloadStopped || stored.NodeID != nil {
		t.Fatalf("unexpected stopped state: reserved=%v actual=%q node=%v", stored.ResourceReserved, stored.ActualState, stored.NodeID)
	}
	node, err = env.store.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("get released node: %v", err)
	}
	if node.CPUAllocated != 0 || node.MemoryAllocated != 0 {
		t.Fatalf("allocated resources after release = (%d CPU, %d memory); want zero", node.CPUAllocated, node.MemoryAllocated)
	}
}

func newMultiNodeEnvironment(t *testing.T) *multiNodeEnvironment {
	t.Helper()

	if os.Getenv(integrationOptInEnv) != "1" {
		t.Skipf("set %s=1 to run PostgreSQL-backed multi-node integration tests", integrationOptInEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		t.Fatalf("create Docker test client: %v", err)
	}
	t.Cleanup(func() {
		_ = dockerClient.Close()
	})

	image := "postgres:18"
	if _, _, err := dockerClient.ImageInspectWithRaw(ctx, image); err != nil {
		reader, pullErr := dockerClient.ImagePull(ctx, image, types.ImagePullOptions{})
		if pullErr != nil {
			t.Fatalf("pull isolated PostgreSQL image %q: %v", image, pullErr)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			t.Fatalf("read PostgreSQL image pull: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("close PostgreSQL image pull: %v", err)
		}
	}

	suffix := integrationID(t, "it")
	containerName := "mini-orchestrator-" + suffix
	username := "orchestrator_it"
	password := integrationID(t, "pw")
	databaseName := "orchestrator_it"
	port, err := nat.NewPort("tcp", "5432")
	if err != nil {
		t.Fatalf("create PostgreSQL port: %v", err)
	}
	created, err := dockerClient.ContainerCreate(
		ctx,
		&container.Config{
			Image: image,
			Env: []string{
				"POSTGRES_USER=" + username,
				"POSTGRES_PASSWORD=" + password,
				"POSTGRES_DB=" + databaseName,
			},
			ExposedPorts: nat.PortSet{port: struct{}{}},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				port: []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: "0"}},
			},
		},
		nil,
		nil,
		containerName,
	)
	if err != nil {
		t.Fatalf("create isolated PostgreSQL container: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := dockerClient.ContainerRemove(
			cleanupCtx,
			created.ID,
			container.RemoveOptions{Force: true},
		); err != nil {
			t.Errorf("remove test-created PostgreSQL container %q: %v", created.ID, err)
		}
	})
	if err := dockerClient.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start isolated PostgreSQL container: %v", err)
	}

	inspection, err := dockerClient.ContainerInspect(ctx, created.ID)
	if err != nil {
		t.Fatalf("inspect isolated PostgreSQL container: %v", err)
	}
	bindings := inspection.NetworkSettings.Ports[port]
	if len(bindings) == 0 {
		t.Fatal("isolated PostgreSQL container has no published port")
	}
	hostPort := bindings[0].HostPort
	if hostPort == "" {
		t.Fatal("isolated PostgreSQL container has an empty published port")
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@127.0.0.1:%s/%s?sslmode=disable",
		username,
		password,
		hostPort,
		databaseName,
	)
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open isolated PostgreSQL database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close isolated PostgreSQL database: %v", err)
		}
	})
	waitForPostgres(t, ctx, database)
	applyMigrations(t, ctx, database)

	pgStore, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect orchestrator store to isolated PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := pgStore.Close(); err != nil {
			t.Errorf("close orchestrator store: %v", err)
		}
	})

	token := integrationID(t, "token")
	serverTLS, clientTLS, tlsFiles := integrationTLSConfig(t)
	controlPlane := httptest.NewUnstartedServer(api.NewServer(
		pgStore,
		token,
		&observability.Metrics{},
	).Routes())
	controlPlane.TLS = serverTLS
	controlPlane.StartTLS()
	controlPlane.Client().Transport = &http.Transport{TLSClientConfig: clientTLS}
	t.Cleanup(controlPlane.Close)

	env := &multiNodeEnvironment{
		database:     database,
		store:        pgStore,
		dsn:          dsn,
		controlPlane: controlPlane,
		token:        token,
		clientTLS:    clientTLS,
		tlsFiles:     tlsFiles,
	}
	for index := 0; index < 2; index++ {
		nodeID := fmt.Sprintf("worker-%02d", index+1)
		nodeFiles := tlsFiles.nodes[nodeID]
		nodeServerTLS, nodeClientTLS, err := transporttls.Load(
			nodeFiles.certificate,
			nodeFiles.privateKey,
			nodeFiles.ca,
		)
		if err != nil {
			t.Fatalf("load TLS configuration for node %q: %v", nodeID, err)
		}
		agent := &node.Agent{
			ID:              nodeID,
			CPUCapacity:     1000,
			MemoryCapacity:  512,
			ControlPlaneURL: controlPlane.URL,
			Client: &http.Client{
				Transport: &http.Transport{TLSClientConfig: nodeClientTLS},
				Timeout:   5 * time.Second,
			},
			Runtime: newIntegrationRuntime(nodeID),
			Token:   token,
		}
		agentServer := httptest.NewUnstartedServer(api.RequireBearerToken(token, agent.Routes()))
		agentServer.TLS = nodeServerTLS.Clone()
		agentServer.StartTLS()
		t.Cleanup(agentServer.Close)

		parsedURL, err := url.Parse(agentServer.URL)
		if err != nil {
			t.Fatalf("parse node agent URL: %v", err)
		}
		agent.Address = parsedURL.Host
		certificateIdentity := "spiffe://mini-orchestrator/node/" + nodeID
		if _, err := env.database.ExecContext(ctx, `
			INSERT INTO nodes (id, address, cpu_capacity_millis, memory_capacity_mb, status, certificate_identity)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO UPDATE SET
				address = EXCLUDED.address,
				cpu_capacity_millis = EXCLUDED.cpu_capacity_millis,
				memory_capacity_mb = EXCLUDED.memory_capacity_mb,
				certificate_identity = EXCLUDED.certificate_identity,
				updated_at = NOW()
		`, nodeID, agent.Address, agent.CPUCapacity, agent.MemoryCapacity, model.NodeReady, certificateIdentity); err != nil {
			t.Fatalf("enroll node agent %q: %v", nodeID, err)
		}
		if err := agent.Register(ctx); err != nil {
			t.Fatalf("register node agent %q: %v", agent.ID, err)
		}

		staleHeartbeat := time.Now().UTC().Add(-time.Minute)
		if _, err := database.ExecContext(
			ctx,
			`UPDATE nodes SET last_heartbeat = $1 WHERE id = $2`,
			staleHeartbeat,
			agent.ID,
		); err != nil {
			t.Fatalf("set stale heartbeat for test node %q: %v", agent.ID, err)
		}
		if err := agent.Heartbeat(ctx); err != nil {
			t.Fatalf("send heartbeat for node %q: %v", agent.ID, err)
		}
		registered, err := pgStore.GetNode(ctx, agent.ID)
		if err != nil {
			t.Fatalf("get registered node %q: %v", agent.ID, err)
		}
		if registered.Status != model.NodeReady || !registered.LastHeartbeat.After(staleHeartbeat) {
			t.Fatalf("node %q heartbeat state = (%q, %s); want READY and newer heartbeat",
				agent.ID, registered.Status, registered.LastHeartbeat)
		}
		if registered.LeaseGeneration < 1 {
			t.Fatalf("node %q has invalid lease generation %d", agent.ID, registered.LeaseGeneration)
		}
		if registered.RegistrationID == "" {
			t.Fatalf("node %q has no registration id", agent.ID)
		}
		if agent.LeaseGeneration != registered.LeaseGeneration {
			t.Fatalf("agent %q generation = %d; database generation = %d", agent.ID, agent.LeaseGeneration, registered.LeaseGeneration)
		}

		env.agents = append(env.agents, agent)
		env.nodes = append(env.nodes, registered)
	}

	return env
}

func waitForPostgres(t *testing.T, ctx context.Context, database *sql.DB) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		lastErr = database.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("isolated PostgreSQL did not become ready: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func pollUntil(
	t *testing.T,
	ctx context.Context,
	timeout time.Duration,
	condition func() (bool, error),
) {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		done, err := condition()
		if err != nil {
			t.Fatalf("poll condition: %v", err)
		}
		if done {
			return
		}
		select {
		case <-pollCtx.Done():
			t.Fatalf("condition was not met within %s: %v", timeout, pollCtx.Err())
		case <-ticker.C:
		}
	}
}

func TestNodeLeaseGenerationIntegration(t *testing.T) {
	if os.Getenv(integrationOptInEnv) != "1" {
		t.Skip("set MINI_ORCHESTRATOR_INTEGRATION=1 to run PostgreSQL-backed multi-node integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	env := newMultiNodeEnvironment(t)
	nodeID := integrationID(t, "lease-node")
	env.cleanupTestData(t, nodeID)

	identity := "spiffe://mini-orchestrator/node/" + nodeID
	if _, err := env.database.ExecContext(ctx, `
		INSERT INTO nodes (id, address, cpu_capacity_millis, memory_capacity_mb, status, certificate_identity)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, nodeID, "127.0.0.1:1", 1000, 512, model.NodeReady, identity); err != nil {
		t.Fatalf("enroll lease node: %v", err)
	}

	const concurrentRegistrations = 8
	generations := make(chan int64, concurrentRegistrations)
	registrationErrors := make(chan error, concurrentRegistrations)
	var wg sync.WaitGroup
	for i := 0; i < concurrentRegistrations; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			generation, err := env.store.RegisterNode(ctx, model.Node{
				ID: nodeID, Address: fmt.Sprintf("127.0.0.1:%d", 9000+index),
				CPUCapacity: 1000, MemoryCapacity: 512, Status: model.NodeReady,
			}, identity, fmt.Sprintf("lease-owner-%02d", index))
			if err != nil {
				registrationErrors <- err
				return
			}
			generations <- generation
		}(i)
	}
	wg.Wait()
	close(generations)
	close(registrationErrors)
	for err := range registrationErrors {
		if err != nil {
			t.Fatalf("concurrent registration: %v", err)
		}
	}

	var finalGeneration int64
	if err := env.database.QueryRowContext(ctx,
		`SELECT lease_generation FROM nodes WHERE id = $1`, nodeID,
	).Scan(&finalGeneration); err != nil {
		t.Fatalf("read final lease generation: %v", err)
	}
	if finalGeneration != concurrentRegistrations+1 {
		t.Fatalf("final generation = %d; want %d", finalGeneration, concurrentRegistrations+1)
	}
	for generation := range generations {
		if generation < 1 || generation > finalGeneration {
			t.Fatalf("unexpected registration generation %d for final %d", generation, finalGeneration)
		}
	}

	retryGeneration := int64(0)
	for i := 0; i < 3; i++ {
		generation, err := env.store.RegisterNode(ctx, model.Node{
			ID: nodeID, Address: "127.0.0.1:9999", CPUCapacity: 1000,
			MemoryCapacity: 512, Status: model.NodeReady,
		}, identity, "lease-retry-owner")
		if err != nil {
			t.Fatalf("registration retry %d: %v", i, err)
		}
		if i == 0 {
			retryGeneration = generation
			if retryGeneration != finalGeneration+1 {
				t.Fatalf("first retry generation = %d; want %d", retryGeneration, finalGeneration+1)
			}
		} else if generation != retryGeneration {
			t.Fatalf("retry generation = %d; want stable generation %d", generation, retryGeneration)
		}
	}
	if err := env.store.UpdateHeartbeat(ctx, nodeID, identity, retryGeneration-1, model.NodeReady, time.Now().UTC()); !errors.Is(err, store.ErrNodeLeaseStale) {
		t.Fatalf("old heartbeat error = %v; want stale lease error", err)
	}
	if err := env.store.SetNodeStatus(ctx, nodeID, model.NodeUnhealthy); err != nil {
		t.Fatalf("set unhealthy before stale heartbeat: %v", err)
	}
	if err := env.store.UpdateHeartbeat(ctx, nodeID, identity, retryGeneration-1, model.NodeReady, time.Now().UTC()); !errors.Is(err, store.ErrNodeLeaseStale) {
		t.Fatalf("stale heartbeat error = %v; want stale lease error", err)
	}
	registered, err := env.store.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("get node after stale heartbeat: %v", err)
	}
	if registered.Status != model.NodeUnhealthy {
		t.Fatalf("stale heartbeat changed status to %q; want UNHEALTHY", registered.Status)
	}
	if err := env.store.UpdateHeartbeat(ctx, nodeID, identity, retryGeneration, model.NodeReady, time.Now().UTC()); err != nil {
		t.Fatalf("current heartbeat: %v", err)
	}
}

func applyMigrations(t *testing.T, ctx context.Context, database *sql.DB) {
	t.Helper()

	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source file")
	}
	migrationDir := filepath.Join(filepath.Dir(sourceFile), "..", "..", "migrations")
	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		t.Fatalf("read migrations directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		migration, err := os.ReadFile(filepath.Join(migrationDir, entry.Name()))
		if err != nil {
			t.Fatalf("read migration %q: %v", entry.Name(), err)
		}
		if _, err := database.ExecContext(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %q: %v", entry.Name(), err)
		}
	}
}

func (e *multiNodeEnvironment) cleanupTestData(
	t *testing.T,
	serviceID string,
	additionalNodeIDs ...string,
) {
	t.Helper()
	nodeIDs := make([]string, 0, len(e.nodes)+len(additionalNodeIDs))
	for _, node := range e.nodes {
		nodeIDs = append(nodeIDs, node.ID)
	}
	nodeIDs = append(nodeIDs, additionalNodeIDs...)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := e.database.ExecContext(
			ctx,
			`DELETE FROM services WHERE id = $1`,
			serviceID,
		); err != nil {
			t.Errorf("clean up test service %q: %v", serviceID, err)
		}
		for _, nodeID := range nodeIDs {
			if _, err := e.database.ExecContext(
				ctx,
				`DELETE FROM nodes WHERE id = $1`,
				nodeID,
			); err != nil {
				t.Errorf("clean up test node %q: %v", nodeID, err)
			}
		}
	})
}

func assertAllocationsMatchActiveWorkloads(
	t *testing.T,
	ctx context.Context,
	env *multiNodeEnvironment,
	serviceID string,
) {
	t.Helper()

	nodes, err := env.store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("list nodes for allocation check: %v", err)
	}
	workloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list workloads for allocation check: %v", err)
	}

	type reservation struct {
		cpu    int
		memory int
	}
	reservations := make(map[string]reservation)
	for _, workload := range workloads {
		if workload.NodeID == nil || workload.DesiredState == model.WorkloadStopped {
			continue
		}
		switch workload.ActualState {
		case model.WorkloadPending, model.WorkloadScheduled, model.WorkloadRunning:
			current := reservations[*workload.NodeID]
			current.cpu += workload.CPURequestMillis
			current.memory += workload.MemoryRequestMB
			reservations[*workload.NodeID] = current
		}
	}

	for _, node := range nodes {
		reserved := reservations[node.ID]
		if node.CPUAllocated != reserved.cpu || node.MemoryAllocated != reserved.memory {
			t.Errorf("node %q allocation = CPU %d, memory %d; active workloads reserve CPU %d, memory %d",
				node.ID, node.CPUAllocated, node.MemoryAllocated, reserved.cpu, reserved.memory)
		}
	}
}

func integrationID(t *testing.T, prefix string) string {
	t.Helper()
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		t.Fatalf("generate integration identifier: %v", err)
	}
	return prefix + "-" + hex.EncodeToString(randomBytes[:])
}

func (e *multiNodeEnvironment) nodeClient() *node.Client {
	return node.NewClient(&http.Client{
		Transport: &http.Transport{TLSClientConfig: e.clientTLS},
		Timeout:   5 * time.Second,
	}, e.token)
}

func integrationTLSConfig(t *testing.T) (*tls.Config, *tls.Config, integrationTLSFiles) {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate integration CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "integration-only CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create integration CA: %v", err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	writeIntegrationPEM(t, caPath, "CERTIFICATE", caDER)

	issue := func(name string, serial int64, nodeID string) integrationTLSFiles {
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate integration leaf key for %s: %v", name, err)
		}
		leafTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "localhost"},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			NotBefore:    now.Add(-time.Minute),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		if nodeID != "" {
			nodeURI, err := url.Parse("spiffe://mini-orchestrator/node/" + nodeID)
			if err != nil {
				t.Fatalf("parse node URI for %s: %v", nodeID, err)
			}
			leafTemplate.URIs = []*url.URL{nodeURI}
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create integration leaf for %s: %v", name, err)
		}
		leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			t.Fatalf("marshal integration leaf key for %s: %v", name, err)
		}
		certPath := filepath.Join(dir, name+"-cert.pem")
		keyPath := filepath.Join(dir, name+"-key.pem")
		writeIntegrationPEM(t, certPath, "CERTIFICATE", leafDER)
		writeIntegrationPEM(t, keyPath, "PRIVATE KEY", leafKeyDER)
		return integrationTLSFiles{certificate: certPath, privateKey: keyPath, ca: caPath}
	}

	controlPlaneFiles := issue("control-plane", 2, "")
	serverTLS, clientTLS, err := transporttls.Load(controlPlaneFiles.certificate, controlPlaneFiles.privateKey, controlPlaneFiles.ca)
	if err != nil {
		t.Fatalf("load integration mutual TLS configuration: %v", err)
	}
	controlPlaneFiles.nodes = map[string]integrationTLSFiles{
		"worker-01": issue("worker-01", 3, "worker-01"),
		"worker-02": issue("worker-02", 4, "worker-02"),
	}
	return serverTLS, clientTLS, controlPlaneFiles
}

func writeIntegrationPEM(t *testing.T, path, kind string, value []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: value}), 0600); err != nil {
		t.Fatalf("write isolated integration TLS material: %v", err)
	}
}
