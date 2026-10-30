package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mini-orchestrator/internal/model"
	orchestratorRuntime "mini-orchestrator/internal/runtime"
)

const processIntegrationOptIn = "MINI_ORCHESTRATOR_PROCESS_INTEGRATION"

type managedProcess struct {
	command *exec.Cmd
	done    chan error
	output  *synchronizedBuffer
	stopped bool
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(value)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func processExitError(process *managedProcess) (error, bool) {
	select {
	case err := <-process.done:
		process.done <- err
		return err, true
	default:
		return nil, false
	}
}

func startManagedProcess(command *exec.Cmd) (*managedProcess, error) {
	if err := command.Start(); err != nil {
		return nil, err
	}
	process := &managedProcess{
		command: command,
		done:    make(chan error, 1),
		output:  command.Stdout.(*synchronizedBuffer),
	}
	go func() {
		process.done <- command.Wait()
	}()
	return process, nil
}

func (p *managedProcess) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	if _, exited := processExitError(p); exited {
		p.stopped = true
		return
	}
	if err := p.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		if _, exited := processExitError(p); !exited {
			t.Errorf("stop test process: %v", err)
		}
	}
	select {
	case <-p.done:
		p.stopped = true
	case <-time.After(5 * time.Second):
		t.Error("test process did not exit after kill")
	}
	if t.Failed() {
		t.Logf("test process output:\n%s", p.output.String())
	}
}

func TestRealAgentsProcessIntegration(t *testing.T) {
	if os.Getenv(processIntegrationOptIn) != "1" {
		t.Skipf("set %s=1 to start real control-plane and node-agent processes", processIntegrationOptIn)
	}
	if os.Getenv(integrationOptInEnv) != "1" {
		t.Skipf("set %s=1 to enable the isolated PostgreSQL environment", integrationOptInEnv)
	}
	if !portAvailable("127.0.0.1:8080") {
		t.Skip("control-plane default port :8080 is already in use")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newMultiNodeEnvironment(t)
	serviceID := integrationID(t, "real-process-service")
	agentIDs := []string{"worker-01", "worker-02"}
	env.cleanupTestData(t, serviceID, agentIDs...)
	for _, fakeNode := range env.nodes {
		if err := env.store.SetNodeStatus(ctx, fakeNode.ID, model.NodeDraining); err != nil {
			t.Fatalf("drain in-process test node %q: %v", fakeNode.ID, err)
		}
	}

	moduleRoot := integrationModuleRoot(t)
	tempDir := t.TempDir()
	controlPlaneBinary := filepath.Join(tempDir, "control-plane"+executableSuffix())
	nodeAgentBinary := filepath.Join(tempDir, "node-agent"+executableSuffix())
	buildGoBinary(t, ctx, moduleRoot, controlPlaneBinary, "./cmd/control-plane")
	buildGoBinary(t, ctx, moduleRoot, nodeAgentBinary, "./cmd/node-agent")

	controlPlaneProcess, err := startManagedProcess(newProcessCommand(
		tempDir,
		controlPlaneBinary,
		map[string]string{
			"DATABASE_URL":               env.dsn,
			"ORCHESTRATOR_NODE_TOKEN":    env.token,
			"ORCHESTRATOR_TLS_CERT_FILE": env.tlsFiles.certificate,
			"ORCHESTRATOR_TLS_KEY_FILE":  env.tlsFiles.privateKey,
			"ORCHESTRATOR_TLS_CA_FILE":   env.tlsFiles.ca,
		},
	))
	if err != nil {
		t.Fatalf("start configured control-plane process: %v", err)
	}
	t.Cleanup(func() { controlPlaneProcess.stop(t) })

	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: env.clientTLS},
		Timeout:   3 * time.Second,
	}
	controlPlaneURL := "https://127.0.0.1:8080"
	pollUntil(t, ctx, 15*time.Second, func() (bool, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, controlPlaneURL+"/readyz", nil)
		if err != nil {
			return false, err
		}
		response, err := client.Do(request)
		if err != nil {
			return false, nil
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK, nil
	})

	dockerRuntime, err := orchestratorRuntime.NewDockerRuntime()
	if err != nil {
		t.Fatalf("create Docker cleanup client: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		workloads, err := env.store.ListWorkloadsByService(cleanupCtx, serviceID)
		if err != nil {
			t.Errorf("list test workloads for container cleanup: %v", err)
			return
		}
		names := make(map[string]struct{}, len(workloads))
		for _, workload := range workloads {
			names["workload-"+workload.ID] = struct{}{}
		}
		containers, err := dockerRuntime.List(cleanupCtx)
		if err != nil {
			t.Errorf("list orchestrator containers for test cleanup: %v", err)
			return
		}
		for _, container := range containers {
			if _, owned := names[container.Name]; !owned {
				continue
			}
			if err := dockerRuntime.Remove(cleanupCtx, container.ID); err != nil &&
				!errors.Is(err, orchestratorRuntime.ErrContainerNotFound) {
				t.Errorf("remove test-created container %q: %v", container.ID, err)
			}
		}
	})

	agents := make([]*managedProcess, 0, 2)
	if agentIDs[0] == agentIDs[1] {
		t.Fatal("integration agent IDs must be distinct")
	}
	for _, agentID := range agentIDs {
		port := availablePort(t)
		address := net.JoinHostPort("127.0.0.1", port)
		command := newProcessCommand(
			tempDir,
			nodeAgentBinary,
			map[string]string{
				"ORCHESTRATOR_NODE_ID":           agentID,
				"ORCHESTRATOR_LISTEN_ADDR":       address,
				"ORCHESTRATOR_NODE_ADDRESS":      address,
				"ORCHESTRATOR_NODE_CPU_MILLIS":   "1000",
				"ORCHESTRATOR_NODE_MEMORY_MB":    "512",
				"ORCHESTRATOR_CONTROL_PLANE_URL": controlPlaneURL,
				"ORCHESTRATOR_NODE_TOKEN":        env.token,
				"ORCHESTRATOR_TLS_CERT_FILE":     env.tlsFiles.certificate,
				"ORCHESTRATOR_TLS_KEY_FILE":      env.tlsFiles.privateKey,
				"ORCHESTRATOR_TLS_CA_FILE":       env.tlsFiles.ca,
			},
		)
		process, err := startManagedProcess(command)
		if err != nil {
			t.Fatalf("start real agent %q: %v", agentID, err)
		}
		agents = append(agents, process)
		agentProcess := process
		t.Cleanup(func() { agentProcess.stop(t) })

		pollUntil(t, ctx, 15*time.Second, func() (bool, error) {
			if processErr, exited := processExitError(process); exited {
				return false, fmt.Errorf("real agent %q exited during startup: %v", agentID, processErr)
			}
			registered, err := env.store.GetNode(ctx, agentID)
			if err != nil {
				return false, nil
			}
			if registered.Status != model.NodeReady ||
				registered.Address != address ||
				registered.CPUCapacity != 1000 ||
				registered.MemoryCapacity != 512 {
				return false, nil
			}
			_, err = env.nodeClient().ListContainers(ctx, address)
			return err == nil, nil
		})
	}

	for _, agentID := range agentIDs {
		nodeBefore, err := env.store.GetNode(ctx, agentID)
		if err != nil {
			t.Fatalf("read registered node %q: %v", agentID, err)
		}
		pollUntil(t, ctx, 8*time.Second, func() (bool, error) {
			nodeAfter, err := env.store.GetNode(ctx, agentID)
			if err != nil {
				return false, err
			}
			return nodeAfter.LastHeartbeat.After(nodeBefore.LastHeartbeat), nil
		})
	}

	createBody, err := json.Marshal(map[string]any{
		"id":                 serviceID,
		"name":               serviceID,
		"image":              "nginx:alpine",
		"replicas":           2,
		"cpu_request_millis": 100,
		"memory_request_mb":  128,
		"deployment_version": 1,
		"max_surge":          1,
		"max_unavailable":    0,
	})
	if err != nil {
		t.Fatalf("encode service request: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, controlPlaneURL+"/services", bytes.NewReader(createBody))
	if err != nil {
		t.Fatalf("create service request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+env.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("create service through control-plane process: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create service status = %d; want %d", response.StatusCode, http.StatusCreated)
	}

	pollUntil(t, ctx, 35*time.Second, func() (bool, error) {
		workloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
		if err != nil {
			return false, err
		}
		if countRunning(workloads) != 2 {
			for _, process := range agents {
				if processErr, exited := processExitError(process); exited {
					return false, fmt.Errorf("node-agent process exited: %v", processErr)
				}
			}
		}
		return countRunning(workloads) == 2, nil
	})
	workloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list real-agent workloads: %v", err)
	}
	assertProcessAgentPlacement(t, ctx, env, workloads, agentIDs)
	existingWorkloadIDs := make(map[string]struct{}, len(workloads))
	for _, workload := range workloads {
		existingWorkloadIDs[workload.ID] = struct{}{}
	}
	missingWorkload := workloads[0]
	if missingWorkload.ContainerID == nil {
		t.Fatal("running workload has no Docker container ID")
	}
	missingContainerID := *missingWorkload.ContainerID
	if err := dockerRuntime.Remove(ctx, missingContainerID); err != nil {
		t.Fatalf("remove test-created container before recovery: %v", err)
	}
	var replacementWorkload model.Workload
	pollUntil(t, ctx, 40*time.Second, func() (bool, error) {
		current, err := env.store.ListWorkloadsByService(ctx, serviceID)
		if err != nil {
			return false, err
		}
		originalFailed := false
		for _, workload := range current {
			if workload.ID == missingWorkload.ID &&
				workload.ActualState == model.WorkloadFailed &&
				workload.ContainerID != nil &&
				*workload.ContainerID == missingContainerID {
				originalFailed = true
				continue
			}
			if _, existedBeforeFailure := existingWorkloadIDs[workload.ID]; existedBeforeFailure ||
				workload.ActualState != model.WorkloadRunning ||
				workload.ContainerID == nil ||
				*workload.ContainerID == "" {
				continue
			}
			container, inspectErr := dockerRuntime.Inspect(ctx, *workload.ContainerID)
			if inspectErr != nil {
				if errors.Is(inspectErr, orchestratorRuntime.ErrContainerNotFound) {
					continue
				}
				return false, inspectErr
			}
			if container.Status == "running" {
				replacementWorkload = workload
				break
			}
		}
		return originalFailed &&
			replacementWorkload.ID != "" &&
			replacementWorkload.ID != missingWorkload.ID &&
			replacementWorkload.ContainerID != nil &&
			*replacementWorkload.ContainerID != "" &&
			countRunning(current) == 2, nil
	})
	workloads, err = env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list workloads after missing-container recovery: %v", err)
	}
	replacementWorkload, err = env.store.GetWorkload(ctx, replacementWorkload.ID)
	if err != nil {
		t.Fatalf("read replacement workload: %v", err)
	}
	if replacementWorkload.ID == missingWorkload.ID ||
		replacementWorkload.ServiceID != serviceID ||
		replacementWorkload.ActualState != model.WorkloadRunning ||
		replacementWorkload.ContainerID == nil ||
		*replacementWorkload.ContainerID == "" {
		t.Fatalf("replacement workload = %+v; want a distinct running workload with a container ID",
			replacementWorkload)
	}
	replacementContainer, err := dockerRuntime.Inspect(ctx, *replacementWorkload.ContainerID)
	if err != nil {
		t.Fatalf("inspect replacement Docker container %q: %v",
			*replacementWorkload.ContainerID, err)
	}
	if replacementContainer.Status != "running" {
		t.Fatalf("replacement Docker container %q status = %q; want running",
			*replacementWorkload.ContainerID, replacementContainer.Status)
	}
	assertProcessAgentPlacement(t, ctx, env, workloads, agentIDs)
	assertAllocationsMatchActiveWorkloads(t, ctx, env, serviceID)

	failedNodeID := ""
	for _, workload := range workloads {
		if workload.ActualState == model.WorkloadRunning && workload.NodeID != nil {
			failedNodeID = *workload.NodeID
			break
		}
	}
	if failedNodeID == "" {
		t.Fatal("no running workload available for heartbeat failure scenario")
	}
	failedIndex := 0
	if agentIDs[1] == failedNodeID {
		failedIndex = 1
	}
	for _, workload := range workloads {
		if workload.ActualState != model.WorkloadRunning ||
			workload.NodeID == nil || *workload.NodeID != failedNodeID ||
			workload.ContainerID == nil {
			continue
		}
		if err := dockerRuntime.Remove(ctx, *workload.ContainerID); err != nil {
			t.Fatalf("remove test-owned workload before simulated node loss: %v", err)
		}
	}
	agents[failedIndex].stop(t)
	pollUntil(t, ctx, 35*time.Second, func() (bool, error) {
		failedNode, err := env.store.GetNode(ctx, failedNodeID)
		if err != nil {
			return false, err
		}
		if failedNode.Status != model.NodeUnhealthy {
			return false, nil
		}
		current, err := env.store.ListWorkloadsByService(ctx, serviceID)
		if err != nil {
			return false, err
		}
		return countRunningOn(current, agentIDs[1-failedIndex]) == 2, nil
	})
	workloads, err = env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list workloads after heartbeat failure recovery: %v", err)
	}
	for _, workload := range workloads {
		if workload.NodeID != nil && *workload.NodeID == failedNodeID &&
			(workload.ActualState == model.WorkloadPending ||
				workload.ActualState == model.WorkloadScheduled ||
				workload.ActualState == model.WorkloadRunning) {
			t.Errorf("active workload %q remains assigned to unhealthy node %q",
				workload.ID, failedNodeID)
		}
	}
	assertAllocationsMatchActiveWorkloads(t, ctx, env, serviceID)

	deleteRequest, err := http.NewRequestWithContext(ctx, http.MethodDelete, controlPlaneURL+"/services/"+serviceID, nil)
	if err != nil {
		t.Fatalf("create service delete request: %v", err)
	}
	deleteRequest.Header.Set("Authorization", "Bearer "+env.token)
	deleteResponse, err := client.Do(deleteRequest)
	if err != nil {
		t.Fatalf("delete service through control-plane process: %v", err)
	}
	deleteResponse.Body.Close()
	if deleteResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("delete service status = %d; want %d", deleteResponse.StatusCode, http.StatusAccepted)
	}
	pollUntil(t, ctx, 35*time.Second, func() (bool, error) {
		service, err := env.store.GetService(ctx, serviceID)
		if err != nil {
			return false, err
		}
		return service.Status == model.ServiceDeleted, nil
	})
	deletedWorkloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list workload history after service deletion: %v", err)
	}
	for _, workload := range deletedWorkloads {
		if workload.ActualState != model.WorkloadStopped &&
			workload.ActualState != model.WorkloadFailed {
			t.Errorf("deleted service retains workload %q in state %q",
				workload.ID, workload.ActualState)
		}
	}
	assertAllocationsMatchActiveWorkloads(t, ctx, env, serviceID)
	for _, agentID := range agentIDs {
		nodeRecord, err := env.store.GetNode(ctx, agentID)
		if err != nil {
			t.Fatalf("get node allocation after deletion: %v", err)
		}
		if nodeRecord.CPUAllocated != 0 || nodeRecord.MemoryAllocated != 0 {
			t.Errorf("node %q retains allocation after deletion: cpu=%d memory=%d",
				nodeRecord.ID, nodeRecord.CPUAllocated, nodeRecord.MemoryAllocated)
		}
	}
}

func assertProcessAgentPlacement(
	t *testing.T,
	ctx context.Context,
	env *multiNodeEnvironment,
	workloads []model.Workload,
	agentIDs []string,
) {
	t.Helper()
	seen := make(map[string]int, len(agentIDs))
	for _, workload := range workloads {
		if workload.ActualState != model.WorkloadRunning {
			continue
		}
		if workload.NodeID == nil {
			t.Fatalf("running workload %q has no node assignment", workload.ID)
		}
		if !contains(agentIDs, *workload.NodeID) {
			t.Fatalf("workload %q assigned to non-process node %q", workload.ID, *workload.NodeID)
		}
		seen[*workload.NodeID]++
	}
	for _, agentID := range agentIDs {
		nodeRecord, err := env.store.GetNode(ctx, agentID)
		if err != nil {
			t.Fatalf("get process node %q: %v", agentID, err)
		}
		if nodeRecord.CPUAllocated != seen[agentID]*100 ||
			nodeRecord.MemoryAllocated != seen[agentID]*128 {
			t.Errorf("node %q allocation = CPU %d memory %d, expected workload totals %d/%d",
				agentID, nodeRecord.CPUAllocated, nodeRecord.MemoryAllocated,
				seen[agentID]*100, seen[agentID]*128)
		}
	}
}

func countRunning(workloads []model.Workload) int {
	count := 0
	for _, workload := range workloads {
		if workload.ActualState == model.WorkloadRunning {
			count++
		}
	}
	return count
}

func countRunningOn(workloads []model.Workload, nodeID string) int {
	count := 0
	for _, workload := range workloads {
		if workload.ActualState == model.WorkloadRunning &&
			workload.NodeID != nil && *workload.NodeID == nodeID {
			count++
		}
	}
	return count
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func buildGoBinary(t *testing.T, ctx context.Context, moduleRoot, output, packagePath string) {
	t.Helper()
	command := exec.CommandContext(ctx, "go", "build", "-o", output, packagePath)
	command.Dir = moduleRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", packagePath, err, strings.TrimSpace(string(output)))
	}
}

func newProcessCommand(
	workDir string,
	binary string,
	variables map[string]string,
) *exec.Cmd {
	command := exec.Command(binary)
	command.Dir = workDir
	output := &synchronizedBuffer{}
	command.Stdout = output
	command.Stderr = output

	environment := make([]string, 0, len(os.Environ())+len(variables))
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if _, replace := variables[name]; !replace {
			environment = append(environment, value)
		}
	}
	for name, value := range variables {
		environment = append(environment, name+"="+value)
	}
	command.Env = environment
	return command
}

func integrationModuleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func executableSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func portAvailable(address string) bool {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return false
	}
	_ = listener.Close()
	return true
}

func availablePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a test agent port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release test agent port: %v", err)
	}
	return fmt.Sprint(port)
}
