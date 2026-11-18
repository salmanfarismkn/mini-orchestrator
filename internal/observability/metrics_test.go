package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestMetricsHandlerExposesCounterFormatAndValues(t *testing.T) {
	metrics := &Metrics{}
	metrics.ReconcileSucceeded()
	metrics.ReconcileSucceeded()
	metrics.ReconcileFailed()
	metrics.WorkloadFailed()
	metrics.ContainerMissing()

	response := httptest.NewRecorder()
	metrics.Handler()(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got !=
		"text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("content type = %q; want Prometheus text exposition format", got)
	}

	expected := map[string]int{
		"orchestrator_reconcile_runs_total":     2,
		"orchestrator_reconcile_errors_total":   1,
		"orchestrator_workloads_failed_total":   1,
		"orchestrator_missing_containers_total": 1,
	}
	values := make(map[string]int)
	for _, line := range strings.Split(response.Body.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if len(fields) != 2 {
			t.Fatalf("invalid metric sample line %q", line)
		}
		if _, ok := expected[fields[0]]; !ok {
			t.Errorf("unexpected metric name %q", fields[0])
			continue
		}
		value, err := strconv.Atoi(fields[1])
		if err != nil {
			t.Errorf("metric %q has invalid integer value %q", fields[0], fields[1])
			continue
		}
		values[fields[0]] = value
	}
	for name, want := range expected {
		if got, ok := values[name]; !ok || got != want {
			t.Errorf("metric %s = %d (present=%v); want %d", name, got, ok, want)
		}
	}
}

func TestMetricsHandlerExportsBoundedOperationalLabels(t *testing.T) {
	metrics := &Metrics{}
	metrics.SetOperationalSnapshotProvider(func(context.Context) (OperationalSnapshot, error) {
		return OperationalSnapshot{
			ActiveDeployments:      2,
			PendingDeployments:     1,
			ProgressingDeployments: 1,
			AvailableDeployments:   3,
			FailedDeployments:      1,
			DesiredReplicas:        9,
			RunningReplicas:        7,
			FailedWorkloads:        2,
			ReadyNodes:             4,
			UnhealthyNodes:         1,
			DrainingNodes:          2,
			ReservedCPUMillis:      2400,
			ReservedMemoryMB:       4096,
		}, nil
	})
	response := httptest.NewRecorder()
	metrics.Handler()(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, sample := range []string{
		"orchestrator_active_deployments 2",
		"orchestrator_deployments{status=\"PENDING\"} 1",
		"orchestrator_deployments{status=\"PROGRESSING\"} 1",
		"orchestrator_deployments{status=\"AVAILABLE\"} 3",
		"orchestrator_deployments{status=\"FAILED\"} 1",
		"orchestrator_deployment_replicas{state=\"desired\"} 9",
		"orchestrator_deployment_replicas{state=\"running\"} 7",
		"orchestrator_nodes{status=\"DRAINING\"} 2",
		"orchestrator_node_resource_allocated_cpu_millis 2400",
		"orchestrator_node_resource_allocated_memory_mb 4096",
	} {
		if !strings.Contains(body, sample) {
			t.Errorf("metrics output missing %q", sample)
		}
	}
	for _, forbidden := range []string{"deployment_id", "service_id", "workload_id", "tenant_id"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("metrics output contains high-cardinality label or identifier %q", forbidden)
		}
	}
}

func TestMetricsHandlerReportsSnapshotFailure(t *testing.T) {
	metrics := &Metrics{}
	metrics.SetOperationalSnapshotProvider(func(context.Context) (OperationalSnapshot, error) {
		return OperationalSnapshot{}, errors.New("database unavailable")
	})
	response := httptest.NewRecorder()
	metrics.Handler()(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusServiceUnavailable)
	}
	if strings.Contains(response.Body.String(), "database unavailable") {
		t.Fatal("metrics endpoint leaked internal provider error")
	}
}
