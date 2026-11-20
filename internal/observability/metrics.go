package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
)

type OperationalSnapshot struct {
	ActiveDeployments      int64
	PendingDeployments     int64
	ProgressingDeployments int64
	AvailableDeployments   int64
	FailedDeployments      int64
	DesiredReplicas        int64
	RunningReplicas        int64
	FailedWorkloads        int64
	ReadyNodes             int64
	UnhealthyNodes         int64
	DrainingNodes          int64
	ReservedCPUMillis      int64
	ReservedMemoryMB       int64
}

type OperationalSnapshotProvider func(context.Context) (OperationalSnapshot, error)

type Metrics struct {
	reconcileRuns     atomic.Int64
	reconcileErrors   atomic.Int64
	workloadsFailed   atomic.Int64
	missingContainers atomic.Int64
	providerMu        sync.RWMutex
	provider          OperationalSnapshotProvider
}

func (m *Metrics) SetOperationalSnapshotProvider(provider OperationalSnapshotProvider) {
	if m == nil {
		return
	}
	m.providerMu.Lock()
	m.provider = provider
	m.providerMu.Unlock()
}

func (m *Metrics) ReconcileSucceeded() {
	m.reconcileRuns.Add(1)
}

func (m *Metrics) ReconcileFailed() {
	m.reconcileErrors.Add(1)
}

func (m *Metrics) WorkloadFailed() {
	m.workloadsFailed.Add(1)
}

func (m *Metrics) ContainerMissing() {
	m.missingContainers.Add(1)
}

func (m *Metrics) Handler() http.HandlerFunc {
	if m == nil {
		m = &Metrics{}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		m.providerMu.RLock()
		provider := m.provider
		m.providerMu.RUnlock()

		var snapshot OperationalSnapshot
		if provider != nil {
			var err error
			snapshot, err = provider(r.Context())
			if err != nil {
				slog.Error("failed to collect operational metrics", "error", err)
				http.Error(w, "operational metrics unavailable", http.StatusServiceUnavailable)
				return
			}
		}

		w.Header().Set(
			"Content-Type",
			"text/plain; version=0.0.4; charset=utf-8",
		)

		w.WriteHeader(http.StatusOK)

		_, _ = fmt.Fprintf(w,
			"# TYPE orchestrator_reconcile_runs_total counter\n"+
				"orchestrator_reconcile_runs_total %d\n",
			m.reconcileRuns.Load(),
		)

		_, _ = fmt.Fprintf(w,
			"# TYPE orchestrator_reconcile_errors_total counter\n"+
				"orchestrator_reconcile_errors_total %d\n",
			m.reconcileErrors.Load(),
		)

		_, _ = fmt.Fprintf(w,
			"# TYPE orchestrator_workloads_failed_total counter\n"+
				"orchestrator_workloads_failed_total %d\n",
			m.workloadsFailed.Load(),
		)

		_, _ = fmt.Fprintf(w,
			"# TYPE orchestrator_missing_containers_total counter\n"+
				"orchestrator_missing_containers_total %d\n",
			m.missingContainers.Load(),
		)

		if provider != nil {
			writeOperationalMetrics(w, snapshot)
		}
	}
}

func writeOperationalMetrics(w http.ResponseWriter, snapshot OperationalSnapshot) {
	_, _ = fmt.Fprintf(w,
		"# TYPE orchestrator_active_deployments gauge\n"+
			"orchestrator_active_deployments %d\n"+
			"# TYPE orchestrator_deployments gauge\n"+
			"orchestrator_deployments{status=\"PENDING\"} %d\n"+
			"orchestrator_deployments{status=\"PROGRESSING\"} %d\n"+
			"orchestrator_deployments{status=\"AVAILABLE\"} %d\n"+
			"orchestrator_deployments{status=\"FAILED\"} %d\n"+
			"# TYPE orchestrator_deployment_replicas gauge\n"+
			"orchestrator_deployment_replicas{state=\"desired\"} %d\n"+
			"orchestrator_deployment_replicas{state=\"running\"} %d\n"+
			"# TYPE orchestrator_workloads_failed gauge\n"+
			"orchestrator_workloads_failed %d\n"+
			"# TYPE orchestrator_nodes gauge\n"+
			"orchestrator_nodes{status=\"READY\"} %d\n"+
			"orchestrator_nodes{status=\"UNHEALTHY\"} %d\n"+
			"orchestrator_nodes{status=\"DRAINING\"} %d\n"+
			"# TYPE orchestrator_nodes_draining gauge\n"+
			"orchestrator_nodes_draining %d\n"+
			"# TYPE orchestrator_node_resource_allocated_cpu_millis gauge\n"+
			"orchestrator_node_resource_allocated_cpu_millis %d\n"+
			"# TYPE orchestrator_node_resource_allocated_memory_mb gauge\n"+
			"orchestrator_node_resource_allocated_memory_mb %d\n",
		snapshot.ActiveDeployments,
		snapshot.PendingDeployments,
		snapshot.ProgressingDeployments,
		snapshot.AvailableDeployments,
		snapshot.FailedDeployments,
		snapshot.DesiredReplicas,
		snapshot.RunningReplicas,
		snapshot.FailedWorkloads,
		snapshot.ReadyNodes,
		snapshot.UnhealthyNodes,
		snapshot.DrainingNodes,
		snapshot.DrainingNodes,
		snapshot.ReservedCPUMillis,
		snapshot.ReservedMemoryMB,
	)
}
