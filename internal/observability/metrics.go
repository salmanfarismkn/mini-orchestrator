package observability

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	reconcileRuns     atomic.Int64
	reconcileErrors   atomic.Int64
	workloadsFailed   atomic.Int64
	missingContainers atomic.Int64
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
	return func(w http.ResponseWriter, r *http.Request) {
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
	}
}
