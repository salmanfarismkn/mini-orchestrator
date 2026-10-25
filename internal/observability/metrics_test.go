package observability

import (
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
