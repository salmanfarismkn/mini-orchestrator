package metrics

import "context"

type WorkloadMetrics struct {
	CPUUsageMillis int
}

type Provider interface {
	GetWorkloadMetrics(
		ctx context.Context,
		workloadID string,
	) (WorkloadMetrics, error)
}