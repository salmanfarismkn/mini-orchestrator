package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"mini-orchestrator/internal/model"
)

type DeploymentOperationalStatus struct {
	ServiceID                   string
	DeploymentVersion           int
	DeploymentStatus            model.DeploymentStatus
	DesiredReplicas             int
	CurrentVersionWorkloadCount int64
	RunningReplicaCount         int64
	FailedReplicaCount          int64
	MaxSurge                    int
	MaxUnavailable              int
	DeploymentStartedAt         *time.Time
	ServiceCreatedAt            time.Time
	DeploymentFailureReason     model.DeploymentFailureReason
	ProgressPercent             float64
}

const deploymentOperationalStatusQuery = `
	SELECT
		s.id,
		s.deployment_version,
		s.deployment_status,
		s.desired_replicas,
		COUNT(w.id) FILTER (
			WHERE w.deployment_version = s.deployment_version
		),
		COUNT(w.id) FILTER (
			WHERE w.deployment_version = s.deployment_version
			  AND w.actual_state = $2
		),
		COUNT(w.id) FILTER (
			WHERE w.deployment_version = s.deployment_version
			  AND w.actual_state = $3
		),
		s.max_surge,
		s.max_unavailable,
		s.deployment_started_at,
		s.created_at,
		s.deployment_failure_reason
	FROM services s
	LEFT JOIN workloads w ON w.service_id = s.id
	WHERE s.id = $1
	GROUP BY s.id
`

func (p *Postgres) GetDeploymentOperationalStatus(
	ctx context.Context,
	serviceID string,
) (DeploymentOperationalStatus, error) {
	var status DeploymentOperationalStatus
	err := p.db.QueryRowContext(ctx, deploymentOperationalStatusQuery,
		serviceID, model.WorkloadRunning, model.WorkloadFailed).Scan(
		&status.ServiceID,
		&status.DeploymentVersion,
		&status.DeploymentStatus,
		&status.DesiredReplicas,
		&status.CurrentVersionWorkloadCount,
		&status.RunningReplicaCount,
		&status.FailedReplicaCount,
		&status.MaxSurge,
		&status.MaxUnavailable,
		&status.DeploymentStartedAt,
		&status.ServiceCreatedAt,
		&status.DeploymentFailureReason,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeploymentOperationalStatus{}, fmt.Errorf("%w: %q", ErrServiceNotFound, serviceID)
		}
		return DeploymentOperationalStatus{}, fmt.Errorf("get deployment operational status for %q: %w", serviceID, err)
	}

	if status.DesiredReplicas == 0 {
		status.ProgressPercent = 100
	} else {
		status.ProgressPercent = float64(status.RunningReplicaCount) * 100 / float64(status.DesiredReplicas)
		if status.ProgressPercent > 100 {
			status.ProgressPercent = 100
		}
	}
	return status, nil
}

type OperationalMetrics struct {
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

const operationalMetricsQuery = `
	WITH per_service AS (
		SELECT
			s.id,
			s.status,
			s.deployment_status,
			s.desired_replicas,
			COUNT(w.id) FILTER (
				WHERE w.deployment_version = s.deployment_version
				  AND w.actual_state = $1
			) AS running_replicas,
			COUNT(w.id) FILTER (
				WHERE w.actual_state = $2
			) AS failed_workloads
		FROM services s
		LEFT JOIN workloads w ON w.service_id = s.id
		GROUP BY s.id
	),
	service_totals AS (
		SELECT
			COUNT(*) FILTER (
				WHERE status = $3 AND deployment_status IN ($4, $5)
			) AS active_deployments,
			COUNT(*) FILTER (WHERE status = $3 AND deployment_status = $4) AS pending_deployments,
			COUNT(*) FILTER (WHERE status = $3 AND deployment_status = $5) AS progressing_deployments,
			COUNT(*) FILTER (WHERE status = $3 AND deployment_status = $6) AS available_deployments,
			COUNT(*) FILTER (WHERE status = $3 AND deployment_status = $7) AS failed_deployments,
			COALESCE(SUM(desired_replicas) FILTER (WHERE status = $3), 0) AS desired_replicas,
			COALESCE(SUM(running_replicas) FILTER (WHERE status = $3), 0) AS running_replicas,
			COALESCE(SUM(failed_workloads), 0) AS failed_workloads
		FROM per_service
	),
	node_totals AS (
		SELECT
			COUNT(*) FILTER (WHERE status = $8) AS ready_nodes,
			COUNT(*) FILTER (WHERE status = $9) AS unhealthy_nodes,
			COUNT(*) FILTER (WHERE status = $10) AS draining_nodes,
			COALESCE(SUM(cpu_allocated_millis), 0) AS reserved_cpu_millis,
			COALESCE(SUM(memory_allocated_mb), 0) AS reserved_memory_mb
		FROM nodes
	)
	SELECT
		service_totals.active_deployments,
		service_totals.pending_deployments,
		service_totals.progressing_deployments,
		service_totals.available_deployments,
		service_totals.failed_deployments,
		service_totals.desired_replicas,
		service_totals.running_replicas,
		service_totals.failed_workloads,
		node_totals.ready_nodes,
		node_totals.unhealthy_nodes,
		node_totals.draining_nodes,
		node_totals.reserved_cpu_millis,
		node_totals.reserved_memory_mb
	FROM service_totals CROSS JOIN node_totals
`

func (p *Postgres) GetOperationalMetrics(ctx context.Context) (OperationalMetrics, error) {
	var metrics OperationalMetrics
	err := p.db.QueryRowContext(ctx, operationalMetricsQuery, model.WorkloadRunning, model.WorkloadFailed, model.ServiceActive,
		model.DeploymentPending, model.DeploymentProgressing, model.DeploymentAvailable, model.DeploymentFailed,
		model.NodeReady, model.NodeUnhealthy, model.NodeDraining).Scan(
		&metrics.ActiveDeployments,
		&metrics.PendingDeployments,
		&metrics.ProgressingDeployments,
		&metrics.AvailableDeployments,
		&metrics.FailedDeployments,
		&metrics.DesiredReplicas,
		&metrics.RunningReplicas,
		&metrics.FailedWorkloads,
		&metrics.ReadyNodes,
		&metrics.UnhealthyNodes,
		&metrics.DrainingNodes,
		&metrics.ReservedCPUMillis,
		&metrics.ReservedMemoryMB,
	)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("get operational metrics: %w", err)
	}
	return metrics, nil
}
