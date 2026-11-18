package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"mini-orchestrator/internal/model"
)

func TestDeploymentOperationalStatusIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := integrationContext(t)
	defer cancel()

	t.Run("zero desired replicas", func(t *testing.T) {
		serviceID := createDeploymentStatusFixture(t, ctx, p, "zero", 0, 3, model.DeploymentAvailable)
		status, err := p.GetDeploymentOperationalStatus(ctx, serviceID)
		if err != nil {
			t.Fatalf("get deployment status: %v", err)
		}
		if status.DesiredReplicas != 0 || status.CurrentVersionWorkloadCount != 0 ||
			status.RunningReplicaCount != 0 || status.FailedReplicaCount != 0 ||
			status.ProgressPercent != 100 {
			t.Fatalf("zero-replica deployment status = %+v", status)
		}
	})

	t.Run("current version counts exclude older workloads", func(t *testing.T) {
		serviceID := createDeploymentStatusFixture(t, ctx, p, "versions", 2, 4, model.DeploymentProgressing)
		createDeploymentStatusWorkload(t, ctx, p, serviceID, "old", 3, model.WorkloadRunning)
		createDeploymentStatusWorkload(t, ctx, p, serviceID, "running", 4, model.WorkloadRunning)
		createDeploymentStatusWorkload(t, ctx, p, serviceID, "failed", 4, model.WorkloadFailed)

		status, err := p.GetDeploymentOperationalStatus(ctx, serviceID)
		if err != nil {
			t.Fatalf("get deployment status: %v", err)
		}
		if status.CurrentVersionWorkloadCount != 2 || status.RunningReplicaCount != 1 ||
			status.FailedReplicaCount != 1 || status.ProgressPercent != 50 {
			t.Fatalf("version-scoped deployment status = %+v", status)
		}
		logOperationalQueryPlan(t, ctx, p, deploymentOperationalStatusQuery,
			serviceID, model.WorkloadRunning, model.WorkloadFailed)
	})

	t.Run("all desired replicas running and failed reason is persisted", func(t *testing.T) {
		serviceID := createDeploymentStatusFixture(t, ctx, p, "running", 2, 5, model.DeploymentProgressing)
		createDeploymentStatusWorkload(t, ctx, p, serviceID, "running-1", 5, model.WorkloadRunning)
		createDeploymentStatusWorkload(t, ctx, p, serviceID, "running-2", 5, model.WorkloadRunning)

		status, err := p.GetDeploymentOperationalStatus(ctx, serviceID)
		if err != nil {
			t.Fatalf("get deployment status: %v", err)
		}
		if status.RunningReplicaCount != 2 || status.ProgressPercent != 100 {
			t.Fatalf("running deployment status = %+v", status)
		}
		if err := p.UpdateDeploymentFailure(ctx, serviceID, model.DeploymentFailureTimeout); err != nil {
			t.Fatalf("persist deployment failure reason: %v", err)
		}
		status, err = p.GetDeploymentOperationalStatus(ctx, serviceID)
		if err != nil {
			t.Fatalf("get failed deployment status: %v", err)
		}
		if status.DeploymentStatus != model.DeploymentFailed ||
			status.DeploymentFailureReason != model.DeploymentFailureTimeout {
			t.Fatalf("failed deployment status = %+v", status)
		}
		service, err := p.GetService(ctx, serviceID)
		if err != nil {
			t.Fatalf("read failed service before next deployment: %v", err)
		}
		service.Image = "integration/test:v2"
		if err := p.UpdateService(ctx, service); err != nil {
			t.Fatalf("start next deployment: %v", err)
		}
		service, err = p.GetService(ctx, serviceID)
		if err != nil {
			t.Fatalf("read next deployment: %v", err)
		}
		if service.DeploymentVersion != 6 || service.DeploymentStatus != model.DeploymentProgressing ||
			service.DeploymentFailureReason != "" {
			t.Fatalf("new deployment retained old failure details: %+v", service)
		}
	})

	t.Run("unknown service", func(t *testing.T) {
		_, err := p.GetDeploymentOperationalStatus(ctx, integrationStoreID(t, "deployment-unknown"))
		if !errors.Is(err, ErrServiceNotFound) {
			t.Fatalf("error = %v; want ErrServiceNotFound", err)
		}
	})

	t.Run("read-only query", func(t *testing.T) {
		serviceID := createDeploymentStatusFixture(t, ctx, p, "read-only", 1, 2, model.DeploymentProgressing)
		workloadID := createDeploymentStatusWorkload(t, ctx, p, serviceID, "pending", 2, model.WorkloadPending)
		beforeService, err := p.GetService(ctx, serviceID)
		if err != nil {
			t.Fatalf("read service before status query: %v", err)
		}
		beforeWorkload, err := p.GetWorkload(ctx, workloadID)
		if err != nil {
			t.Fatalf("read workload before status query: %v", err)
		}
		if _, err := p.GetDeploymentOperationalStatus(ctx, serviceID); err != nil {
			t.Fatalf("get deployment status: %v", err)
		}
		afterService, err := p.GetService(ctx, serviceID)
		if err != nil {
			t.Fatalf("read service after status query: %v", err)
		}
		afterWorkload, err := p.GetWorkload(ctx, workloadID)
		if err != nil {
			t.Fatalf("read workload after status query: %v", err)
		}
		if !beforeService.UpdatedAt.Equal(afterService.UpdatedAt) ||
			beforeWorkload.ActualState != afterWorkload.ActualState ||
			beforeWorkload.DesiredState != afterWorkload.DesiredState ||
			beforeWorkload.ResourceReserved != afterWorkload.ResourceReserved {
			t.Fatalf("status query mutated persisted state: service before=%+v after=%+v; workload before=%+v after=%+v",
				beforeService, afterService, beforeWorkload, afterWorkload)
		}
	})
}

func TestDeploymentOperationalStatusSnapshotOverlapIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := integrationContext(t)
	defer cancel()

	serviceID := createDeploymentStatusFixture(t, ctx, p, "snapshot", 1, 1, model.DeploymentProgressing)
	workloadID := createDeploymentStatusWorkload(t, ctx, p, serviceID, "pending", 1, model.WorkloadPending)
	start := make(chan struct{})
	var wg sync.WaitGroup
	readErr := make(chan error, 20)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10; i++ {
			status, err := p.GetDeploymentOperationalStatus(ctx, serviceID)
			if err != nil {
				readErr <- err
				return
			}
			if !((status.DeploymentStatus == model.DeploymentProgressing && status.RunningReplicaCount == 0) ||
				(status.DeploymentStatus == model.DeploymentAvailable && status.RunningReplicaCount == 1)) {
				readErr <- fmt.Errorf("inconsistent status snapshot: %+v", status)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			readErr <- err
			return
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workloads SET actual_state = $1 WHERE id = $2`,
			model.WorkloadRunning, workloadID); err != nil {
			_ = tx.Rollback()
			readErr <- err
			return
		}
		if _, err := tx.ExecContext(ctx, `UPDATE services SET deployment_status = $1 WHERE id = $2`,
			model.DeploymentAvailable, serviceID); err != nil {
			_ = tx.Rollback()
			readErr <- err
			return
		}
		if err := tx.Commit(); err != nil {
			readErr <- err
		}
	}()
	close(start)
	wg.Wait()
	close(readErr)
	for err := range readErr {
		t.Fatal(err)
	}
}

func TestOperationalMetricsIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := integrationContext(t)
	defer cancel()

	before, err := p.GetOperationalMetrics(ctx)
	if err != nil {
		t.Fatalf("read operational metrics before fixtures: %v", err)
	}
	serviceID := createDeploymentStatusFixture(t, ctx, p, "metrics", 2, 1, model.DeploymentProgressing)
	createDeploymentStatusWorkload(t, ctx, p, serviceID, "running", 1, model.WorkloadRunning)
	createDeploymentStatusWorkload(t, ctx, p, serviceID, "failed", 1, model.WorkloadFailed)
	nodeID := integrationStoreID(t, "metrics-node")
	t.Cleanup(func() {
		_, _ = p.db.ExecContext(context.Background(), `DELETE FROM nodes WHERE id = $1`, nodeID)
	})
	if _, err := p.db.ExecContext(ctx, `
		INSERT INTO nodes (id, address, cpu_capacity_millis, memory_capacity_mb,
			cpu_allocated_millis, memory_allocated_mb, status)
		VALUES ($1, '127.0.0.1:1', 4000, 8192, 300, 512, $2)
	`, nodeID, model.NodeDraining); err != nil {
		t.Fatalf("create draining node for metrics: %v", err)
	}

	metrics, err := p.GetOperationalMetrics(ctx)
	if err != nil {
		t.Fatalf("get operational metrics: %v", err)
	}
	if metrics.ActiveDeployments-before.ActiveDeployments != 1 ||
		metrics.ProgressingDeployments-before.ProgressingDeployments != 1 ||
		metrics.DesiredReplicas-before.DesiredReplicas != 2 ||
		metrics.RunningReplicas-before.RunningReplicas != 1 ||
		metrics.FailedWorkloads-before.FailedWorkloads != 1 ||
		metrics.DrainingNodes-before.DrainingNodes != 1 ||
		metrics.ReservedCPUMillis-before.ReservedCPUMillis != 300 ||
		metrics.ReservedMemoryMB-before.ReservedMemoryMB != 512 {
		t.Fatalf("operational metrics deltas do not match fixtures: before=%+v after=%+v", before, metrics)
	}
	logOperationalQueryPlan(t, ctx, p, operationalMetricsQuery,
		model.WorkloadRunning, model.WorkloadFailed, model.ServiceActive,
		model.DeploymentPending, model.DeploymentProgressing, model.DeploymentAvailable, model.DeploymentFailed,
		model.NodeReady, model.NodeUnhealthy, model.NodeDraining)
}

func logOperationalQueryPlan(t *testing.T, ctx context.Context, p *Postgres, query string, args ...any) {
	t.Helper()
	rows, err := p.db.QueryContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) "+query, args...)
	if err != nil {
		t.Fatalf("explain operational query: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("read operational query plan: %v", err)
		}
		plan = append(plan, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate operational query plan: %v", err)
	}
	t.Logf("PostgreSQL operational query plan:\n%s", strings.Join(plan, "\n"))
}

func createDeploymentStatusFixture(
	t *testing.T,
	ctx context.Context,
	p *Postgres,
	suffix string,
	desiredReplicas int,
	version int,
	status model.DeploymentStatus,
) string {
	t.Helper()
	serviceID := integrationStoreID(t, "deployment-"+suffix)
	now := time.Now().UTC()
	if err := p.CreateService(ctx, model.Service{
		ID: serviceID, Name: serviceID, Image: "integration/test:latest",
		DesiredReplicas: desiredReplicas, CPURequestMillis: 100, MemoryRequestMB: 128,
		DeploymentVersion: version, MaxSurge: 1, DeploymentStatus: status,
		Status: model.ServiceActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create deployment status service: %v", err)
	}
	t.Cleanup(func() {
		_, _ = p.db.ExecContext(context.Background(), `DELETE FROM services WHERE id = $1`, serviceID)
	})
	return serviceID
}

func createDeploymentStatusWorkload(
	t *testing.T,
	ctx context.Context,
	p *Postgres,
	serviceID string,
	suffix string,
	version int,
	state model.WorkloadState,
) string {
	t.Helper()
	workloadID := integrationStoreID(t, "deployment-workload-"+suffix)
	now := time.Now().UTC()
	if err := p.CreateWorkload(ctx, model.Workload{
		ID: workloadID, ServiceID: serviceID, Image: "integration/test:latest",
		CPURequestMillis: 100, MemoryRequestMB: 128, DeploymentVersion: version,
		DesiredState: state, ActualState: state, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create deployment status workload: %v", err)
	}
	return workloadID
}
