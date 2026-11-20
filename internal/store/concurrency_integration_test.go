package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"mini-orchestrator/internal/model"
)

const (
	storeIntegrationDatabaseEnv = "DATABASE_URL"
	storeIntegrationTimeout     = 30 * time.Second
)

func TestNodeDrainReservationOrderingIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), storeIntegrationTimeout)
	defer cancel()

	t.Run("reservation commits before drain", func(t *testing.T) {
		nodeID, workloadID := createReservationFixture(t, ctx, p, "before-drain")
		if err := p.ReserveNodeForWorkload(ctx, workloadID, nodeID, 300, 256); err != nil {
			t.Fatalf("reserve workload: %v", err)
		}
		if err := p.ReserveNodeForWorkload(ctx, workloadID, nodeID, 300, 256); err != nil {
			t.Fatalf("repeat reservation: %v", err)
		}

		if _, err := p.DrainNode(ctx, nodeID); err != nil {
			t.Fatalf("drain node: %v", err)
		}
		if _, err := p.DrainNode(ctx, nodeID); err != nil {
			t.Fatalf("repeat drain: %v", err)
		}

		node, err := p.GetNode(ctx, nodeID)
		if err != nil {
			t.Fatalf("get drained node: %v", err)
		}
		if node.Status != model.NodeDraining || node.CPUAllocated != 300 || node.MemoryAllocated != 256 {
			t.Fatalf("drained node = status %q, allocations (%d, %d); want DRAINING, (300, 256)",
				node.Status, node.CPUAllocated, node.MemoryAllocated)
		}

		workload, err := p.GetWorkload(ctx, workloadID)
		if err != nil {
			t.Fatalf("get reserved workload: %v", err)
		}
		if workload.NodeID == nil || *workload.NodeID != nodeID || !workload.ResourceReserved {
			t.Fatalf("reserved workload = node %v, resource_reserved=%v; want assigned and reserved",
				workload.NodeID, workload.ResourceReserved)
		}

		rejectedID := createPendingWorkload(t, ctx, p, "after-drain")
		err = p.ReserveNodeForWorkload(ctx, rejectedID, nodeID, 100, 100)
		if err == nil {
			t.Fatal("reserve workload on drained node succeeded")
		}
		rejected, err := p.GetWorkload(ctx, rejectedID)
		if err != nil {
			t.Fatalf("get rejected workload: %v", err)
		}
		if rejected.NodeID != nil || rejected.ResourceReserved {
			t.Fatalf("rejected workload changed: node %v, resource_reserved=%v", rejected.NodeID, rejected.ResourceReserved)
		}
	})

	t.Run("drain commits before reservation", func(t *testing.T) {
		nodeID, workloadID := createReservationFixture(t, ctx, p, "after-drain")
		if _, err := p.DrainNode(ctx, nodeID); err != nil {
			t.Fatalf("drain node: %v", err)
		}

		err := p.ReserveNodeForWorkload(ctx, workloadID, nodeID, 300, 256)
		if err == nil {
			t.Fatal("reserve workload on drained node succeeded")
		}
		node, err := p.GetNode(ctx, nodeID)
		if err != nil {
			t.Fatalf("get drained node: %v", err)
		}
		if node.CPUAllocated != 0 || node.MemoryAllocated != 0 {
			t.Fatalf("drained node allocations = (%d, %d); want zero", node.CPUAllocated, node.MemoryAllocated)
		}
		workload, err := p.GetWorkload(ctx, workloadID)
		if err != nil {
			t.Fatalf("get rejected workload: %v", err)
		}
		if workload.NodeID != nil || workload.ResourceReserved {
			t.Fatalf("rejected workload changed: node %v, resource_reserved=%v", workload.NodeID, workload.ResourceReserved)
		}
	})
}

func TestConcurrentNodeDrainAndReservationIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), storeIntegrationTimeout)
	defer cancel()

	for i := 0; i < 10; i++ {
		nodeID, workloadID := createReservationFixture(t, ctx, p, fmt.Sprintf("concurrent-%d", i))
		start := make(chan struct{})
		var wg sync.WaitGroup
		reservationErr := make(chan error, 1)
		drainErr := make(chan error, 1)

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			reservationErr <- p.ReserveNodeForWorkload(ctx, workloadID, nodeID, 300, 256)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, err := p.DrainNode(ctx, nodeID)
			drainErr <- err
		}()
		close(start)
		wg.Wait()
		close(reservationErr)
		close(drainErr)

		if err := <-drainErr; err != nil {
			t.Fatalf("iteration %d drain failed: %v", i, err)
		}
		reservationResult := <-reservationErr
		node, err := p.GetNode(ctx, nodeID)
		if err != nil {
			t.Fatalf("iteration %d get node: %v", i, err)
		}
		workload, err := p.GetWorkload(ctx, workloadID)
		if err != nil {
			t.Fatalf("iteration %d get workload: %v", i, err)
		}

		if node.Status != model.NodeDraining {
			t.Fatalf("iteration %d node status = %q; want DRAINING", i, node.Status)
		}
		if node.CPUAllocated < 0 || node.CPUAllocated > node.CPUCapacity ||
			node.MemoryAllocated < 0 || node.MemoryAllocated > node.MemoryCapacity {
			t.Fatalf("iteration %d invalid allocations: (%d, %d) of (%d, %d)",
				i, node.CPUAllocated, node.MemoryAllocated, node.CPUCapacity, node.MemoryCapacity)
		}
		if reservationResult == nil {
			if workload.NodeID == nil || *workload.NodeID != nodeID || !workload.ResourceReserved ||
				node.CPUAllocated != 300 || node.MemoryAllocated != 256 {
				t.Fatalf("iteration %d successful reservation has inconsistent state", i)
			}
		} else if workload.NodeID != nil || workload.ResourceReserved ||
			node.CPUAllocated != 0 || node.MemoryAllocated != 0 {
			t.Fatalf("iteration %d failed reservation changed state: err=%v", i, reservationResult)
		}
	}
}

func newStoreIntegration(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv(storeIntegrationDatabaseEnv)
	if dsn == "" {
		t.Skipf("set %s to run PostgreSQL-backed store integration tests", storeIntegrationDatabaseEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), storeIntegrationTimeout)
	defer cancel()
	p, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		_ = p.Close()
	})
	return p
}

func createReservationFixture(t *testing.T, ctx context.Context, p *Postgres, suffix string) (string, string) {
	t.Helper()
	nodeID := integrationStoreID(t, "node-"+suffix)
	serviceID := integrationStoreID(t, "service-"+suffix)
	workloadID := integrationStoreID(t, "workload-"+suffix)
	now := time.Now().UTC()

	t.Cleanup(func() {
		_, _ = p.db.ExecContext(context.Background(), "DELETE FROM services WHERE id = $1", serviceID)
		_, _ = p.db.ExecContext(context.Background(), "DELETE FROM nodes WHERE id = $1", nodeID)
	})
	if _, err := p.db.ExecContext(ctx, `
		INSERT INTO nodes (id, address, cpu_capacity_millis, memory_capacity_mb, status)
		VALUES ($1, $2, $3, $4, $5)
	`, nodeID, "127.0.0.1:1", 1000, 1024, model.NodeReady); err != nil {
		t.Fatalf("create node: %v", err)
	}
	if err := p.CreateService(ctx, model.Service{
		ID: serviceID, Name: serviceID, Image: "integration/test:latest",
		DesiredReplicas: 1, CPURequestMillis: 300, MemoryRequestMB: 256,
		DeploymentVersion: 1, MaxSurge: 1, DeploymentStatus: model.DeploymentAvailable,
		Status: model.ServiceActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := p.CreatePendingWorkload(ctx, model.Workload{
		ID: workloadID, ServiceID: serviceID, Image: "integration/test:latest",
		CPURequestMillis: 300, MemoryRequestMB: 256, DeploymentVersion: 1,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create workload: %v", err)
	}
	return nodeID, workloadID
}

func createPendingWorkload(t *testing.T, ctx context.Context, p *Postgres, suffix string) string {
	t.Helper()
	workloadID := integrationStoreID(t, "workload-"+suffix)
	serviceID := integrationStoreID(t, "service-"+suffix)
	now := time.Now().UTC()
	if err := p.CreateService(ctx, model.Service{
		ID: serviceID, Name: serviceID, Image: "integration/test:latest",
		DesiredReplicas: 1, CPURequestMillis: 100, MemoryRequestMB: 100,
		DeploymentVersion: 1, MaxSurge: 1, DeploymentStatus: model.DeploymentAvailable,
		Status: model.ServiceActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create service: %v", err)
	}
	t.Cleanup(func() {
		_, _ = p.db.ExecContext(context.Background(), "DELETE FROM services WHERE id = $1", serviceID)
	})
	if err := p.CreatePendingWorkload(ctx, model.Workload{
		ID: workloadID, ServiceID: serviceID, Image: "integration/test:latest",
		CPURequestMillis: 100, MemoryRequestMB: 100, DeploymentVersion: 1,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create pending workload: %v", err)
	}
	return workloadID
}

func integrationStoreID(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("store-it-%s-%d", prefix, time.Now().UnixNano())
}
