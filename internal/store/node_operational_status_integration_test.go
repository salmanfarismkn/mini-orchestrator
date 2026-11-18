package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"mini-orchestrator/internal/model"
)

func integrationContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), storeIntegrationTimeout)
}

func TestNodeOperationalStatusIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := integrationContext(t)
	defer cancel()

	t.Run("ready node is schedulable and empty", func(t *testing.T) {
		nodeID, _ := createReservationFixture(t, ctx, p, "operational-ready")
		status, err := p.GetNodeOperationalStatus(ctx, nodeID)
		if err != nil {
			t.Fatalf("get operational status: %v", err)
		}
		if status.Node.Status != model.NodeReady || !status.Schedulable || !status.Empty ||
			status.AssignedWorkloadCount != 0 || status.ReservedCPU != 0 || status.ReservedMemory != 0 {
			t.Fatalf("unexpected ready status: %+v", status)
		}
	})

	t.Run("draining node preserves assigned workloads and reservations", func(t *testing.T) {
		nodeID, workloadID := createReservationFixture(t, ctx, p, "operational-assigned")
		if err := p.ReserveNodeForWorkload(ctx, workloadID, nodeID, 300, 256); err != nil {
			t.Fatalf("reserve workload: %v", err)
		}
		if _, err := p.DrainNode(ctx, nodeID); err != nil {
			t.Fatalf("drain node: %v", err)
		}

		status, err := p.GetNodeOperationalStatus(ctx, nodeID)
		if err != nil {
			t.Fatalf("get operational status: %v", err)
		}
		if status.Node.Status != model.NodeDraining || status.Schedulable || status.Empty ||
			status.AssignedWorkloadCount != 1 || status.ReservedCPU != 300 || status.ReservedMemory != 256 ||
			status.AvailableCPU != 700 || status.AvailableMemory != 768 {
			t.Fatalf("unexpected draining status: %+v", status)
		}
	})

	t.Run("multiple assigned workloads are counted", func(t *testing.T) {
		nodeID, workloadID := createReservationFixture(t, ctx, p, "operational-multiple")
		if err := p.ReserveNodeForWorkload(ctx, workloadID, nodeID, 300, 256); err != nil {
			t.Fatalf("reserve first workload: %v", err)
		}
		secondID := createPendingWorkload(t, ctx, p, "multi2")
		if err := p.ReserveNodeForWorkload(ctx, secondID, nodeID, 100, 128); err != nil {
			t.Fatalf("reserve second workload: %v", err)
		}

		status, err := p.GetNodeOperationalStatus(ctx, nodeID)
		if err != nil {
			t.Fatalf("get operational status: %v", err)
		}
		if status.AssignedWorkloadCount != 2 || status.ReservedCPU != 400 ||
			status.ReservedMemory != 384 || status.AvailableCPU != 600 ||
			status.AvailableMemory != 640 || status.Empty {
			t.Fatalf("unexpected multiple-workload status: %+v", status)
		}
	})

	t.Run("draining node with no assignments is empty", func(t *testing.T) {
		nodeID, _ := createReservationFixture(t, ctx, p, "operational-empty")
		if _, err := p.DrainNode(ctx, nodeID); err != nil {
			t.Fatalf("drain node: %v", err)
		}
		status, err := p.GetNodeOperationalStatus(ctx, nodeID)
		if err != nil {
			t.Fatalf("get operational status: %v", err)
		}
		if status.Node.Status != model.NodeDraining || status.Schedulable ||
			!status.Empty || status.AssignedWorkloadCount != 0 {
			t.Fatalf("unexpected empty draining status: %+v", status)
		}
	})

	t.Run("reactivation reports ready", func(t *testing.T) {
		nodeID, _ := createReservationFixture(t, ctx, p, "operational-reactivate")
		if _, err := p.DrainNode(ctx, nodeID); err != nil {
			t.Fatalf("drain node: %v", err)
		}
		if _, err := p.ReactivateNode(ctx, nodeID); err != nil {
			t.Fatalf("reactivate node: %v", err)
		}
		status, err := p.GetNodeOperationalStatus(ctx, nodeID)
		if err != nil {
			t.Fatalf("get operational status: %v", err)
		}
		if status.Node.Status != model.NodeReady || !status.Schedulable {
			t.Fatalf("unexpected reactivated status: %+v", status)
		}
	})

	t.Run("unknown node", func(t *testing.T) {
		_, err := p.GetNodeOperationalStatus(ctx, integrationStoreID(t, "operational-unknown"))
		if !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("error = %v; want ErrNodeNotFound", err)
		}
	})

	t.Run("read-only inspection", func(t *testing.T) {
		nodeID, workloadID := createReservationFixture(t, ctx, p, "operational-read-only")
		beforeNode, err := p.GetNode(ctx, nodeID)
		if err != nil {
			t.Fatalf("get node before inspection: %v", err)
		}
		beforeWorkload, err := p.GetWorkload(ctx, workloadID)
		if err != nil {
			t.Fatalf("get workload before inspection: %v", err)
		}
		if _, err := p.GetNodeOperationalStatus(ctx, nodeID); err != nil {
			t.Fatalf("inspect node: %v", err)
		}
		afterNode, err := p.GetNode(ctx, nodeID)
		if err != nil {
			t.Fatalf("get node after inspection: %v", err)
		}
		afterWorkload, err := p.GetWorkload(ctx, workloadID)
		if err != nil {
			t.Fatalf("get workload after inspection: %v", err)
		}
		if beforeNode.CPUAllocated != afterNode.CPUAllocated ||
			beforeNode.MemoryAllocated != afterNode.MemoryAllocated ||
			beforeWorkload.ResourceReserved != afterWorkload.ResourceReserved ||
			beforeWorkload.NodeID != nil || afterWorkload.NodeID != nil {
			t.Fatalf("inspection changed reservation state: before node=%+v workload=%+v, after node=%+v workload=%+v",
				beforeNode, beforeWorkload, afterNode, afterWorkload)
		}
	})
}

func TestNodeOperationalStatusOverlapsDrainIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := integrationContext(t)
	defer cancel()

	nodeID, _ := createReservationFixture(t, ctx, p, "operational-overlap")
	start := make(chan struct{})
	var wg sync.WaitGroup
	drainErr := make(chan error, 1)
	statusResult := make(chan NodeOperationalStatus, 1)
	statusErr := make(chan error, 1)

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, err := p.DrainNode(ctx, nodeID)
		drainErr <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		status, err := p.GetNodeOperationalStatus(ctx, nodeID)
		statusResult <- status
		statusErr <- err
	}()
	close(start)
	wg.Wait()

	if err := <-drainErr; err != nil {
		t.Fatalf("drain node: %v", err)
	}
	if err := <-statusErr; err != nil {
		t.Fatalf("inspect node during drain: %v", err)
	}
	status := <-statusResult
	if status.Node.Status != model.NodeReady && status.Node.Status != model.NodeDraining {
		t.Fatalf("unexpected overlapping status: %+v", status)
	}
	if status.AssignedWorkloadCount != 0 || !status.Empty ||
		status.ReservedCPU != 0 || status.ReservedMemory != 0 {
		t.Fatalf("overlapping inspection has inconsistent empty-node state: %+v", status)
	}
}

func TestMarkNodeUnhealthyRequiresStalePersistedHeartbeatIntegration(t *testing.T) {
	p := newStoreIntegration(t)
	ctx, cancel := integrationContext(t)
	defer cancel()

	nodeID, _ := createReservationFixture(t, ctx, p, "health-heartbeat-race")
	now := time.Now().UTC()
	cutoff := now.Add(-time.Minute)
	if _, err := p.db.ExecContext(ctx, `
		UPDATE nodes
		SET status = $1, last_heartbeat = $2
		WHERE id = $3
	`, model.NodeReady, now, nodeID); err != nil {
		t.Fatalf("record fresh heartbeat: %v", err)
	}

	marked, err := p.MarkNodeUnhealthyIfHeartbeatBefore(ctx, nodeID, cutoff)
	if err != nil {
		t.Fatalf("mark node with fresh heartbeat unhealthy: %v", err)
	}
	if marked {
		t.Fatal("fresh heartbeat was overwritten by a stale health-check decision")
	}
	node, err := p.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("read node after fresh heartbeat check: %v", err)
	}
	if node.Status != model.NodeReady {
		t.Fatalf("node status = %q after fresh heartbeat; want READY", node.Status)
	}

	staleHeartbeat := cutoff.Add(-time.Second)
	if _, err := p.db.ExecContext(ctx, `
		UPDATE nodes
		SET last_heartbeat = $1
		WHERE id = $2
	`, staleHeartbeat, nodeID); err != nil {
		t.Fatalf("record stale heartbeat: %v", err)
	}
	marked, err = p.MarkNodeUnhealthyIfHeartbeatBefore(ctx, nodeID, cutoff)
	if err != nil {
		t.Fatalf("mark genuinely stale node unhealthy: %v", err)
	}
	if !marked {
		t.Fatal("stale heartbeat did not mark node unhealthy")
	}
	node, err = p.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("read node after stale heartbeat check: %v", err)
	}
	if node.Status != model.NodeUnhealthy {
		t.Fatalf("node status = %q after stale heartbeat; want UNHEALTHY", node.Status)
	}
}
