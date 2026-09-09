package store

import (
	"context"
	"fmt"

	"mini-orchestrator/internal/model"
)

func (p *Postgres) CreateWorkload(
	ctx context.Context,
	workload model.Workload,
) error {
	const query = `
		INSERT INTO workloads (
			id,
			service_id,
			node_id,
			container_id,
			image,
			cpu_request_millis,
			memory_request_mb,
			desired_state,
			actual_state
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err := p.db.ExecContext(
		ctx,
		query,
		workload.ID,
		workload.ServiceID,
		workload.NodeID,
		workload.ContainerID,
		workload.Image,
		workload.CPURequestMillis,
		workload.MemoryRequestMB,
		workload.DesiredState,
		workload.ActualState,
	)

	if err != nil {
		return fmt.Errorf("create workload %q: %w", workload.ID, err)
	}

	return nil
}

func (p *Postgres) GetWorkload(
	ctx context.Context,
	id string,
) (model.Workload, error) {
	const query = `
		SELECT
			id,
			service_id,
			node_id,
			container_id,
			image,
			cpu_request_millis,
			memory_request_mb,
			desired_state,
			actual_state,
			created_at,
			updated_at
		FROM workloads
		WHERE id = $1
	`

	var workload model.Workload

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&workload.ID,
		&workload.ServiceID,
		&workload.NodeID,
		&workload.ContainerID,
		&workload.Image,
		&workload.CPURequestMillis,
		&workload.MemoryRequestMB,
		&workload.DesiredState,
		&workload.ActualState,
		&workload.CreatedAt,
		&workload.UpdatedAt,
	)

	if err != nil {
		return model.Workload{}, fmt.Errorf(
			"get workload %q: %w",
			id,
			err,
		)
	}

	return workload, nil
}

func (p *Postgres) ListWorkloadsByService(
	ctx context.Context,
	serviceID string,
) ([]model.Workload, error) {
	const query = `
		SELECT
			id,
			service_id,
			node_id,
			container_id,
			image,
			cpu_request_millis,
			memory_request_mb,
			desired_state,
			actual_state,
			created_at,
			updated_at
		FROM workloads
		WHERE service_id = $1
		ORDER BY created_at
	`

	rows, err := p.db.QueryContext(ctx, query, serviceID)
	if err != nil {
		return nil, fmt.Errorf(
			"list workloads for service %q: %w",
			serviceID,
			err,
		)
	}
	defer rows.Close()

	var workloads []model.Workload

	for rows.Next() {
		var workload model.Workload

		if err := rows.Scan(
			&workload.ID,
			&workload.ServiceID,
			&workload.NodeID,
			&workload.ContainerID,
			&workload.Image,
			&workload.CPURequestMillis,
			&workload.MemoryRequestMB,
			&workload.DesiredState,
			&workload.ActualState,
			&workload.CreatedAt,
			&workload.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workload: %w", err)
		}

		workloads = append(workloads, workload)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workloads: %w", err)
	}

	return workloads, nil
}

func (p *Postgres) ReserveNodeForWorkload(
	ctx context.Context,
	workloadID string,
	nodeID string,
	cpuMillis int,
	memoryMB int,
) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin resource reservation: %w", err)
	}
	defer tx.Rollback()

	var (
		cpuCapacity     int
		memoryCapacity  int
		cpuAllocated    int
		memoryAllocated int
		status          model.NodeStatus
	)

	const nodeQuery = `
        SELECT
            cpu_capacity_millis,
            memory_capacity_mb,
            cpu_allocated_millis,
            memory_allocated_mb,
            status
        FROM nodes
        WHERE id = $1
        FOR UPDATE
    `

	err = tx.QueryRowContext(
		ctx,
		nodeQuery,
		nodeID,
	).Scan(
		&cpuCapacity,
		&memoryCapacity,
		&cpuAllocated,
		&memoryAllocated,
		&status,
	)

	if err != nil {
		return fmt.Errorf("lock node %q: %w", nodeID, err)
	}

	if status != model.NodeReady {
		return fmt.Errorf("node %q is not ready", nodeID)
	}

	availableCPU := cpuCapacity - cpuAllocated
	availableMemory := memoryCapacity - memoryAllocated

	if availableCPU < cpuMillis {
		return fmt.Errorf(
			"node %q has insufficient CPU: available=%d requested=%d",
			nodeID,
			availableCPU,
			cpuMillis,
		)
	}

	if availableMemory < memoryMB {
		return fmt.Errorf(
			"node %q has insufficient memory: available=%d requested=%d",
			nodeID,
			availableMemory,
			memoryMB,
		)
	}

	const updateNodeQuery = `
        UPDATE nodes
        SET
            cpu_allocated_millis = cpu_allocated_millis + $2,
            memory_allocated_mb = memory_allocated_mb + $3,
            updated_at = NOW()
        WHERE id = $1
    `

	_, err = tx.ExecContext(ctx, updateNodeQuery, nodeID, cpuMillis, memoryMB)
	if err != nil {
		return fmt.Errorf("reserve resources on node %q: %w", nodeID, err)
	}

	// Convert nodeID into a pointer for workloads table
	nodeIDValue := nodeID

	const assignWorkloadQuery = `
        UPDATE workloads
        SET
            node_id = $2,
            desired_state = $3,
            updated_at = NOW()
        WHERE id = $1
    `

	result, err := tx.ExecContext(
		ctx,
		assignWorkloadQuery,
		workloadID,
		nodeIDValue,
		model.WorkloadScheduled,
	)
	if err != nil {
		return fmt.Errorf("assign workload %q: %w", workloadID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check workload assignment: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("workload %q not found", workloadID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit resource reservation: %w", err)
	}

	return nil
}

func (p *Postgres) CreatePendingWorkload(
	ctx context.Context,
	workload model.Workload,
) error {
	workload.NodeID = nil
	workload.ContainerID = nil
	workload.DesiredState = model.WorkloadPending
	workload.ActualState = model.WorkloadPending

	return p.CreateWorkload(ctx, workload)
}
