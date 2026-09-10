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
            deployment_version,
            desired_state,
            actual_state,
            created_at,
            updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
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
        workload.DeploymentVersion,
        workload.DesiredState,
        workload.ActualState,
        workload.CreatedAt,
        workload.UpdatedAt,
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
			deployment_version,
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
			deployment_version,
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
			&workload.DeploymentVersion,
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

func (p *Postgres) MarkWorkloadRunning(
	ctx context.Context,
	workloadID string,
	containerID string,
) error {
	result, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET
			container_id = $1,
			actual_state = $2,
			updated_at = NOW()
		WHERE id = $3
		  AND desired_state = $4
	`,
		containerID,
		model.WorkloadRunning,
		workloadID,
		model.WorkloadScheduled,
	)
	if err != nil {
		return fmt.Errorf("mark workload running: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check workload update: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("workload %q is not scheduled", workloadID)
	}

	return nil
}

func (p *Postgres) MarkWorkloadFailed(
	ctx context.Context,
	workloadID string,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET
			actual_state = $1,
			updated_at = NOW()
		WHERE id = $2
	`,
		model.WorkloadFailed,
		workloadID,
	)
	if err != nil {
		return fmt.Errorf("mark workload failed: %w", err)
	}

	return nil
}

func (p *Postgres) GetWorkloadByContainerID(
	ctx context.Context,
	containerID string,
) (model.Workload, error) {
	var workload model.Workload

	err := p.db.QueryRowContext(ctx, `
		SELECT
			id,
			service_id,
			node_id,
			container_id,
			image,
			cpu_request_millis,
			memory_request_mb,
			deployment_version,
			desired_state,
			actual_state,
			created_at,
			updated_at
		FROM workloads
		WHERE container_id = $1
	`, containerID).Scan(
		&workload.ID,
		&workload.ServiceID,
		&workload.NodeID,
		&workload.ContainerID,
		&workload.Image,
		&workload.CPURequestMillis,
		&workload.MemoryRequestMB,
		&workload.DeploymentVersion,
		&workload.DesiredState,
		&workload.ActualState,
		&workload.CreatedAt,
		&workload.UpdatedAt,
	)

	if err != nil {
		return model.Workload{}, fmt.Errorf(
			"get workload by container ID: %w",
			err,
		)
	}

	return workload, nil
}

func (p *Postgres) UpdateWorkloadActualState(
	ctx context.Context,
	workloadID string,
	state model.WorkloadState,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET
			actual_state = $1,
			updated_at = NOW()
		WHERE id = $2
	`,
		state,
		workloadID,
	)

	if err != nil {
		return fmt.Errorf("update workload actual state: %w", err)
	}

	return nil
}

func (p *Postgres) MarkWorkloadFailedAndReleaseResources(
	ctx context.Context,
	workloadID string,
) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failure transaction: %w", err)
	}
	defer tx.Rollback()

	var (
		nodeID           *string
		cpuRequest       int
		memoryRequest    int
		actualState      model.WorkloadState
	)

	err = tx.QueryRowContext(ctx, `
		SELECT
			node_id,
			cpu_request_millis,
			memory_request_mb,
			actual_state
		FROM workloads
		WHERE id = $1
		FOR UPDATE
	`, workloadID).Scan(
		&nodeID,
		&cpuRequest,
		&memoryRequest,
		&actualState,
	)

	if err != nil {
		return fmt.Errorf("get workload for failure: %w", err)
	}

	// Already failed; nothing more to release.
	if actualState == model.WorkloadFailed {
		return nil
	}

	if nodeID != nil {
		result, err := tx.ExecContext(ctx, `
			UPDATE nodes
			SET
				cpu_allocated_millis =
					cpu_allocated_millis - $1,
				memory_allocated_mb =
					memory_allocated_mb - $2,
				updated_at = NOW()
			WHERE id = $3
			  AND cpu_allocated_millis >= $1
			  AND memory_allocated_mb >= $2
		`,
			cpuRequest,
			memoryRequest,
			*nodeID,
		)

		if err != nil {
			return fmt.Errorf("release node resources: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("check resource release: %w", err)
		}

		if rows == 0 {
			return fmt.Errorf(
				"could not release resources for node %q",
				*nodeID,
			)
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE workloads
		SET
			actual_state = $1,
			updated_at = NOW()
		WHERE id = $2
	`,
		model.WorkloadFailed,
		workloadID,
	)

	if err != nil {
		return fmt.Errorf("mark workload failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit workload failure: %w", err)
	}

	return nil
}

func (p *Postgres) RecoverWorkloadsFromNode(
	ctx context.Context,
	nodeID string,
) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin node recovery transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT
			id,
			cpu_request_millis,
			memory_request_mb
		FROM workloads
		WHERE node_id = $1
		  AND actual_state IN ($2, $3, $4)
		FOR UPDATE
	`,
		nodeID,
		model.WorkloadPending,
		model.WorkloadScheduled,
		model.WorkloadRunning,
	)
	if err != nil {
		return fmt.Errorf("find node workloads: %w", err)
	}
	defer rows.Close()

	type workloadResource struct {
		ID        string
		CPU       int
		Memory    int
	}

	var workloads []workloadResource

	for rows.Next() {
		var w workloadResource

		if err := rows.Scan(
			&w.ID,
			&w.CPU,
			&w.Memory,
		); err != nil {
			return fmt.Errorf("scan node workload: %w", err)
		}

		workloads = append(workloads, w)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate node workloads: %w", err)
	}

	for _, workload := range workloads {
		_, err := tx.ExecContext(ctx, `
			UPDATE workloads
			SET
				node_id = NULL,
				container_id = NULL,
				desired_state = $1,
				actual_state = $1,
				updated_at = NOW()
			WHERE id = $2
		`,
			model.WorkloadPending,
			workload.ID,
		)

		if err != nil {
			return fmt.Errorf(
				"reset workload %q: %w",
				workload.ID,
				err,
			)
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE nodes
		SET
			cpu_allocated_millis = 0,
			memory_allocated_mb = 0,
			updated_at = NOW()
		WHERE id = $1
	`, nodeID)

	if err != nil {
		return fmt.Errorf("reset node resources: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit node recovery: %w", err)
	}

	return nil
}

func (p *Postgres) UpdateWorkloadDesiredState(
	ctx context.Context,
	workloadID string,
	state model.WorkloadState,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET
			desired_state = $1,
			updated_at = NOW()
		WHERE id = $2
	`,
		state,
		workloadID,
	)

	if err != nil {
		return fmt.Errorf("update workload desired state: %w", err)
	}

	return nil
}

func (p *Postgres) MarkWorkloadStoppedAndReleaseResources(
	ctx context.Context,
	workloadID string,
) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin stop transaction: %w", err)
	}
	defer tx.Rollback()

	var (
		nodeID        *string
		cpuRequest    int
		memoryRequest int
		actualState   model.WorkloadState
	)

	err = tx.QueryRowContext(ctx, `
		SELECT
			node_id,
			cpu_request_millis,
			memory_request_mb,
			actual_state
		FROM workloads
		WHERE id = $1
		FOR UPDATE
	`, workloadID).Scan(
		&nodeID,
		&cpuRequest,
		&memoryRequest,
		&actualState,
	)

	if err != nil {
		return fmt.Errorf(
			"get workload for stop: %w",
			err,
		)
	}

	// Already stopped, so resources should already be released.
	if actualState == model.WorkloadStopped {
		return nil
	}

	if nodeID != nil {
		_, err = tx.ExecContext(ctx, `
			UPDATE nodes
			SET
				cpu_allocated_millis =
					cpu_allocated_millis - $1,
				memory_allocated_mb =
					memory_allocated_mb - $2,
				updated_at = NOW()
			WHERE id = $3
			  AND cpu_allocated_millis >= $1
			  AND memory_allocated_mb >= $2
		`,
			cpuRequest,
			memoryRequest,
			*nodeID,
		)

		if err != nil {
			return fmt.Errorf(
				"release workload resources: %w",
				err,
			)
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE workloads
		SET
			actual_state = $1,
			updated_at = NOW()
		WHERE id = $2
	`,
		model.WorkloadStopped,
		workloadID,
	)

	if err != nil {
		return fmt.Errorf(
			"mark workload stopped: %w",
			err,
		)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(
			"commit workload stop: %w",
			err,
		)
	}

	return nil
}

