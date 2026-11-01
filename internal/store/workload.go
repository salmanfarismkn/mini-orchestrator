package store

import (
	"context"
	"database/sql"
	"fmt"

	"mini-orchestrator/internal/model"
)

func (p *Postgres) CreateWorkload(
	ctx context.Context,
	workload model.Workload,
) error {
	if workload.ExecutionState == "" {
		workload.ExecutionState = model.ExecutionIdle
	}
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
            execution_state,
            execution_error,
            resource_reserved,
            created_at,
            updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
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
		workload.ExecutionState,
		workload.ExecutionError,
		workload.ResourceReserved,
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
			execution_state,
			execution_error,
			resource_reserved,
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
		&workload.DeploymentVersion,
		&workload.DesiredState,
		&workload.ActualState,
		&workload.ExecutionState,
		&workload.ExecutionError,
		&workload.ResourceReserved,
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
			execution_state,
			execution_error,
			resource_reserved,
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
			&workload.ExecutionState,
			&workload.ExecutionError,
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
		desiredState     model.WorkloadState
		actualState      model.WorkloadState
		resourceReserved bool
		existingNodeID   sql.NullString
	)

	err = tx.QueryRowContext(ctx, `
		SELECT desired_state, actual_state, resource_reserved, node_id
		FROM workloads
		WHERE id = $1
		FOR UPDATE
	`, workloadID).Scan(&desiredState, &actualState, &resourceReserved, &existingNodeID)
	if err != nil {
		return fmt.Errorf("lock workload %q for reservation: %w", workloadID, err)
	}
	if resourceReserved {
		if !existingNodeID.Valid || existingNodeID.String != nodeID {
			return fmt.Errorf("workload %q already holds resources on a different or missing node", workloadID)
		}
		return nil
	}
	if desiredState != model.WorkloadPending || actualState != model.WorkloadPending || existingNodeID.Valid {
		return fmt.Errorf("workload %q is not eligible for scheduling", workloadID)
	}

	var cpuCapacity, memoryCapacity, cpuAllocated, memoryAllocated int
	var status model.NodeStatus
	err = tx.QueryRowContext(ctx, `
		SELECT cpu_capacity_millis, memory_capacity_mb,
		       cpu_allocated_millis, memory_allocated_mb, status
		FROM nodes
		WHERE id = $1
		FOR UPDATE
	`, nodeID).Scan(&cpuCapacity, &memoryCapacity, &cpuAllocated, &memoryAllocated, &status)
	if err != nil {
		return fmt.Errorf("lock node %q: %w", nodeID, err)
	}
	if status != model.NodeReady {
		return fmt.Errorf("node %q is not ready", nodeID)
	}
	if cpuCapacity < cpuAllocated || memoryCapacity < memoryAllocated {
		return fmt.Errorf("node %q has inconsistent resource allocations", nodeID)
	}

	availableCPU := cpuCapacity - cpuAllocated
	availableMemory := memoryCapacity - memoryAllocated
	if availableCPU < cpuMillis {
		return fmt.Errorf("node %q has insufficient CPU: available=%d requested=%d", nodeID, availableCPU, cpuMillis)
	}
	if availableMemory < memoryMB {
		return fmt.Errorf("node %q has insufficient memory: available=%d requested=%d", nodeID, availableMemory, memoryMB)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE nodes
		SET cpu_allocated_millis = cpu_allocated_millis + $2,
		    memory_allocated_mb = memory_allocated_mb + $3,
		    updated_at = NOW()
		WHERE id = $1
		  AND status = $4
		  AND cpu_capacity_millis - cpu_allocated_millis >= $2
		  AND memory_capacity_mb - memory_allocated_mb >= $3
	`, nodeID, cpuMillis, memoryMB, model.NodeReady)
	if err != nil {
		return fmt.Errorf("reserve resources on node %q: %w", nodeID, err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("reserve resources on node %q: capacity changed or node became unavailable", nodeID)
	}

	result, err = tx.ExecContext(ctx, `
		UPDATE workloads
		SET node_id = $2,
		    desired_state = $3,
		    execution_state = $4,
		    execution_error = '',
		    resource_reserved = TRUE,
		    updated_at = NOW()
		WHERE id = $1
		  AND desired_state = $5
		  AND actual_state = $5
		  AND node_id IS NULL
		  AND resource_reserved = FALSE
	`, workloadID, nodeID, model.WorkloadScheduled, model.ExecutionIdle, model.WorkloadPending)
	if err != nil {
		return fmt.Errorf("assign workload %q: %w", workloadID, err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("workload %q eligibility changed during reservation", workloadID)
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

func (p *Postgres) ClaimExecution(
	ctx context.Context,
	workloadID string,
) (bool, error) {
	result, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET execution_state = $1,
			execution_error = '',
			updated_at = NOW()
		WHERE id = $2
		  AND desired_state = $3
		  AND execution_state IN ($4, $5)
	`,
		model.ExecutionStarting,
		workloadID,
		model.WorkloadScheduled,
		model.ExecutionIdle,
		model.ExecutionStarting,
	)
	if err != nil {
		return false, fmt.Errorf("claim workload execution: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check execution claim: %w", err)
	}
	return rows == 1, nil
}

func (p *Postgres) PersistContainerID(
	ctx context.Context,
	workloadID string,
	containerID string,
) error {
	result, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET container_id = $1,
			execution_state = $2,
			execution_error = '',
			updated_at = NOW()
		WHERE id = $3
		  AND desired_state = $4
		  AND execution_state IN ($5, $6)
	  AND (container_id IS NULL OR container_id = $1)
	`,
		containerID,
		model.ExecutionCreated,
		workloadID,
		model.WorkloadScheduled,
		model.ExecutionStarting,
		model.ExecutionCreated,
	)
	if err != nil {
		return fmt.Errorf("persist workload container ID: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check container ID persistence: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("workload %q execution state or container identity changed", workloadID)
	}
	return nil
}

func (p *Postgres) SetExecutionError(
	ctx context.Context,
	workloadID string,
	state model.ExecutionState,
	message string,
) error {
	result, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET execution_state = $1,
			execution_error = $2,
			updated_at = NOW()
		WHERE id = $3
		  AND desired_state = $4
	`, state, message, workloadID, model.WorkloadScheduled)
	if err != nil {
		return fmt.Errorf("set workload execution error: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("check execution error update: %w", err)
	} else if rows == 0 {
		return fmt.Errorf("workload %q is not scheduled", workloadID)
	}
	return nil
}

func (p *Postgres) ResetExecutionIntent(
	ctx context.Context,
	workloadID string,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET container_id = NULL,
			actual_state = 'SCHEDULED',
			execution_state = $1,
			execution_error = '',
			updated_at = NOW()
		WHERE id = $2
		  AND desired_state = $3
		  AND execution_state IN ($1, $4, $5)
	`, model.ExecutionStarting, workloadID, model.WorkloadScheduled, model.ExecutionCreated, model.ExecutionRunning)
	if err != nil {
		return fmt.Errorf("reset workload execution intent: %w", err)
	}
	return nil
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
			execution_state = $5,
			execution_error = '',
			updated_at = NOW()
		WHERE id = $3
		  AND desired_state = $4
	`,
		containerID,
		model.WorkloadRunning,
		workloadID,
		model.WorkloadScheduled,
		model.ExecutionRunning,
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
	return p.MarkWorkloadFailedAndReleaseResources(ctx, workloadID)
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
			execution_state,
			execution_error,
			resource_reserved,
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
		&workload.ExecutionState,
		&workload.ExecutionError,
		&workload.ResourceReserved,
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
	if state == model.WorkloadFailed {
		return p.MarkWorkloadFailedAndReleaseResources(ctx, workloadID)
	}
	if state == model.WorkloadStopped {
		return p.MarkWorkloadStoppedAndReleaseResources(ctx, workloadID)
	}
	_, err := p.db.ExecContext(ctx, `
		UPDATE workloads
		SET actual_state = $1, updated_at = NOW()
		WHERE id = $2
	`, state, workloadID)
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
		resourceReserved bool
	)

	err = tx.QueryRowContext(ctx, `
		SELECT node_id, cpu_request_millis, memory_request_mb,
		       actual_state, resource_reserved
		FROM workloads WHERE id = $1 FOR UPDATE
	`, workloadID).Scan(&nodeID, &cpuRequest, &memoryRequest, &actualState, &resourceReserved)
	if err != nil {
		return fmt.Errorf("get workload for failure: %w", err)
	}

	if actualState == model.WorkloadFailed && !resourceReserved {
		return nil
	}

	if resourceReserved {
		if nodeID == nil {
			return fmt.Errorf("inconsistent reservation for workload %q: reserved without node", workloadID)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE nodes
			SET cpu_allocated_millis = cpu_allocated_millis - $1,
			    memory_allocated_mb = memory_allocated_mb - $2,
			    updated_at = NOW()
			WHERE id = $3
			  AND cpu_allocated_millis >= $1
			  AND memory_allocated_mb >= $2
		`, cpuRequest, memoryRequest, *nodeID)
		if err != nil {
			return fmt.Errorf("release node resources: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("check resource release: %w", err)
		} else if rows != 1 {
			return fmt.Errorf("inconsistent resource allocation on node %q for workload %q", *nodeID, workloadID)
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE workloads
		SET actual_state = $1,
		    execution_state = $2,
		    resource_reserved = FALSE,
		    node_id = NULL,
		    container_id = NULL,
		    updated_at = NOW()
		WHERE id = $3
	`, model.WorkloadFailed, model.ExecutionFailed, workloadID)
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
		  AND resource_reserved = TRUE
		  AND actual_state IN ($2, $3, $4, $5)
		FOR UPDATE
	`,
		nodeID,
		model.WorkloadPending,
		model.WorkloadScheduled,
		model.WorkloadRunning,
		model.WorkloadFailed,
	)
	if err != nil {
		return fmt.Errorf("find node workloads: %w", err)
	}
	defer rows.Close()

	type workloadResource struct {
		ID     string
		CPU    int
		Memory int
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
				execution_state = $3,
				execution_error = '',
				resource_reserved = FALSE,
				updated_at = NOW()
			WHERE id = $2
		`,
			model.WorkloadPending,
			workload.ID,
			model.ExecutionIdle,
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
		nodeID           *string
		cpuRequest       int
		memoryRequest    int
		actualState      model.WorkloadState
		resourceReserved bool
	)

	err = tx.QueryRowContext(ctx, `
		SELECT node_id, cpu_request_millis, memory_request_mb,
		       actual_state, resource_reserved
		FROM workloads WHERE id = $1 FOR UPDATE
	`, workloadID).Scan(&nodeID, &cpuRequest, &memoryRequest, &actualState, &resourceReserved)

	if err != nil {
		return fmt.Errorf(
			"get workload for stop: %w",
			err,
		)
	}

	if actualState == model.WorkloadStopped && !resourceReserved {
		return nil
	}

	if resourceReserved {
		if nodeID == nil {
			return fmt.Errorf("inconsistent reservation for workload %q: reserved without node", workloadID)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE nodes
			SET cpu_allocated_millis = cpu_allocated_millis - $1,
			    memory_allocated_mb = memory_allocated_mb - $2,
			    updated_at = NOW()
			WHERE id = $3
			  AND cpu_allocated_millis >= $1
			  AND memory_allocated_mb >= $2
		`, cpuRequest, memoryRequest, *nodeID)
		if err != nil {
			return fmt.Errorf("release workload resources: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("check resource release: %w", err)
		} else if rows != 1 {
			return fmt.Errorf("inconsistent resource allocation on node %q for workload %q", *nodeID, workloadID)
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE workloads
		SET actual_state = $1,
		    container_id = NULL,
		    node_id = NULL,
		    resource_reserved = FALSE,
		    execution_state = $2,
		    execution_error = '',
		    updated_at = NOW()
		WHERE id = $3
	`,
		model.WorkloadStopped,
		model.ExecutionIdle,
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

func (p *Postgres) ListWorkloadsByNode(
	ctx context.Context,
	nodeID string,
) ([]model.Workload, error) {
	rows, err := p.db.QueryContext(
		ctx,
		`
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
			execution_state,
			execution_error,
			resource_reserved,
			created_at,
			updated_at
		FROM workloads
		WHERE node_id = $1
		ORDER BY created_at
		`,
		nodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("list workloads by node: %w", err)
	}
	defer rows.Close()

	var workloads []model.Workload

	for rows.Next() {
		var w model.Workload
		var nodeID sql.NullString
		var containerID sql.NullString

		if err := rows.Scan(
			&w.ID,
			&w.ServiceID,
			&nodeID,
			&containerID,
			&w.Image,
			&w.CPURequestMillis,
			&w.MemoryRequestMB,
			&w.DeploymentVersion,
			&w.DesiredState,
			&w.ActualState,
			&w.ExecutionState,
			&w.ExecutionError,
			&w.ResourceReserved,
			&w.CreatedAt,
			&w.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workload: %w", err)
		}

		if nodeID.Valid {
			w.NodeID = &nodeID.String
		}

		if containerID.Valid {
			w.ContainerID = &containerID.String
		}

		workloads = append(workloads, w)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workloads: %w", err)
	}

	return workloads, nil
}
