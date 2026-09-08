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
