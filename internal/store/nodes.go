package store

import (
	"context"
	"fmt"
	"time"

	"mini-orchestrator/internal/model"
)

func (p *Postgres) RegisterNode(ctx context.Context, node model.Node) error {
	const query = `
		INSERT INTO nodes (
			id,
			address,
			cpu_capacity_millis,
			memory_capacity_mb,
			status,
			last_heartbeat
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id)
		DO UPDATE SET
			address = EXCLUDED.address,
			cpu_capacity_millis = EXCLUDED.cpu_capacity_millis,
			memory_capacity_mb = EXCLUDED.memory_capacity_mb,
			status = EXCLUDED.status,
			last_heartbeat = EXCLUDED.last_heartbeat,
			updated_at = NOW()
	`

	_, err := p.db.ExecContext(
		ctx,
		query,
		node.ID,
		node.Address,
		node.CPUCapacity,
		node.MemoryCapacity,
		node.Status,
		node.LastHeartbeat,
	)

	if err != nil {
		return fmt.Errorf("register node %q: %w", node.ID, err)
	}

	return nil
}

func (p *Postgres) GetNode(ctx context.Context, id string) (model.Node, error) {
	const query = `
		SELECT
			id,
			address,
			cpu_capacity_millis,
			memory_capacity_mb,
			cpu_allocated_millis,
			memory_allocated_mb,
			status,
			last_heartbeat,
			created_at,
			updated_at
		FROM nodes
		WHERE id = $1
	`

	var node model.Node

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&node.ID,
		&node.Address,
		&node.CPUCapacity,
		&node.MemoryCapacity,
		&node.CPUAllocated,
		&node.MemoryAllocated,
		&node.Status,
		&node.LastHeartbeat,
		&node.CreatedAt,
		&node.UpdatedAt,
	)

	if err != nil {
		return model.Node{}, fmt.Errorf("get node %q: %w", id, err)
	}

	return node, nil
}

func (p *Postgres) ListNodes(ctx context.Context) ([]model.Node, error) {
	const query = `
		SELECT
			id,
			address,
			cpu_capacity_millis,
			memory_capacity_mb,
			cpu_allocated_millis,
			memory_allocated_mb,
			status,
			last_heartbeat,
			created_at,
			updated_at
		FROM nodes
		ORDER BY id
	`

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()

	var nodes []model.Node

	for rows.Next() {
		var node model.Node

		if err := rows.Scan(
			&node.ID,
			&node.Address,
			&node.CPUCapacity,
			&node.MemoryCapacity,
			&node.CPUAllocated,
			&node.MemoryAllocated,
			&node.Status,
			&node.LastHeartbeat,
			&node.CreatedAt,
			&node.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}

		nodes = append(nodes, node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate nodes: %w", err)
	}

	return nodes, nil
}

func (p *Postgres) UpdateHeartbeat(
	ctx context.Context,
	id string,
	status model.NodeStatus,
	now time.Time,
) error {
	const query = `
		UPDATE nodes
		SET
			status = $2,
			last_heartbeat = $3,
			updated_at = NOW()
		WHERE id = $1
	`

	result, err := p.db.ExecContext(ctx, query, id, status, now)
	if err != nil {
		return fmt.Errorf("update heartbeat for node %q: %w", id, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check heartbeat update: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("node %q not found", id)
	}

	return nil
}

func (p *Postgres) SetNodeStatus(
	ctx context.Context,
	id string,
	status model.NodeStatus,
) error {
	const query = `
		UPDATE nodes
		SET
			status = $2,
			updated_at = NOW()
		WHERE id = $1
	`

	result, err := p.db.ExecContext(
		ctx,
		query,
		id,
		status,
	)
	if err != nil {
		return fmt.Errorf(
			"set node %q status: %w",
			id,
			err,
		)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"check node status update: %w",
			err,
		)
	}

	if rows == 0 {
		return fmt.Errorf("node %q not found", id)
	}

	return nil
}
