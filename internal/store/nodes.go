package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"mini-orchestrator/internal/model"
)

var ErrNodeIdentityNotBound = errors.New("node identity is not enrolled")
var ErrNodeIdentityMismatch = errors.New("authenticated certificate identity does not match node")
var ErrNodeLeaseStale = errors.New("node lease generation is stale")
var ErrNodeNotFound = errors.New("node not found")
var ErrInvalidNodeTransition = errors.New("invalid node state transition")

type NodeOperationalStatus struct {
	Node                  model.Node
	Schedulable           bool
	AssignedWorkloadCount int
	ReservedCPU           int
	ReservedMemory        int
	AvailableCPU          int
	AvailableMemory       int
	Empty                 bool
}

func (p *Postgres) RegisterNode(ctx context.Context, node model.Node, certificateIdentity, registrationID string) (int64, error) {
	if registrationID == "" {
		return 0, fmt.Errorf("registration id is required for node %q", node.ID)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin node registration: %w", err)
	}
	defer tx.Rollback()

	var boundIdentity sql.NullString
	var currentGeneration int64
	if err := tx.QueryRowContext(ctx, `
		SELECT certificate_identity, lease_generation
		FROM nodes
		WHERE id = $1
		FOR UPDATE
	`, node.ID).Scan(&boundIdentity, &currentGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("%w: node %q is not enrolled", ErrNodeIdentityNotBound, node.ID)
		}
		return 0, fmt.Errorf("lock node registration %q: %w", node.ID, err)
	}
	if !boundIdentity.Valid {
		return 0, fmt.Errorf("%w: node %q is unbound", ErrNodeIdentityNotBound, node.ID)
	}
	if boundIdentity.String != certificateIdentity {
		return 0, fmt.Errorf("%w: node %q", ErrNodeIdentityMismatch, node.ID)
	}

	var historicalGeneration sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT lease_generation
		FROM node_registration_history
		WHERE node_id = $1 AND registration_id = $2
	`, node.ID, registrationID).Scan(&historicalGeneration); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read registration history for node %q: %w", node.ID, err)
	}

	assignedGeneration := currentGeneration
	if historicalGeneration.Valid {
		if historicalGeneration.Int64 != currentGeneration {
			return 0, fmt.Errorf("%w: node %q registration %q generation %d; current %d",
				ErrNodeLeaseStale, node.ID, registrationID, historicalGeneration.Int64, currentGeneration)
		}
	} else {
		if currentGeneration == int64(^uint64(0)>>1) {
			return 0, fmt.Errorf("node %q lease generation overflow", node.ID)
		}
		assignedGeneration++
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE nodes
		SET address = $2,
			cpu_capacity_millis = $3,
			memory_capacity_mb = $4,
			status = CASE WHEN status = 'DRAINING' THEN 'DRAINING' ELSE $5 END,
			last_heartbeat = $6,
			lease_generation = $7,
			registration_id = $8,
			updated_at = NOW()
		WHERE id = $1
	`, node.ID, node.Address, node.CPUCapacity, node.MemoryCapacity,
		node.Status, node.LastHeartbeat, assignedGeneration, registrationID); err != nil {
		return 0, fmt.Errorf("register node %q: %w", node.ID, err)
	}
	if !historicalGeneration.Valid {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO node_registration_history (node_id, registration_id, lease_generation)
			VALUES ($1, $2, $3)
		`, node.ID, registrationID, assignedGeneration); err != nil {
			return 0, fmt.Errorf("record registration history for node %q: %w", node.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit node registration %q: %w", node.ID, err)
	}
	return assignedGeneration, nil
}

func (p *Postgres) GetNode(ctx context.Context, id string) (model.Node, error) {
	const query = `
		SELECT
			id,
			certificate_identity,
			lease_generation,
			registration_id,
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

	var (
		node                model.Node
		certificateIdentity sql.NullString
		registrationID      sql.NullString
		lastHeartbeat       sql.NullTime
	)

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&node.ID,
		&certificateIdentity,
		&node.LeaseGeneration,
		&registrationID,
		&node.Address,
		&node.CPUCapacity,
		&node.MemoryCapacity,
		&node.CPUAllocated,
		&node.MemoryAllocated,
		&node.Status,
		&lastHeartbeat,
		&node.CreatedAt,
		&node.UpdatedAt,
	)

	if certificateIdentity.Valid {
		node.CertificateIdentity = certificateIdentity.String
	}
	if registrationID.Valid {
		node.RegistrationID = registrationID.String
	}
	if lastHeartbeat.Valid {
		node.LastHeartbeat = lastHeartbeat.Time
	}

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Node{}, fmt.Errorf("%w: node %q", ErrNodeNotFound, id)
		}
		return model.Node{}, fmt.Errorf("get node %q: %w", id, err)
	}

	return node, nil
}

func (p *Postgres) GetNodeOperationalStatus(ctx context.Context, id string) (NodeOperationalStatus, error) {
	const query = `
		SELECT
			n.id,
			n.address,
			n.cpu_capacity_millis,
			n.memory_capacity_mb,
			n.cpu_allocated_millis,
			n.memory_allocated_mb,
			n.status,
			n.created_at,
			n.updated_at,
			COUNT(w.id)::INTEGER
		FROM nodes n
		LEFT JOIN workloads w ON w.node_id = n.id
		WHERE n.id = $1
		GROUP BY
			n.id,
			n.address,
			n.cpu_capacity_millis,
			n.memory_capacity_mb,
			n.cpu_allocated_millis,
			n.memory_allocated_mb,
			n.status,
			n.created_at,
			n.updated_at
	`

	var status NodeOperationalStatus
	if err := p.db.QueryRowContext(ctx, query, id).Scan(
		&status.Node.ID,
		&status.Node.Address,
		&status.Node.CPUCapacity,
		&status.Node.MemoryCapacity,
		&status.Node.CPUAllocated,
		&status.Node.MemoryAllocated,
		&status.Node.Status,
		&status.Node.CreatedAt,
		&status.Node.UpdatedAt,
		&status.AssignedWorkloadCount,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NodeOperationalStatus{}, fmt.Errorf("%w: node %q", ErrNodeNotFound, id)
		}
		return NodeOperationalStatus{}, fmt.Errorf("get operational status for node %q: %w", id, err)
	}

	status.Schedulable = status.Node.Status == model.NodeReady
	status.ReservedCPU = status.Node.CPUAllocated
	status.ReservedMemory = status.Node.MemoryAllocated
	status.AvailableCPU = status.Node.CPUCapacity - status.ReservedCPU
	status.AvailableMemory = status.Node.MemoryCapacity - status.ReservedMemory
	status.Empty = status.AssignedWorkloadCount == 0
	return status, nil
}

func (p *Postgres) ListNodes(ctx context.Context) ([]model.Node, error) {
	const query = `
		SELECT
			id,
			certificate_identity,
			lease_generation,
			registration_id,
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
		var (
			node                model.Node
			certificateIdentity sql.NullString
			registrationID      sql.NullString
			lastHeartbeat       sql.NullTime
		)

		if err := rows.Scan(
			&node.ID,
			&certificateIdentity,
			&node.LeaseGeneration,
			&registrationID,
			&node.Address,
			&node.CPUCapacity,
			&node.MemoryCapacity,
			&node.CPUAllocated,
			&node.MemoryAllocated,
			&node.Status,
			&lastHeartbeat,
			&node.CreatedAt,
			&node.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}

		if certificateIdentity.Valid {
			node.CertificateIdentity = certificateIdentity.String
		}
		if registrationID.Valid {
			node.RegistrationID = registrationID.String
		}
		if lastHeartbeat.Valid {
			node.LastHeartbeat = lastHeartbeat.Time
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
	certificateIdentity string,
	leaseGeneration int64,
	status model.NodeStatus,
	now time.Time,
) error {
	const query = `
		UPDATE nodes
		SET
			status = CASE WHEN status = 'DRAINING' THEN 'DRAINING' ELSE $3 END,
			last_heartbeat = $4,
			updated_at = NOW()
		WHERE id = $1
		  AND certificate_identity = $2
		  AND lease_generation = $5
	`

	result, err := p.db.ExecContext(ctx, query, id, certificateIdentity, status, now, leaseGeneration)
	if err != nil {
		return fmt.Errorf("update heartbeat for node %q: %w", id, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check heartbeat update: %w", err)
	}
	if rows == 1 {
		return nil
	}

	var boundIdentity sql.NullString
	var currentGeneration int64
	if err := p.db.QueryRowContext(ctx,
		`SELECT certificate_identity, lease_generation FROM nodes WHERE id = $1`, id,
	).Scan(&boundIdentity, &currentGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: node %q is not enrolled", ErrNodeIdentityNotBound, id)
		}
		return fmt.Errorf("check node identity and lease %q: %w", id, err)
	}
	if !boundIdentity.Valid {
		return fmt.Errorf("%w: node %q is unbound", ErrNodeIdentityNotBound, id)
	}
	if boundIdentity.String != certificateIdentity {
		return fmt.Errorf("%w: node %q", ErrNodeIdentityMismatch, id)
	}
	if currentGeneration != leaseGeneration {
		return fmt.Errorf("%w: node %q generation %d; current %d", ErrNodeLeaseStale, id, leaseGeneration, currentGeneration)
	}
	return fmt.Errorf("node heartbeat was not applied for node %q", id)
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
		  AND ($2 != 'UNHEALTHY' OR status != 'DRAINING')
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

func (p *Postgres) MarkNodeUnhealthyIfHeartbeatBefore(
	ctx context.Context,
	id string,
	cutoff time.Time,
) (bool, error) {
	result, err := p.db.ExecContext(ctx, `
		UPDATE nodes
		SET status = $2,
		    updated_at = NOW()
		WHERE id = $1
		  AND status <> $3
		  AND (last_heartbeat IS NULL OR last_heartbeat < $4)
	`, id, model.NodeUnhealthy, model.NodeDraining, cutoff)
	if err != nil {
		return false, fmt.Errorf("mark node %q unhealthy if heartbeat is stale: %w", id, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check stale heartbeat transition for node %q: %w", id, err)
	}
	return rows == 1, nil
}

func (p *Postgres) DrainNode(ctx context.Context, id string) (model.Node, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Node{}, fmt.Errorf("begin node drain: %w", err)
	}
	defer tx.Rollback()

	var current model.NodeStatus
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM nodes WHERE id = $1 FOR UPDATE`, id,
	).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Node{}, fmt.Errorf("%w: node %q", ErrNodeNotFound, id)
		}
		return model.Node{}, fmt.Errorf("lock node %q for drain: %w", id, err)
	}

	switch current {
	case model.NodeDraining:
		// Repeated drain requests are idempotent.
	case model.NodeReady, model.NodeUnhealthy:
		if _, err := tx.ExecContext(ctx,
			`UPDATE nodes SET status = 'DRAINING', updated_at = NOW() WHERE id = $1`, id,
		); err != nil {
			return model.Node{}, fmt.Errorf("drain node %q: %w", id, err)
		}
		current = model.NodeDraining
	default:
		return model.Node{}, fmt.Errorf("%w: node %q cannot transition from %s to DRAINING", ErrInvalidNodeTransition, id, current)
	}

	if err := tx.Commit(); err != nil {
		return model.Node{}, fmt.Errorf("commit node drain: %w", err)
	}
	return model.Node{ID: id, Status: current}, nil
}

func (p *Postgres) ReactivateNode(ctx context.Context, id string) (model.Node, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Node{}, fmt.Errorf("begin node reactivation: %w", err)
	}
	defer tx.Rollback()

	var current model.NodeStatus
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM nodes WHERE id = $1 FOR UPDATE`, id,
	).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Node{}, fmt.Errorf("%w: node %q", ErrNodeNotFound, id)
		}
		return model.Node{}, fmt.Errorf("lock node %q for reactivation: %w", id, err)
	}

	switch current {
	case model.NodeReady:
		// Repeated reactivation requests are idempotent.
	case model.NodeDraining:
		if _, err := tx.ExecContext(ctx,
			`UPDATE nodes SET status = 'READY', updated_at = NOW() WHERE id = $1`, id,
		); err != nil {
			return model.Node{}, fmt.Errorf("reactivate node %q: %w", id, err)
		}
		current = model.NodeReady
	case model.NodeUnhealthy:
		return model.Node{}, fmt.Errorf("%w: node %q is UNHEALTHY and cannot be reactivated", ErrInvalidNodeTransition, id)
	default:
		return model.Node{}, fmt.Errorf("%w: node %q cannot be reactivated from %s", ErrInvalidNodeTransition, id, current)
	}

	if err := tx.Commit(); err != nil {
		return model.Node{}, fmt.Errorf("commit node reactivation: %w", err)
	}
	return model.Node{ID: id, Status: current}, nil
}
