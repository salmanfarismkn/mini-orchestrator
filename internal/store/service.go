package store

import (
	"context"
	"fmt"

	"mini-orchestrator/internal/model"
)

func (p *Postgres) CreateService(
	ctx context.Context,
	service model.Service,
) error {
	const query = `
		INSERT INTO services (
			id,
			name,
			image,
			desired_replicas,
			cpu_request_millis,
			memory_request_mb
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := p.db.ExecContext(
		ctx,
		query,
		service.ID,
		service.Name,
		service.Image,
		service.DesiredReplicas,
		service.CPURequestMillis,
		service.MemoryRequestMB,
	)

	if err != nil {
		return fmt.Errorf("create service %q: %w", service.Name, err)
	}

	return nil
}

func (p *Postgres) GetService(
	ctx context.Context,
	id string,
) (model.Service, error) {
	const query = `
		SELECT
			id,
			name,
			image,
			desired_replicas,
			cpu_request_millis,
			memory_request_mb,
			created_at,
			updated_at
		FROM services
		WHERE id = $1
	`

	var service model.Service

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&service.ID,
		&service.Name,
		&service.Image,
		&service.DesiredReplicas,
		&service.CPURequestMillis,
		&service.MemoryRequestMB,
		&service.CreatedAt,
		&service.UpdatedAt,
	)

	if err != nil {
		return model.Service{}, fmt.Errorf(
			"get service %q: %w",
			id,
			err,
		)
	}

	return service, nil
}

func (p *Postgres) UpdateService(
	ctx context.Context,
	service model.Service,
) error {
	const query = `
		UPDATE services
		SET
			image = $2,
			desired_replicas = $3,
			cpu_request_millis = $4,
			memory_request_mb = $5,
			updated_at = NOW()
		WHERE id = $1
	`

	result, err := p.db.ExecContext(
		ctx,
		query,
		service.ID,
		service.Image,
		service.DesiredReplicas,
		service.CPURequestMillis,
		service.MemoryRequestMB,
	)

	if err != nil {
		return fmt.Errorf("update service %q: %w", service.ID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check service update: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("service %q not found", service.ID)
	}

	return nil
}
