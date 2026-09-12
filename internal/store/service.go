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
    _, err := p.db.ExecContext(ctx, `
        INSERT INTO services (
            id,
            name,
            image,
            desired_replicas,
            cpu_request_millis,
            memory_request_mb,
            deployment_version,
            max_surge,
            max_unavailable,
            created_at,
            updated_at
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
        )
    `,
        service.ID,
        service.Name,
        service.Image,
        service.DesiredReplicas,
        service.CPURequestMillis,
        service.MemoryRequestMB,
        service.DeploymentVersion,
        service.MaxSurge,
        service.MaxUnavailable,
        service.CreatedAt,
        service.UpdatedAt,
    )

	if err != nil {
		return fmt.Errorf("create service: %w", err)
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
            deployment_version,
            max_surge,
            max_unavailable,
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
        &service.DeploymentVersion,
        &service.MaxSurge,
        &service.MaxUnavailable,
        &service.CreatedAt,
        &service.UpdatedAt,
    )

    if err != nil {
        return model.Service{}, fmt.Errorf("get service %q: %w", id, err)
    }

    return service, nil
}

func (p *Postgres) UpdateService(
	ctx context.Context,
	service model.Service,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE services
		SET
			name = $1,
			image = $2,
			desired_replicas = $3,
			cpu_request_millis = $4,
			memory_request_mb = $5,
			deployment_version =
				CASE
					WHEN image <> $2
					THEN deployment_version + 1
					ELSE deployment_version
				END,
			updated_at = NOW()
		WHERE id = $6
	`,
		service.Name,
		service.Image,
		service.DesiredReplicas,
		service.CPURequestMillis,
		service.MemoryRequestMB,
		service.ID,
	)

	if err != nil {
		return fmt.Errorf("update service: %w", err)
	}

	return nil
}

func (p *Postgres) ListServices(ctx context.Context) ([]model.Service, error) {
    rows, err := p.db.QueryContext(ctx, `
        SELECT
            id,
            name,
            image,
            desired_replicas,
            cpu_request_millis,
            memory_request_mb,
            deployment_version,
            max_surge,
            max_unavailable,
            created_at,
            updated_at
        FROM services
        ORDER BY created_at
    `)
    if err != nil {
        return nil, fmt.Errorf("list services: %w", err)
    }
    defer rows.Close()

    var services []model.Service

    for rows.Next() {
        var service model.Service

        if err := rows.Scan(
            &service.ID,
            &service.Name,
            &service.Image,
            &service.DesiredReplicas,
            &service.CPURequestMillis,
            &service.MemoryRequestMB,
            &service.DeploymentVersion,
            &service.MaxSurge,
            &service.MaxUnavailable,
            &service.CreatedAt,
            &service.UpdatedAt,
        ); err != nil {
            return nil, fmt.Errorf("scan service: %w", err)
        }

        services = append(services, service)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("iterate services: %w", err)
    }

    return services, nil
}
