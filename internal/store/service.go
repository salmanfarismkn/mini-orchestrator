package store

import (
	"context"
	"fmt"
	"time"

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
            deployment_status,
            deployment_started_at,
            autoscaling_enabled,
            autoscaling_min_replicas,
            autoscaling_max_replicas,
            autoscaling_target_cpu,
			autoscaling_scale_up_cooldown_seconds,
			autoscaling_scale_down_cooldown_seconds,
			autoscaling_required_observations,
			autoscaling_max_scale_step,
			autoscaling_last_scaled_at,
			status,
            created_at,
            updated_at
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23
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
		&service.DeploymentStatus,
		&service.DeploymentStartedAt,
		service.Autoscaling.Enabled,
		service.Autoscaling.MinReplicas,
		service.Autoscaling.MaxReplicas,
		service.Autoscaling.TargetCPU,
		service.Autoscaling.ScaleUpCooldownSeconds,
		service.Autoscaling.ScaleDownCooldownSeconds,
		service.Autoscaling.RequiredObservations,
		service.Autoscaling.MaxScaleStep,
		service.AutoscalingLastScaledAt,
		service.Status,
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
            deployment_status,
            deployment_started_at,
            autoscaling_enabled,
            autoscaling_min_replicas,
            autoscaling_max_replicas,
            autoscaling_target_cpu,
			autoscaling_scale_up_cooldown_seconds,
			autoscaling_scale_down_cooldown_seconds,
			autoscaling_required_observations,
			autoscaling_max_scale_step,
			autoscaling_last_scaled_at,
			status,
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
		&service.DeploymentStatus,
		&service.DeploymentStartedAt,
		&service.Autoscaling.Enabled,
		&service.Autoscaling.MinReplicas,
		&service.Autoscaling.MaxReplicas,
		&service.Autoscaling.TargetCPU,
		&service.Autoscaling.ScaleUpCooldownSeconds,
		&service.Autoscaling.ScaleDownCooldownSeconds,
		&service.Autoscaling.RequiredObservations,
		&service.Autoscaling.MaxScaleStep,
		&service.AutoscalingLastScaledAt,
		&service.Status,
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
            deployment_status,
            deployment_started_at,
            autoscaling_enabled,
            autoscaling_min_replicas,
            autoscaling_max_replicas,
            autoscaling_target_cpu,
			autoscaling_scale_up_cooldown_seconds,
			autoscaling_scale_down_cooldown_seconds,
			autoscaling_required_observations,
			autoscaling_max_scale_step,
			autoscaling_last_scaled_at,
			status,
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
			&service.DeploymentStatus,
			&service.DeploymentStartedAt,
			&service.Autoscaling.Enabled,
			&service.Autoscaling.MinReplicas,
			&service.Autoscaling.MaxReplicas,
			&service.Autoscaling.TargetCPU,
			&service.Autoscaling.ScaleUpCooldownSeconds,
			&service.Autoscaling.ScaleDownCooldownSeconds,
			&service.Autoscaling.RequiredObservations,
			&service.Autoscaling.MaxScaleStep,
			&service.AutoscalingLastScaledAt,
			&service.Status,
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

func (p *Postgres) StartDeployment(
	ctx context.Context,
	serviceID string,
	version int,
	startedAt time.Time,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE services
		SET
			deployment_version = $1,
			deployment_status = $2,
			deployment_started_at = $3,
			updated_at = NOW()
		WHERE id = $4
	`,
		version,
		model.DeploymentProgressing,
		startedAt,
		serviceID,
	)

	if err != nil {
		return fmt.Errorf("start deployment: %w", err)
	}

	return nil
}

func (p *Postgres) UpdateDeploymentStatus(
	ctx context.Context,
	serviceID string,
	status model.DeploymentStatus,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE services
		SET
			deployment_status = $1,
			updated_at = NOW()
		WHERE id = $2
	`,
		status,
		serviceID,
	)

	if err != nil {
		return fmt.Errorf(
			"update deployment status: %w",
			err,
		)
	}

	return nil
}

func (p *Postgres) UpdateDesiredReplicas(
	ctx context.Context,
	serviceID string,
	replicas int,
) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE services
		SET
			desired_replicas = $1,
			autoscaling_last_scaled_at = NOW(),
			updated_at = NOW()
		WHERE id = $2
	`,
		replicas,
		serviceID,
	)

	if err != nil {
		return fmt.Errorf(
			"update desired replicas: %w",
			err,
		)
	}

	return nil
}

func (p *Postgres) MarkServiceDeleting(
	ctx context.Context,
	serviceID string,
) error {
	result, err := p.db.ExecContext(
		ctx,
		`
		UPDATE services
		SET status = 'DELETING',
		    updated_at = NOW()
		WHERE id = $1
		  AND status = 'ACTIVE'
		`,
		serviceID,
	)
	if err != nil {
		return fmt.Errorf("mark service deleting: %w", err)
	}

	if rows, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("check service deletion rows: %w", err)
	} else if rows == 0 {
		// Treat an already-deleting/deleted service as idempotent.
		var exists bool
		err := p.db.QueryRowContext(
			ctx,
			`SELECT EXISTS(SELECT 1 FROM services WHERE id = $1)`,
			serviceID,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check service existence: %w", err)
		}
		if !exists {
			return fmt.Errorf("service not found")
		}
	}

	return nil
}

func (p *Postgres) MarkServiceDeleted(
	ctx context.Context,
	serviceID string,
) error {
	result, err := p.db.ExecContext(
		ctx,
		`
		UPDATE services
		SET status = 'DELETED',
		    updated_at = NOW()
		WHERE id = $1
		  AND status = 'DELETING'
		`,
		serviceID,
	)
	if err != nil {
		return fmt.Errorf("mark service deleted: %w", err)
	}

	if rows, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("check service deleted rows: %w", err)
	} else if rows == 0 {
		return fmt.Errorf("service is not deleting")
	}

	return nil
}
