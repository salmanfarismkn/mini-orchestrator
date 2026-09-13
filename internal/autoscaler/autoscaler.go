package autoscaler

import (
	"context"
	"fmt"
	"math"
	"time"

	"mini-orchestrator/internal/metrics"
	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type Autoscaler struct {
	store   *store.Postgres
	metrics metrics.Provider

	observations map[string]observation
}

func New(
	store *store.Postgres,
	metricsProvider metrics.Provider,
) *Autoscaler {
	return &Autoscaler{
		store:        store,
		metrics:      metricsProvider,
		observations: make(map[string]observation),
	}
}

func (a *Autoscaler) ReconcileService(
	ctx context.Context,
	service model.Service,
) error {
	
	a.cleanupObservations(time.Now())

	config := service.Autoscaling

	if service.DeploymentStatus ==
		model.DeploymentProgressing {
		return nil
	}
	
	if !config.Enabled {
		return nil
	}

	if config.TargetCPU <= 0 {
		return nil
	}

	workloads, err := a.store.ListWorkloadsByService(
		ctx,
		service.ID,
	)
	if err != nil {
		return fmt.Errorf(
			"list workloads: %w",
			err,
		)
	}

	var totalUsage float64
	running := 0

	for _, workload := range workloads {
		if workload.DeploymentVersion !=
			service.DeploymentVersion {
			continue
		}

		if workload.ActualState != model.WorkloadRunning {
			continue
		}

		stats, err := a.metrics.GetWorkloadMetrics(
			ctx,
			workload.ID,
		)
		if err != nil {
			continue
		}

		totalUsage += float64(stats.CPUUsageMillis)
		running++
	}

	if running == 0 {
		return nil
	}

	targetPerReplica :=
		float64(service.CPURequestMillis) *
			config.TargetCPU / 100

	if targetPerReplica <= 0 {
		return nil
	}

	desired := int(math.Ceil(
		totalUsage / targetPerReplica,
	))

	if desired < config.MinReplicas {
		desired = config.MinReplicas
	}

	if desired > config.MaxReplicas {
		desired = config.MaxReplicas
	}

	if desired == service.DesiredReplicas {
		return nil
	}

	now := time.Now()

	desired = limitScaleStep(
		service.DesiredReplicas,
		desired,
		config.MaxScaleStep,
	)

	if desired == service.DesiredReplicas {
		return nil
	}

	if cooldownActive(
		service,
		desired,
		now,
	) {
		return nil
	}

	if !stableObservation(
		a.observations,
		service.ID,
		desired,
		config.RequiredObservations,
		now,
	) {
		return nil
	}
	return a.store.UpdateDesiredReplicas(
		ctx,
		service.ID,
		desired,
	)
}

func (a *Autoscaler) cleanupObservations(
	now time.Time,
) {
	for serviceID, observation := range a.observations {
		if now.Sub(observation.lastSeen) > 10*time.Minute {
			delete(a.observations, serviceID)
		}
	}
}