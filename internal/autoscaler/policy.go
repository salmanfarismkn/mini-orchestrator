package autoscaler

import (
	"time"

	"mini-orchestrator/internal/model"
)

func cooldownActive(
	service model.Service,
	desired int,
	now time.Time,
) bool {
	if service.AutoscalingLastScaledAt == nil {
		return false
	}

	elapsed := now.Sub(
		*service.AutoscalingLastScaledAt,
	)

	if desired > service.DesiredReplicas {
		return elapsed <
			time.Duration(
				service.Autoscaling.ScaleUpCooldownSeconds,
			)*time.Second
	}

	if desired < service.DesiredReplicas {
		return elapsed <
			time.Duration(
				service.Autoscaling.ScaleDownCooldownSeconds,
			)*time.Second
	}

	return false
}

func limitScaleStep(
	current int,
	desired int,
	maxStep int,
) int {
	if maxStep <= 0 {
		return current
	}

	if desired > current+maxStep {
		return current + maxStep
	}

	if desired < current-maxStep {
		return current - maxStep
	}

	return desired
}

func stableObservation(
	observations map[string]observation,
	serviceID string,
	desired int,
	required int,
	now time.Time,
) bool {
	if required <= 1 {
		return true
	}

	current, ok := observations[serviceID]

	if !ok || current.desiredReplicas != desired {
		observations[serviceID] = observation{
			desiredReplicas: desired,
			count:           1,
			lastSeen:        now,
		}

		return false
	}

	current.count++
	current.lastSeen = now
	observations[serviceID] = current

	return current.count >= required
}
