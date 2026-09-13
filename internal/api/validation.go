package api

import (
	"fmt"
	"strings"
)

func validateServiceRequest(
	req createServiceRequest,
) error {
	if strings.TrimSpace(req.ID) == "" {
		return fmt.Errorf("id is required")
	}

	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("name is required")
	}

	if strings.TrimSpace(req.Image) == "" {
		return fmt.Errorf("image is required")
	}

	if req.Replicas < 0 {
		return fmt.Errorf("replicas cannot be negative")
	}

	if req.CPURequestMillis <= 0 {
		return fmt.Errorf(
			"cpu_request_millis must be greater than zero",
		)
	}

	if req.MemoryRequestMB <= 0 {
		return fmt.Errorf(
			"memory_request_mb must be greater than zero",
		)
	}

	if req.MaxSurge < 0 {
		return fmt.Errorf("max_surge cannot be negative")
	}

	if req.MaxUnavailable < 0 {
		return fmt.Errorf(
			"max_unavailable cannot be negative",
		)
	}

	if req.MaxSurge == 0 &&
		req.MaxUnavailable == 0 &&
		req.Replicas > 0 {
		return fmt.Errorf(
			"max_surge and max_unavailable cannot both be zero",
		)
	}

	if req.Autoscaling != nil {
		if err := validateAutoscaling(
			*req.Autoscaling,
		); err != nil {
			return err
		}
	}

	return nil
}

func validateAutoscaling(
	req autoscalingRequest,
) error {
	if !req.Enabled {
		return nil
	}

	if req.MinReplicas < 1 {
		return fmt.Errorf(
			"autoscaling min_replicas must be at least 1",
		)
	}

	if req.MaxReplicas < req.MinReplicas {
		return fmt.Errorf(
			"autoscaling max_replicas must be >= min_replicas",
		)
	}

	if req.TargetCPU <= 0 || req.TargetCPU > 100 {
		return fmt.Errorf(
			"autoscaling target_cpu must be between 0 and 100",
		)
	}

	if req.ScaleUpCooldownSeconds < 0 {
		return fmt.Errorf(
			"scale_up_cooldown_seconds cannot be negative",
		)
	}

	if req.ScaleDownCooldownSeconds < 0 {
		return fmt.Errorf(
			"scale_down_cooldown_seconds cannot be negative",
		)
	}

	if req.RequiredObservations < 1 {
		return fmt.Errorf(
			"required_observations must be at least 1",
		)
	}

	if req.MaxScaleStep < 1 {
		return fmt.Errorf(
			"max_scale_step must be at least 1",
		)
	}

	return nil
}
