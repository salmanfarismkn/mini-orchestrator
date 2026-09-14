package api

import (
    "encoding/json"
    "testing"
)

func TestCreateServiceRequestCompatibility(t *testing.T) {
    payload := []byte(`{
        "id": "my-service",
        "name": "my-service",
        "image": "nginx:alpine",
        "desired_replicas": 2,
        "cpu_request_millis": 500,
        "memory_request_mb": 128,
        "deployment_version": 1,
        "max_surge": 1,
        "max_unavailable": 0,
        "deployment_status": "Pending",
        "autoscaling_enabled": false,
        "autoscaling_min_replicas": 1,
        "autoscaling_max_replicas": 6,
        "autoscaling_target_cpu": 70,
        "autoscaling_scale_up_cooldown_seconds": 30,
        "autoscaling_scale_down_cooldown_seconds": 30,
        "autoscaling_required_observations": 3,
        "autoscaling_max_scale_step": 2
    }`)

    var req createServiceRequest
    if err := json.Unmarshal(payload, &req); err != nil {
        t.Fatalf("decode service request: %v", err)
    }

    if req.effectiveReplicas() != 2 {
        t.Fatalf("effectiveReplicas() = %d; want 2", req.effectiveReplicas())
    }

    if req.Autoscaling == nil && req.AutoscalingEnabled {
        t.Fatal("expected autoscaling compatibility fields to populate the autoscaling request")
    }
}
