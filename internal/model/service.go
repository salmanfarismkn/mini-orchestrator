package model

import "time"

type DeploymentStatus string

const (
	DeploymentPending     DeploymentStatus = "PENDING"
	DeploymentProgressing DeploymentStatus = "PROGRESSING"
	DeploymentAvailable   DeploymentStatus = "AVAILABLE"
	DeploymentFailed      DeploymentStatus = "FAILED"
)

type Service struct {
	ID                string
	Name              string
	Image             string
	DesiredReplicas   int
	CPURequestMillis  int
	MemoryRequestMB   int
	DeploymentVersion int

	MaxSurge       int
	MaxUnavailable int

	DeploymentStatus    DeploymentStatus
	DeploymentStartedAt *time.Time

	Autoscaling             AutoscalingConfig
	AutoscalingLastScaledAt *time.Time
	Status                  ServiceStatus
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type AutoscalingConfig struct {
	Enabled     bool
	MinReplicas int
	MaxReplicas int
	TargetCPU   float64

	ScaleUpCooldownSeconds   int
	ScaleDownCooldownSeconds int
	RequiredObservations     int
	MaxScaleStep             int
}

type ServiceStatus string

const (
	ServiceActive   ServiceStatus = "ACTIVE"
	ServiceDeleting ServiceStatus = "DELETING"
	ServiceDeleted  ServiceStatus = "DELETED"
)
