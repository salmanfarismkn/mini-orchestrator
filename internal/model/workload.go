package model

import "time"

type WorkloadState string

const (
	WorkloadPending   WorkloadState = "PENDING"
	WorkloadScheduled WorkloadState = "SCHEDULED"
	WorkloadRunning   WorkloadState = "RUNNING"
	WorkloadFailed    WorkloadState = "FAILED"
	WorkloadStopped   WorkloadState = "STOPPED"
)

type Workload struct {
	ID                string
	ServiceID         string
	NodeID            *string
	ContainerID       *string
	Image             string
	CPURequestMillis  int
	MemoryRequestMB   int
	DeploymentVersion int
	DesiredState      WorkloadState
	ActualState       WorkloadState
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
