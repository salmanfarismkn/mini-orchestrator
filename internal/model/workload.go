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

type ExecutionState string

const (
	ExecutionIdle     ExecutionState = "IDLE"
	ExecutionStarting ExecutionState = "STARTING"
	ExecutionCreated  ExecutionState = "CREATED"
	ExecutionRunning  ExecutionState = "RUNNING"
	ExecutionFailed   ExecutionState = "FAILED"
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
	ExecutionState    ExecutionState
	ExecutionError    string
	ResourceReserved  bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
