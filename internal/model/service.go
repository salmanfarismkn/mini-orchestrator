package model

import "time"

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

	CreatedAt time.Time
	UpdatedAt time.Time
}