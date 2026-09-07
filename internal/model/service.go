package model

import "time"

type Service struct {
	ID               string
	Name             string
	Image            string
	DesiredReplicas  int
	CPURequestMillis int
	MemoryRequestMB  int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}
