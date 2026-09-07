package model

import "time"

type NodeStatus string

const (
	NodeReady     NodeStatus = "READY"
	NodeUnhealthy NodeStatus = "UNHEALTHY"
	NodeDraining  NodeStatus = "DRAINING"
)

type Node struct {
	ID              string
	Address         string
	CPUCapacity     int
	MemoryCapacity  int
	CPUAllocated    int
	MemoryAllocated int
	Status          NodeStatus
	LastHeartbeat   time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
