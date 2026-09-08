package main

import (
	"context"
	"log"
	"time"

	"mini-orchestrator/internal/node"
)

func main() {
	agent := &node.Agent{
		ID:              "worker-01",
		Address:         "127.0.0.1:9001",
		CPUCapacity:     4000,
		MemoryCapacity:  8192,
		ControlPlaneURL: "http://localhost:8080",
	}

	ctx := context.Background()

	registerCtx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()

	if err := agent.Register(registerCtx); err != nil {
		log.Fatalf("node registration failed: %v", err)
	}

	log.Printf("node %s registered successfully", agent.ID)

	agent.RunHeartbeatLoop(ctx)
}