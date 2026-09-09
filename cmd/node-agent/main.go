package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/runtime"
)

func main() {
	dockerRuntime, err := runtime.NewDockerRuntime()
	if err != nil {
		log.Fatalf("create docker runtime: %v", err)
	}

	agent := &node.Agent{
		ID:              "worker-01",
		Address:         "127.0.0.1:9001",
		CPUCapacity:     4000,
		MemoryCapacity:  8192,
		ControlPlaneURL: "http://localhost:8080",
		Runtime:         dockerRuntime,
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

	go agent.RunHeartbeatLoop(ctx)

	server := &http.Server{
		Addr:              ":9001",
		Handler:           agent.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("node agent listening on :9001")

	if err := server.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {
		log.Fatalf("node agent failed: %v", err)
	}
}
