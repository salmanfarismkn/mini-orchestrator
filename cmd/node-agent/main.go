package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"

	"mini-orchestrator/internal/api"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/runtime"
)

// runtimeAdapter adapts the Docker runtime implementation to the node runtime interface.
type runtimeAdapter struct {
	*runtime.DockerRuntime
}

// Stats adapts the Docker runtime Stats API to the runtime.Runtime interface.
func (a *runtimeAdapter) Stats(ctx context.Context, containerID string) (runtime.ContainerStats, error) {
	return a.DockerRuntime.Stats(ctx, containerID)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Warn(".env file not found")
	}

	nodeToken := os.Getenv("ORCHESTRATOR_NODE_TOKEN")
	if nodeToken == "" {
		slog.Error("node authentication configuration is missing")
		os.Exit(1)
	}

	dockerRuntime, err := runtime.NewDockerRuntime()
	if err != nil {
		slog.Error("create Docker runtime failed", "error", err)
		os.Exit(1)
	}

	agent := &node.Agent{
		ID:              "worker-01",
		Address:         "127.0.0.1:9001",
		CPUCapacity:     4000,
		MemoryCapacity:  8192,
		ControlPlaneURL: "http://localhost:8080",
		Runtime:         &runtimeAdapter{DockerRuntime: dockerRuntime},
		Token:           nodeToken,
	}

	ctx := context.Background()

	registerCtx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()

	if err := agent.Register(registerCtx); err != nil {
		slog.Error("node registration failed", "node_id", agent.ID, "error", err)
		os.Exit(1)
	}

	slog.Info(
		"node registered successfully",
		"node_id", agent.ID,
		"address", agent.Address,
	)

	go agent.RunHeartbeatLoop(ctx)

	server := &http.Server{
		Addr:    ":9001",
		Handler: api.RequireBearerToken(nodeToken, agent.Routes()),
	}

	slog.Info(
		"node agent started",
		"address", ":9001",
	)

	if err := server.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {
		slog.Error("node agent HTTP server failed", "node_id", agent.ID, "error", err)
		os.Exit(1)
	}
}
