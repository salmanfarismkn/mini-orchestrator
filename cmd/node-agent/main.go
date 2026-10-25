package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"

	"mini-orchestrator/internal/api"
	"mini-orchestrator/internal/httpconfig"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/runtime"
	"mini-orchestrator/internal/transporttls"
)

// runtimeAdapter adapts the Docker runtime implementation to the node runtime interface.
type runtimeAdapter struct {
	*runtime.DockerRuntime
}

type agentConfig struct {
	ID              string
	ListenAddress   string
	Address         string
	CPUCapacity     int
	MemoryCapacity  int
	ControlPlaneURL string
	Token           string
}

func loadAgentConfig(getenv func(string) string) (agentConfig, error) {
	config := agentConfig{
		ID:              envOrDefault(getenv, "ORCHESTRATOR_NODE_ID", "worker-01"),
		ListenAddress:   envOrDefault(getenv, "ORCHESTRATOR_LISTEN_ADDR", ":9001"),
		CPUCapacity:     4000,
		MemoryCapacity:  8192,
		Address:         strings.TrimSpace(getenv("ORCHESTRATOR_NODE_ADDRESS")),
		ControlPlaneURL: strings.TrimSpace(getenv("ORCHESTRATOR_CONTROL_PLANE_URL")),
		Token:           strings.TrimSpace(getenv("ORCHESTRATOR_NODE_TOKEN")),
	}
	if config.Token == "" {
		return agentConfig{}, fmt.Errorf("ORCHESTRATOR_NODE_TOKEN is required")
	}
	if config.Address == "" {
		return agentConfig{}, fmt.Errorf("ORCHESTRATOR_NODE_ADDRESS is required")
	}
	if err := validateAddress(config.ListenAddress, true); err != nil {
		return agentConfig{}, fmt.Errorf("ORCHESTRATOR_LISTEN_ADDR: %w", err)
	}
	if err := validateAddress(config.Address, false); err != nil {
		return agentConfig{}, fmt.Errorf("ORCHESTRATOR_NODE_ADDRESS: %w", err)
	}
	controlPlaneURL, err := url.Parse(config.ControlPlaneURL)
	if err != nil || controlPlaneURL.Scheme != "https" || controlPlaneURL.Host == "" {
		return agentConfig{}, fmt.Errorf("ORCHESTRATOR_CONTROL_PLANE_URL must be an HTTPS URL")
	}
	if strings.TrimSpace(config.ID) == "" {
		return agentConfig{}, fmt.Errorf("ORCHESTRATOR_NODE_ID must not be empty")
	}

	if value := strings.TrimSpace(getenv("ORCHESTRATOR_NODE_CPU_MILLIS")); value != "" {
		config.CPUCapacity, err = strconv.Atoi(value)
		if err != nil {
			return agentConfig{}, fmt.Errorf("ORCHESTRATOR_NODE_CPU_MILLIS must be an integer")
		}
	}
	if value := strings.TrimSpace(getenv("ORCHESTRATOR_NODE_MEMORY_MB")); value != "" {
		config.MemoryCapacity, err = strconv.Atoi(value)
		if err != nil {
			return agentConfig{}, fmt.Errorf("ORCHESTRATOR_NODE_MEMORY_MB must be an integer")
		}
	}
	if config.CPUCapacity <= 0 || config.MemoryCapacity <= 0 {
		return agentConfig{}, fmt.Errorf("node CPU and memory capacities must be greater than zero")
	}

	return config, nil
}

func envOrDefault(getenv func(string) string, name, fallback string) string {
	if value := strings.TrimSpace(getenv(name)); value != "" {
		return value
	}
	return fallback
}

func validateAddress(address string, allowEmptyHost bool) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("must be a host:port address: %w", err)
	}
	if host == "" && !allowEmptyHost {
		return fmt.Errorf("must include a reachable host; wildcard addresses cannot be advertised")
	}
	if parsedIP := net.ParseIP(host); parsedIP != nil && parsedIP.IsUnspecified() && !allowEmptyHost {
		return fmt.Errorf("must include a reachable host; wildcard addresses cannot be advertised")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("port must be an integer from 1 to 65535")
	}
	return nil
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

	config, err := loadAgentConfig(os.Getenv)
	if err != nil {
		slog.Error("node agent configuration is invalid", "error", err)
		os.Exit(1)
	}
	serverTLS, clientTLS, err := transporttls.LoadFromEnv()
	if err != nil {
		slog.Error("mutual TLS configuration failed", "error", err)
		os.Exit(1)
	}

	advertisedHost, _, _ := net.SplitHostPort(config.Address)
	if err := transporttls.ValidateServerName(serverTLS, advertisedHost); err != nil {
		slog.Error("node certificate does not match advertised address", "error", err)
		os.Exit(1)
	}

	dockerRuntime, err := runtime.NewDockerRuntime()
	if err != nil {
		slog.Error("create Docker runtime failed", "error", err)
		os.Exit(1)
	}

	agent := &node.Agent{
		ID:              config.ID,
		Address:         config.Address,
		CPUCapacity:     config.CPUCapacity,
		MemoryCapacity:  config.MemoryCapacity,
		ControlPlaneURL: config.ControlPlaneURL,
		Client: &http.Client{
			Transport: &http.Transport{TLSClientConfig: clientTLS},
			Timeout:   10 * time.Second,
		},
		Runtime: &runtimeAdapter{DockerRuntime: dockerRuntime},
		Token:   config.Token,
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
		Addr:      config.ListenAddress,
		Handler:   api.RequireBearerToken(config.Token, agent.Routes()),
		TLSConfig: serverTLS,
	}
	httpconfig.Apply(server)

	slog.Info(
		"node agent started",
		"node_id", agent.ID,
		"listen_address", config.ListenAddress,
		"advertised_address", agent.Address,
	)

	if err := server.ListenAndServeTLS("", ""); err != nil &&
		err != http.ErrServerClosed {
		slog.Error("node agent HTTP server failed", "node_id", agent.ID, "error", err)
		os.Exit(1)
	}
}
