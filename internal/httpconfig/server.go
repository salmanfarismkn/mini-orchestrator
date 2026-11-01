package httpconfig

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 15 * time.Second
	WriteTimeout      = 30 * time.Second
	IdleTimeout       = 60 * time.Second
	MaxHeaderBytes    = 1 << 20
)

func Apply(server *http.Server) {
	server.ReadHeaderTimeout = ReadHeaderTimeout
	server.ReadTimeout = ReadTimeout
	server.WriteTimeout = WriteTimeout
	server.IdleTimeout = IdleTimeout
	server.MaxHeaderBytes = MaxHeaderBytes
}

const DefaultShutdownTimeout = 30 * time.Second

func ShutdownTimeoutFromEnv(getenv func(string) string) (time.Duration, error) {
	value := strings.TrimSpace(getenv("ORCHESTRATOR_SHUTDOWN_TIMEOUT"))
	if value == "" {
		return DefaultShutdownTimeout, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return 0, fmt.Errorf("ORCHESTRATOR_SHUTDOWN_TIMEOUT must be a positive number of seconds")
	}
	return time.Duration(seconds) * time.Second, nil
}

// ShutdownGracefully stops accepting requests, waits for active requests, and
// forces connection closure if the bounded wait expires.
func ShutdownGracefully(server *http.Server, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		if closeErr := server.Close(); closeErr != nil {
			return fmt.Errorf("graceful shutdown failed: %v; force close failed: %w", err, closeErr)
		}
		return fmt.Errorf("graceful shutdown timed out; remaining connections force-closed: %w", err)
	}
	return nil
}
