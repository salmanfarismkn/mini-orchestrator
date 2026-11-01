package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"mini-orchestrator/internal/api"
	"mini-orchestrator/internal/autoscaler"
	"mini-orchestrator/internal/controller"
	"mini-orchestrator/internal/httpconfig"
	"mini-orchestrator/internal/metrics"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/observability"
	"mini-orchestrator/internal/reconciler"
	"mini-orchestrator/internal/scheduler"
	"mini-orchestrator/internal/store"
	"mini-orchestrator/internal/transporttls"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Warn(".env file not found")
	}

	dsn := os.Getenv("DATABASE_URL")

	if dsn == "" {
		slog.Error("database configuration is missing")
		os.Exit(1)
	}

	nodeToken := os.Getenv("ORCHESTRATOR_NODE_TOKEN")
	if nodeToken == "" {
		slog.Error("node authentication configuration is missing")
		os.Exit(1)
	}
	serverTLS, clientTLS, err := transporttls.LoadFromEnv()
	if err != nil {
		slog.Error("mutual TLS configuration failed", "error", err)
		os.Exit(1)
	}
	serverTLS = serverTLS.Clone()
	serverTLS.ClientAuth = tls.VerifyClientCertIfGiven

	dbCtx, cancelDB := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	db, err := store.NewPostgres(dbCtx, dsn)
	cancelDB()
	if err != nil {
		slog.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	apiMetrics := &observability.Metrics{}

	shutdownTimeout, err := httpconfig.ShutdownTimeoutFromEnv(os.Getenv)
	if err != nil {
		slog.Error("shutdown configuration is invalid", "error", err)
		os.Exit(1)
	}

	rootCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	appCtx, cancelApp := context.WithCancel(rootCtx)
	var backgroundWG sync.WaitGroup

	healthChecker := node.NewHealthChecker(
		db,
		5*time.Second,
		15*time.Second,
	)

	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()
		healthChecker.Run(appCtx)
	}()
	schedulerEngine := scheduler.New()

	schedulerService := scheduler.NewService(
		db,
		schedulerEngine,
	)
	nodeClient := node.NewClient(
		&http.Client{
			Transport: &http.Transport{TLSClientConfig: clientTLS},
			Timeout:   10 * time.Second,
		},
		nodeToken,
	)
	runtimeReconciler := reconciler.NewRuntimeReconciler(
		db,
		nodeClient,
		apiMetrics,
	)
	executor := reconciler.NewWorkloadExecutor(
		db,
		nodeClient,
	)
	replicaController := controller.NewReplicaController(db)
	deploymentController := controller.NewDeploymentController(db)
	deploymentStatus := controller.NewDeploymentStatusController(db, 2*time.Minute)

	reconciler := reconciler.New(
		db,
		replicaController,
		deploymentController,
		deploymentStatus,
		schedulerService,
		executor,
		runtimeReconciler,
		5*time.Second,
		apiMetrics,
	)
	metricsProvider := metrics.NewNodeProvider(
		db,
		nodeClient,
	)

	autoscalerSvc := autoscaler.New(
		db,
		metricsProvider,
	)

	autoscalerLoop := autoscaler.NewLoop(
		db,
		autoscalerSvc,
		30*time.Second,
	)

	backgroundWG.Add(2)
	go func() {
		defer backgroundWG.Done()
		autoscalerLoop.Run(appCtx)
	}()
	go func() {
		defer backgroundWG.Done()
		reconciler.Run(appCtx)
	}()

	server := api.NewServer(
		db,
		nodeToken,
		apiMetrics,
	)

	httpServer := &http.Server{
		Addr:      ":8080",
		Handler:   server.Routes(),
		TLSConfig: serverTLS,
	}
	httpconfig.Apply(httpServer)

	slog.Info("control plane started", "address", httpServer.Addr)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- httpServer.ListenAndServeTLS("", "")
	}()

	shutdownRequested := false
	var listenErr error
	select {
	case <-rootCtx.Done():
		shutdownRequested = true
		slog.Info("control plane shutdown started")
	case listenErr = <-serverErr:
		if listenErr != nil && listenErr != http.ErrServerClosed {
			slog.Error("control plane HTTP server failed", "error", listenErr)
		}
	}

	cancelApp()

	shutdownErr := httpconfig.ShutdownGracefully(httpServer, shutdownTimeout)
	if shutdownErr != nil {
		shutdownRequested = false
		slog.Error("control plane HTTP shutdown did not complete gracefully", "error", shutdownErr)
	}

	waitDone := make(chan struct{})
	go func() {
		backgroundWG.Wait()
		close(waitDone)
	}()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer waitCancel()
	backgroundStopped := false
	select {
	case <-waitDone:
		backgroundStopped = true
	case <-waitCtx.Done():
		shutdownRequested = false
		slog.Error("control plane background components did not stop within shutdown timeout")
	}

	if backgroundStopped {
		if err := db.Close(); err != nil {
			shutdownRequested = false
			slog.Error("control plane database close failed", "error", err)
		}
	}

	if shutdownRequested && listenErr == nil {
		slog.Info("control plane shutdown complete")
		return
	}
	if listenErr != nil && listenErr != http.ErrServerClosed {
		os.Exit(1)
	}
	if !shutdownRequested {
		return
	}
}
