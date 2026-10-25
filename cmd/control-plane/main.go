package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"os"
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
	defer db.Close()

	apiMetrics := &observability.Metrics{}

	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	healthChecker := node.NewHealthChecker(
		db,
		5*time.Second,
		15*time.Second,
	)

	go healthChecker.Run(appCtx)
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

	go autoscalerLoop.Run(appCtx)
	go reconciler.Run(appCtx)

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

	if err := httpServer.ListenAndServeTLS("", ""); err != nil &&
		err != http.ErrServerClosed {
		slog.Error("control plane HTTP server failed", "error", err)
		os.Exit(1)
	}
}
