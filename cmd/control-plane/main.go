package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"mini-orchestrator/internal/api"
	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/store"
	"mini-orchestrator/internal/controller"
	"mini-orchestrator/internal/reconciler"
	"mini-orchestrator/internal/scheduler"
	"mini-orchestrator/internal/autoscaler"
	"mini-orchestrator/internal/metrics"

)

func main() {
	dsn := os.Getenv("DATABASE_URL")

	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	db, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("database connection failed: %v", err)
	}
	defer db.Close()
	healthChecker := node.NewHealthChecker(
		db,
		5*time.Second,
		15*time.Second,
	)

	go healthChecker.Run(context.Background())
	schedulerEngine := scheduler.New()

	schedulerService := scheduler.NewService(
		db,
		schedulerEngine,
	)
	nodeClient := node.NewClient(nil)
	runtimeReconciler := reconciler.NewRuntimeReconciler(
		db,
		nodeClient,
	)
	executor := reconciler.NewWorkloadExecutor(
		db,
		nodeClient,
	)
	replicaController := controller.NewReplicaController(db)
	deploymentController := controller.NewDeploymentController(db)
	deploymentStatus := controller.NewDeploymentStatusController(db,2*time.Minute,)

	reconciler := reconciler.New(
		db,
		replicaController,
		deploymentController,
		deploymentStatus,
		schedulerService,
		executor,
		runtimeReconciler,
		5*time.Second,
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

	go autoscalerLoop.Run(ctx)
	go reconciler.Run(ctx)
	
	server := api.NewServer(db)

	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("control plane listening on :8080")

	if err := httpServer.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
