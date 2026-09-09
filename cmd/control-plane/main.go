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
