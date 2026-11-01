package node

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAgentRegistrationStoresGenerationAndRegistrationID(t *testing.T) {
	var registration registrationRequest
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nodes/register" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&registration); err != nil {
			t.Fatalf("decode registration: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"lease_generation": int64(7)})
	}))
	defer server.Close()

	agent := &Agent{
		ID:              "worker-01",
		Address:         "127.0.0.1:9001",
		CPUCapacity:     1000,
		MemoryCapacity:  512,
		ControlPlaneURL: server.URL,
		Client:          server.Client(),
		Token:           "secret",
	}
	if err := agent.Register(context.Background()); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if registration.RegistrationID == "" {
		t.Fatal("registration id was not generated")
	}
	if agent.RegistrationID != registration.RegistrationID {
		t.Fatal("agent did not retain registration id")
	}
	if agent.LeaseGeneration != 7 {
		t.Fatalf("lease generation = %d; want 7", agent.LeaseGeneration)
	}
}

func TestAgentHeartbeatSendsGeneration(t *testing.T) {
	var heartbeat heartbeatRequest
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	agent := &Agent{
		ID:              "worker-01",
		ControlPlaneURL: server.URL,
		Client:          server.Client(),
		Token:           "secret",
		LeaseGeneration: 4,
	}
	if err := agent.Heartbeat(context.Background()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if heartbeat.LeaseGeneration != 4 {
		t.Fatalf("heartbeat generation = %d; want 4", heartbeat.LeaseGeneration)
	}
}

func TestAgentHeartbeatLoopStopsWhenContextCanceled(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	agent := &Agent{
		ID:              "worker-01",
		ControlPlaneURL: server.URL,
		Client:          server.Client(),
		Token:           "secret",
		LeaseGeneration: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		agent.RunHeartbeatLoop(ctx)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not stop after context cancellation")
	}
}

func TestAgentStaleHeartbeatFencesProcess(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	agent := &Agent{
		ID:              "worker-01",
		ControlPlaneURL: server.URL,
		Client:          server.Client(),
		Token:           "secret",
		LeaseGeneration: 3,
	}
	if err := agent.Heartbeat(context.Background()); err == nil {
		t.Fatal("stale heartbeat unexpectedly succeeded")
	}
	agent.leaseMu.RLock()
	fenced := agent.Fenced
	agent.leaseMu.RUnlock()
	if !fenced {
		t.Fatal("stale heartbeat did not fence agent")
	}
	if err := agent.Register(context.Background()); err == nil {
		t.Fatal("fenced agent unexpectedly re-registered")
	}
}
