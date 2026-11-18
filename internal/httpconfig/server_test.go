package httpconfig

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestApplySetsBoundedServerLimits(t *testing.T) {
	server := &http.Server{}
	Apply(server)
	if server.ReadHeaderTimeout != ReadHeaderTimeout ||
		server.ReadTimeout != ReadTimeout ||
		server.WriteTimeout != WriteTimeout ||
		server.IdleTimeout != IdleTimeout ||
		server.MaxHeaderBytes != MaxHeaderBytes {
		t.Fatalf("server limits were not fully applied: %+v", server)
	}
}

func TestShutdownTimeoutFromEnv(t *testing.T) {
	timeout, err := ShutdownTimeoutFromEnv(func(string) string { return "12" })
	if err != nil || timeout != 12*time.Second {
		t.Fatalf("timeout = %v, err = %v", timeout, err)
	}
	if _, err := ShutdownTimeoutFromEnv(func(string) string { return "0" }); err == nil {
		t.Fatal("zero shutdown timeout was accepted")
	}
}

func TestShutdownStopsAcceptingNewRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := ShutdownGracefully(server.Config, time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	client := server.Client()
	if _, err := client.Get(server.URL); err == nil {
		server.Close()
		t.Fatal("new request succeeded after shutdown")
	}
}

func TestShutdownAllowsInFlightRequestToFinish(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))

	requestDone := make(chan error, 1)
	go func() {
		resp, err := server.Client().Get(server.URL)
		if err == nil {
			resp.Body.Close()
		}
		requestDone <- err
	}()
	<-started

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- ShutdownGracefully(server.Config, time.Second)
	}()

	select {
	case err := <-shutdownDone:
		server.Close()
		t.Fatalf("shutdown completed before in-flight request was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)

	if err := <-shutdownDone; err != nil {
		server.Close()
		t.Fatalf("graceful shutdown: %v", err)
	}
	if err := <-requestDone; err != nil {
		server.Close()
		t.Fatalf("in-flight request: %v", err)
	}
}

func TestShutdownTimeoutForceCloses(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))

	go server.Client().Get(server.URL)
	<-started
	if err := ShutdownGracefully(server.Config, 20*time.Millisecond); err == nil {
		server.Close()
		t.Fatal("forced shutdown timeout was not reported")
	}
	server.Close()
}
