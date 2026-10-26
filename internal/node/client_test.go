package node

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

type recordingTransport struct {
	scheme string
}

func (transport *recordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.scheme = request.URL.Scheme
	return nil, errors.New("test transport stopped request")
}

func TestNodeClientUsesHTTPS(t *testing.T) {
	transport := &recordingTransport{}
	client := NewClient(&http.Client{Transport: transport}, "test-token")

	if _, err := client.ListContainers(context.Background(), "worker.example:9001"); err == nil {
		t.Fatal("expected test transport to stop request")
	}
	if transport.scheme != "https" {
		t.Fatalf("request scheme = %q; want https", transport.scheme)
	}
}

func TestAgentRegistrationAndHeartbeatRejectPlainHTTP(t *testing.T) {
	requests := 0
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("unexpected request")
	})
	agent := &Agent{
		ID:              "test-node",
		ControlPlaneURL: "http://control-plane.test",
		Client:          &http.Client{Transport: transport},
		Token:           "test-only-token",
	}

	if err := agent.Register(context.Background()); err == nil {
		t.Fatal("registration accepted a plaintext control-plane URL")
	}
	if err := agent.Heartbeat(context.Background()); err == nil {
		t.Fatal("heartbeat accepted a plaintext control-plane URL")
	}
	if requests != 0 {
		t.Fatalf("plaintext URL sent %d HTTP request(s); want none", requests)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
