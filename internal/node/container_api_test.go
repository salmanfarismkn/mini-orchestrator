package node

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"mini-orchestrator/internal/runtime"
)

func TestContainerIDFromPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{name: "start route", path: "/containers/abc123/start", want: "abc123"},
		{name: "inspect route", path: "/containers/abc123", want: "abc123"},
		{name: "list route", path: "/containers", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{URL: &url.URL{Path: tc.path}}
			if got := containerIDFromPath(r); got != tc.want {
				t.Fatalf("containerIDFromPath(%q) = %q; want %q", tc.path, got, tc.want)
			}
		})
	}
}

func TestAgentRejectsPlaintextControlPlaneURL(t *testing.T) {
	agent := &Agent{ControlPlaneURL: "http://control-plane"}
	if err := agent.Register(context.Background()); err == nil {
		t.Fatal("registration succeeded with a plaintext control-plane URL")
	}
	if err := agent.Heartbeat(context.Background()); err == nil {
		t.Fatal("heartbeat succeeded with a plaintext control-plane URL")
	}
}

func TestCreateContainerRejectsOversizedBody(t *testing.T) {
	agent := &Agent{}
	payload := append([]byte(`{"name":"`), bytes.Repeat([]byte("x"), 1<<20)...)
	payload = append(payload, []byte(`"}`)...)
	body := bytes.NewReader(payload)
	request := httptest.NewRequest(http.MethodPost, "/containers", body)
	response := httptest.NewRecorder()

	agent.CreateContainer(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusBadRequest)
	}
}

type failingRuntime struct {
	err error
}

func (r failingRuntime) Create(context.Context, runtime.ContainerConfig) (runtime.Container, error) {
	return runtime.Container{}, r.err
}

func (r failingRuntime) Start(context.Context, string) error { return r.err }
func (r failingRuntime) Stop(context.Context, string) error  { return r.err }
func (r failingRuntime) Remove(context.Context, string) error {
	return r.err
}
func (r failingRuntime) Inspect(context.Context, string) (runtime.Container, error) {
	return runtime.Container{}, r.err
}
func (r failingRuntime) List(context.Context) ([]runtime.Container, error) {
	return nil, r.err
}
func (r failingRuntime) Stats(context.Context, string) (runtime.ContainerStats, error) {
	return runtime.ContainerStats{}, r.err
}

func TestStopAndRemoveTreatMissingContainerAsIdempotent(t *testing.T) {
	for _, route := range []struct {
		name    string
		method  string
		handler func(*Agent, http.ResponseWriter, *http.Request)
	}{
		{name: "stop", method: http.MethodPost, handler: (*Agent).StopContainer},
		{name: "remove", method: http.MethodDelete, handler: (*Agent).RemoveContainer},
	} {
		t.Run(route.name, func(t *testing.T) {
			agent := &Agent{Runtime: failingRuntime{err: runtime.ErrContainerNotFound}}
			request := httptest.NewRequest(route.method, "/containers/missing", nil)
			response := httptest.NewRecorder()
			route.handler(agent, response, request)
			if response.Code != http.StatusNoContent {
				t.Fatalf("status = %d; want %d", response.Code, http.StatusNoContent)
			}
		})
	}
}

func TestStopDoesNotHideNonNotFoundRuntimeErrors(t *testing.T) {
	agent := &Agent{Runtime: failingRuntime{err: errors.New("docker daemon unavailable")}}
	request := httptest.NewRequest(http.MethodPost, "/containers/id/stop", nil)
	response := httptest.NewRecorder()
	agent.StopContainer(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusInternalServerError)
	}

	request = httptest.NewRequest(http.MethodGet, "/containers/id", nil)
	response = httptest.NewRecorder()
	agent.InspectContainer(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("inspect status = %d; want %d", response.Code, http.StatusInternalServerError)
	}
}
