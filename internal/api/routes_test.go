package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type readinessFunc func(context.Context) error

func (f readinessFunc) Ping(ctx context.Context) error {
	return f(ctx)
}

func TestServiceManagementRoutesRequireBearerToken(t *testing.T) {
	handler := (&Server{nodeToken: "route-test-token"}).Routes()
	for _, route := range []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodPost, path: "/services", body: `{}`},
		{method: http.MethodGet, path: "/services/demo"},
		{method: http.MethodPut, path: "/services/demo", body: `{}`},
		{method: http.MethodDelete, path: "/services/demo"},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			request := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d; want %d", response.Code, http.StatusUnauthorized)
			}
		})
	}
}

func TestServiceRouteAuthorizedRequestsReachHandler(t *testing.T) {
	handler := (&Server{nodeToken: "route-test-token"}).Routes()
	request := httptest.NewRequest(http.MethodPost, "/services", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer route-test-token")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d after authorized malformed request", response.Code, http.StatusBadRequest)
	}
	if !strings.Contains(response.Body.String(), "id is required") {
		t.Fatalf("response = %q; want service validation error", response.Body.String())
	}
}

func TestServiceManagementRoutesFailClosedWithoutConfiguredToken(t *testing.T) {
	handler := (&Server{}).Routes()
	request := httptest.NewRequest(http.MethodGet, "/services/demo", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestHealthAndReadinessRoutes(t *testing.T) {
	var readinessDeadline time.Time
	server := &Server{
		readiness: readinessFunc(func(ctx context.Context) error {
			readinessDeadline, _ = ctx.Deadline()
			return errors.New("database unavailable")
		}),
	}
	handler := server.Routes()

	healthResponse := httptest.NewRecorder()
	handler.ServeHTTP(healthResponse, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if healthResponse.Code != http.StatusOK {
		t.Fatalf("health status = %d; want %d", healthResponse.Code, http.StatusOK)
	}

	readyResponse := httptest.NewRecorder()
	handler.ServeHTTP(readyResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d; want %d", readyResponse.Code, http.StatusServiceUnavailable)
	}
	if readinessDeadline.IsZero() || time.Until(readinessDeadline) > 2*time.Second {
		t.Fatalf("readiness deadline = %v; want a bounded DB check", readinessDeadline)
	}
}
