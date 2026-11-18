package api

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestRequireBearerToken(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		wantStatus int
		wantCalled bool
	}{
		{name: "missing token", wantStatus: http.StatusUnauthorized},
		{name: "incorrect token", header: "Bearer wrong", wantStatus: http.StatusUnauthorized},
		{name: "correct token", header: "Bearer secret", wantStatus: http.StatusNoContent, wantCalled: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			handler := RequireBearerToken("secret", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			if test.header != "" {
				request.Header.Set("Authorization", test.header)
			}
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d; want %d", response.Code, test.wantStatus)
			}
			if called != test.wantCalled {
				t.Fatalf("handler called = %t; want %t", called, test.wantCalled)
			}
		})
	}
}

func TestRequireBearerTokenRejectsMissingToken(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := RequireBearerToken("test-secret", next)
	req := httptest.NewRequest(http.MethodGet, "/containers", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireBearerTokenFailsClosedWhenExpectedTokenIsEmpty(t *testing.T) {
	called := false
	handler := RequireBearerToken("", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer any-value")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusServiceUnavailable)
	}
	if called {
		t.Fatal("handler was called with no configured bearer token")
	}
}

func TestParseNodeIdentity(t *testing.T) {
	tests := []struct {
		name     string
		identity string
		wantID   string
		wantErr  error
	}{
		{name: "valid", identity: "spiffe://mini-orchestrator/node/worker-01", wantID: "worker-01"},
		{name: "missing URI", identity: "", wantErr: ErrMalformedNodeIdentity},
		{name: "wrong scheme", identity: "https://mini-orchestrator/node/worker-01", wantErr: ErrMalformedNodeIdentity},
		{name: "wrong trust domain", identity: "spiffe://example.org/node/worker-01", wantErr: ErrMalformedNodeIdentity},
		{name: "missing node path", identity: "spiffe://mini-orchestrator/service/worker-01", wantErr: ErrMalformedNodeIdentity},
		{name: "nested node path", identity: "spiffe://mini-orchestrator/node/team/worker-01", wantErr: ErrMalformedNodeIdentity},
		{name: "invalid node character", identity: "spiffe://mini-orchestrator/node/worker 01", wantErr: ErrMalformedNodeIdentity},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotID, err := parseNodeIdentity(test.identity)
			if test.wantErr != nil {
				if err != test.wantErr {
					t.Fatalf("error = %v; want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseNodeIdentity: %v", err)
			}
			if gotID != test.wantID {
				t.Fatalf("node ID = %q; want %q", gotID, test.wantID)
			}
		})
	}
}

func TestAuthenticatedNodeIdentity(t *testing.T) {
	uri := func(value string) *url.URL {
		parsed, err := url.Parse(value)
		if err != nil {
			t.Fatalf("parse URI: %v", err)
		}
		return parsed
	}

	missingLeaf := &x509.Certificate{}
	duplicateLeaf := &x509.Certificate{URIs: []*url.URL{uri("spiffe://mini-orchestrator/node/worker-01"), uri("spiffe://mini-orchestrator/node/worker-02")}}
	validLeaf := &x509.Certificate{URIs: []*url.URL{uri("spiffe://mini-orchestrator/node/worker-01")}}

	tests := []struct {
		name     string
		tlsState *tls.ConnectionState
		wantID   string
		wantErr  error
	}{
		{name: "unverified TLS", tlsState: &tls.ConnectionState{}, wantErr: ErrUnverifiedNodeCertificate},
		{name: "missing URI SAN", tlsState: &tls.ConnectionState{PeerCertificates: []*x509.Certificate{missingLeaf}, VerifiedChains: [][]*x509.Certificate{{missingLeaf}}}, wantErr: ErrMissingNodeIdentity},
		{name: "duplicate URI SAN", tlsState: &tls.ConnectionState{PeerCertificates: []*x509.Certificate{duplicateLeaf}, VerifiedChains: [][]*x509.Certificate{{duplicateLeaf}}}, wantErr: ErrDuplicateNodeIdentity},
		{name: "valid identity", tlsState: &tls.ConnectionState{PeerCertificates: []*x509.Certificate{validLeaf}, VerifiedChains: [][]*x509.Certificate{{validLeaf}}}, wantID: "worker-01"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/nodes/heartbeat", nil)
			request.TLS = test.tlsState
			gotID, err := authenticatedNodeIdentity(request)
			if test.wantErr != nil {
				if err != test.wantErr {
					t.Fatalf("error = %v; want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("authenticatedNodeIdentity: %v", err)
			}
			if gotID != test.wantID {
				t.Fatalf("node ID = %q; want %q", gotID, test.wantID)
			}
		})
	}
}

func TestRequireMutualTLS(t *testing.T) {
	tests := []struct {
		name     string
		tlsState *tls.ConnectionState
		wantCode int
	}{
		{name: "missing TLS", wantCode: http.StatusForbidden},
		{name: "unverified client", tlsState: &tls.ConnectionState{}, wantCode: http.StatusForbidden},
		{name: "verified client", tlsState: &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}, wantCode: http.StatusNoContent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := RequireMutualTLS(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.TLS = test.tlsState
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantCode {
				t.Fatalf("status = %d; want %d", response.Code, test.wantCode)
			}
		})
	}
}
