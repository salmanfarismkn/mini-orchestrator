package node

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"mini-orchestrator/internal/api"
	"mini-orchestrator/internal/runtime"
	"mini-orchestrator/internal/transporttls"
)

func TestAgentRoutesRequireControlPlaneIdentityAndLocalTarget(t *testing.T) {
	fixture := newAgentTLSFixture(t)
	agent := &Agent{ID: "worker-01", Runtime: authorizationTestRuntime{}}
	server := httptest.NewUnstartedServer(api.RequireBearerToken("node-token", agent.Routes()))
	server.TLS = fixture.serverTLS
	server.StartTLS()
	t.Cleanup(server.Close)

	tests := []struct {
		name       string
		clientTLS  *tls.Config
		bearer     string
		targetNode string
		wantStatus int
		wantError  bool
	}{
		{name: "control plane", clientTLS: fixture.clients[transporttls.ControlPlaneCertificateIdentity], bearer: "node-token", targetNode: "worker-01", wantStatus: http.StatusOK},
		{name: "node certificate cannot call peer agent", clientTLS: fixture.clients[transporttls.NodeCertificateIdentity("worker-02")], bearer: "node-token", targetNode: "worker-01", wantStatus: http.StatusForbidden},
		{name: "wrong target node", clientTLS: fixture.clients[transporttls.ControlPlaneCertificateIdentity], bearer: "node-token", targetNode: "worker-02", wantStatus: http.StatusForbidden},
		{name: "missing target node", clientTLS: fixture.clients[transporttls.ControlPlaneCertificateIdentity], bearer: "node-token", wantStatus: http.StatusForbidden},
		{name: "certificate without identity", clientTLS: fixture.clients[""], bearer: "node-token", targetNode: "worker-01", wantStatus: http.StatusForbidden},
		{name: "malformed certificate identity", clientTLS: fixture.clients["malformed"], bearer: "node-token", targetNode: "worker-01", wantStatus: http.StatusForbidden},
		{name: "missing bearer", clientTLS: fixture.clients[transporttls.ControlPlaneCertificateIdentity], targetNode: "worker-01", wantStatus: http.StatusUnauthorized},
		{name: "invalid bearer", clientTLS: fixture.clients[transporttls.ControlPlaneCertificateIdentity], bearer: "wrong-token", targetNode: "worker-01", wantStatus: http.StatusUnauthorized},
		{name: "untrusted certificate", clientTLS: fixture.untrustedClient, bearer: "node-token", targetNode: "worker-01", wantError: true},
		{name: "missing certificate", clientTLS: nil, bearer: "node-token", targetNode: "worker-01", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{
				Transport: &http.Transport{TLSClientConfig: test.clientTLS},
				Timeout:   3 * time.Second,
			}
			request, err := http.NewRequest(http.MethodGet, server.URL+"/containers", nil)
			if err != nil {
				t.Fatalf("create request: %v", err)
			}
			if test.bearer != "" {
				request.Header.Set("Authorization", "Bearer "+test.bearer)
			}
			if test.targetNode != "" {
				request.Header.Set(targetNodeHeader, test.targetNode)
			}
			response, err := client.Do(request)
			if test.wantError {
				if err == nil {
					response.Body.Close()
					t.Fatal("request succeeded; want TLS handshake failure")
				}
				return
			}
			if err != nil {
				t.Fatalf("send request: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d; want %d", response.StatusCode, test.wantStatus)
			}
		})
	}
}

func TestNodeClientVerifiesTargetAgentCertificateBeforeSendingCommand(t *testing.T) {
	fixture := newAgentTLSFixture(t)
	agent := &Agent{ID: "worker-02", Runtime: authorizationTestRuntime{}}
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		api.RequireBearerToken("node-token", agent.Routes()).ServeHTTP(w, r)
	})
	server := httptest.NewUnstartedServer(handler)
	server.TLS = fixture.nodeServerTLS["worker-02"]
	server.StartTLS()
	t.Cleanup(server.Close)

	client := NewClient(&http.Client{
		Transport: &http.Transport{TLSClientConfig: fixture.clients[transporttls.ControlPlaneCertificateIdentity]},
		Timeout:   3 * time.Second,
	}, "node-token")

	if _, err := client.ListContainers(context.Background(), "worker-01", server.Listener.Addr().String()); err == nil {
		t.Fatal("client accepted worker-02 certificate for worker-01 target")
	}
	if requests.Load() != 0 {
		t.Fatalf("mismatched target reached handler %d times; want zero", requests.Load())
	}

	containers, err := client.ListContainers(context.Background(), "worker-02", server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("valid control-plane request to worker-02: %v", err)
	}
	if len(containers) != 0 || requests.Load() != 1 {
		t.Fatalf("containers = %v, handler requests = %d; want empty response and one request", containers, requests.Load())
	}
}

type authorizationTestRuntime struct{}

func (authorizationTestRuntime) Create(context.Context, runtime.ContainerConfig) (runtime.Container, error) {
	return runtime.Container{}, errors.New("unexpected create")
}
func (authorizationTestRuntime) Start(context.Context, string, string) error {
	return errors.New("unexpected start")
}
func (authorizationTestRuntime) Stop(context.Context, string, string) error {
	return errors.New("unexpected stop")
}
func (authorizationTestRuntime) Remove(context.Context, string, string) error {
	return errors.New("unexpected remove")
}
func (authorizationTestRuntime) Inspect(context.Context, string, string) (runtime.Container, error) {
	return runtime.Container{}, runtime.ErrContainerNotFound
}
func (authorizationTestRuntime) List(context.Context, string) ([]runtime.Container, error) {
	return []runtime.Container{}, nil
}
func (authorizationTestRuntime) Stats(context.Context, string, string) (runtime.ContainerStats, error) {
	return runtime.ContainerStats{}, errors.New("unexpected stats")
}

type agentTLSFixture struct {
	serverTLS       *tls.Config
	clients         map[string]*tls.Config
	nodeServerTLS   map[string]*tls.Config
	untrustedClient *tls.Config
}

func newAgentTLSFixture(t *testing.T) agentTLSFixture {
	t.Helper()
	now := time.Now()
	caKey, caCert := makeCertificateAuthority(t, 1, now)
	caDER, err := x509.CreateCertificate(rand.Reader, caCert, caCert, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test CA: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse test CA: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)

	issue := func(serial int64, identity string) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate leaf key: %v", err)
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "localhost"},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			NotBefore:    now.Add(-time.Minute),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		}
		if identity != "" {
			parsed, err := url.Parse(identity)
			if err != nil {
				t.Fatalf("parse certificate identity: %v", err)
			}
			leaf.URIs = []*url.URL{parsed}
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create leaf certificate: %v", err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	newClient := func(cert tls.Certificate) *tls.Config {
		return &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
			RootCAs:      roots,
		}
	}
	newServer := func(cert tls.Certificate) *tls.Config {
		return &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    roots,
		}
	}

	controlPlaneCert := issue(2, transporttls.ControlPlaneCertificateIdentity)
	nodeOneCert := issue(3, transporttls.NodeCertificateIdentity("worker-01"))
	nodeTwoCert := issue(4, transporttls.NodeCertificateIdentity("worker-02"))
	noIdentityCert := issue(5, "")
	malformedCert := issue(6, "spiffe://mini-orchestrator/node/worker-01/extra")
	nodeOneServerCert := issue(7, transporttls.NodeCertificateIdentity("worker-01"))
	nodeTwoServerCert := issue(8, transporttls.NodeCertificateIdentity("worker-02"))

	untrustedKey, untrustedCA := makeCertificateAuthority(t, 20, now)
	untrustedDER, err := x509.CreateCertificate(rand.Reader, untrustedCA, untrustedCA, &untrustedKey.PublicKey, untrustedKey)
	if err != nil {
		t.Fatalf("create untrusted CA: %v", err)
	}
	untrustedCA, err = x509.ParseCertificate(untrustedDER)
	if err != nil {
		t.Fatalf("parse untrusted CA: %v", err)
	}
	untrustedLeafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate untrusted leaf key: %v", err)
	}
	untrustedLeafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(21),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	untrustedLeafDER, err := x509.CreateCertificate(rand.Reader, untrustedLeafTemplate, untrustedCA, &untrustedLeafKey.PublicKey, untrustedKey)
	if err != nil {
		t.Fatalf("create untrusted leaf certificate: %v", err)
	}
	untrustedLeaf, err := x509.ParseCertificate(untrustedLeafDER)
	if err != nil {
		t.Fatalf("parse untrusted leaf certificate: %v", err)
	}
	untrustedClientCert := tls.Certificate{
		Certificate: [][]byte{untrustedLeafDER},
		PrivateKey:  untrustedLeafKey,
		Leaf:        untrustedLeaf,
	}

	return agentTLSFixture{
		serverTLS: newServer(nodeOneServerCert),
		clients: map[string]*tls.Config{
			transporttls.ControlPlaneCertificateIdentity:      newClient(controlPlaneCert),
			transporttls.NodeCertificateIdentity("worker-01"): newClient(nodeOneCert),
			transporttls.NodeCertificateIdentity("worker-02"): newClient(nodeTwoCert),
			"":          newClient(noIdentityCert),
			"malformed": newClient(malformedCert),
		},
		nodeServerTLS: map[string]*tls.Config{
			"worker-01": newServer(nodeOneServerCert),
			"worker-02": newServer(nodeTwoServerCert),
		},
		untrustedClient: newClient(untrustedClientCert),
	}
}

func makeCertificateAuthority(t *testing.T, serial int64, now time.Time) (*ecdsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	return key, &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
}
