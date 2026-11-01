package transporttls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadRequiresValidMutualTLSMaterial(t *testing.T) {
	certFile, keyFile, caFile := testCertificateFiles(t)

	serverTLS, clientTLS, err := Load(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("load TLS configuration: %v", err)
	}
	if serverTLS.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("server client auth = %v; want RequireAndVerifyClientCert", serverTLS.ClientAuth)
	}
	if len(clientTLS.Certificates) != 1 || clientTLS.RootCAs == nil {
		t.Fatal("client TLS configuration must present a certificate and trusted CA")
	}
	if clientTLS.InsecureSkipVerify {
		t.Fatal("client certificate verification must remain enabled")
	}
	if err := ValidateServerName(serverTLS, "127.0.0.1"); err != nil {
		t.Fatalf("server certificate did not match advertised IP SAN: %v", err)
	}
	if err := ValidateServerName(serverTLS, "worker.invalid"); err == nil {
		t.Fatal("server certificate accepted an address outside its SANs")
	}
	if _, _, err := Load(certFile, keyFile, filepath.Join(t.TempDir(), "missing-ca.pem")); err == nil {
		t.Fatal("Load succeeded with missing CA file")
	}
}

func TestLoadFromEnvFailsWhenTLSPathsAreMissing(t *testing.T) {
	t.Setenv(CertFileEnv, "")
	t.Setenv(KeyFileEnv, "")
	t.Setenv(CAFileEnv, "")
	if _, _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv succeeded without required TLS paths")
	}
}

func TestMutualTLSHandshakeRequiresTrustedClientCertificate(t *testing.T) {
	certFile, keyFile, caFile := testCertificateFiles(t)
	serverTLS, clientTLS, err := Load(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("load TLS configuration: %v", err)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverTLS
	server.StartTLS()
	t.Cleanup(server.Close)

	untrustedClient := &http.Client{Timeout: 2 * time.Second}
	if _, err := untrustedClient.Get(server.URL); err == nil {
		t.Fatal("TLS handshake succeeded without a client certificate")
	}
	untrustedRoots := clientTLS.Clone()
	untrustedRoots.RootCAs = x509.NewCertPool()
	untrustedServerClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: untrustedRoots},
		Timeout:   2 * time.Second,
	}
	if _, err := untrustedServerClient.Get(server.URL); err == nil {
		t.Fatal("TLS handshake succeeded without trusting the server CA")
	}
	mismatchedNameTLS := clientTLS.Clone()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split test server address: %v", err)
	}
	mismatchedNameClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: mismatchedNameTLS,
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			},
		},
		Timeout: 2 * time.Second,
	}
	wrongHostURL := url.URL{
		Scheme: "https",
		Host:   net.JoinHostPort("wrong-host.invalid", port),
		Path:   "/",
	}
	if _, err := mismatchedNameClient.Get(wrongHostURL.String()); err == nil {
		t.Fatal("TLS handshake succeeded with a hostname not in the server certificate SANs")
	}

	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: clientTLS},
		Timeout:   2 * time.Second,
	}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("mutual TLS request failed: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d; want %d", response.StatusCode, http.StatusNoContent)
	}
}

func TestDevelopmentNodeCertificateIdentityProvisioning(t *testing.T) {
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "development CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("generate CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	type provisioned struct {
		certificate *x509.Certificate
		privateKey  *ecdsa.PrivateKey
	}
	provision := func(nodeID string, serial int64) provisioned {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate node key: %v", err)
		}
		nodeURI, err := url.Parse("spiffe://mini-orchestrator/node/" + nodeID)
		if err != nil {
			t.Fatalf("parse node URI: %v", err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "localhost"},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			URIs:         []*url.URL{nodeURI},
			NotBefore:    now.Add(-time.Minute),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("issue node certificate: %v", err)
		}
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("parse node certificate: %v", err)
		}
		return provisioned{certificate: certificate, privateKey: key}
	}

	worker01 := provision("worker-01", 2)
	worker02 := provision("worker-02", 3)

	fingerprint := func(cert *x509.Certificate) [32]byte {
		return sha256.Sum256(cert.Raw)
	}
	if fingerprint(worker01.certificate) == fingerprint(worker02.certificate) {
		t.Fatal("worker certificates have the same fingerprint")
	}
	if worker01.privateKey.D.Cmp(worker02.privateKey.D) == 0 {
		t.Fatal("worker certificates use the same private key")
	}

	for _, tc := range []struct {
		name string
		cert *x509.Certificate
		uri  string
	}{
		{name: "worker-01", cert: worker01.certificate, uri: "spiffe://mini-orchestrator/node/worker-01"},
		{name: "worker-02", cert: worker02.certificate, uri: "spiffe://mini-orchestrator/node/worker-02"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.cert.URIs) != 1 || tc.cert.URIs[0].String() != tc.uri {
				t.Fatalf("URI SAN = %v; want %s", tc.cert.URIs, tc.uri)
			}
			if err := tc.cert.VerifyHostname("localhost"); err != nil {
				t.Fatalf("certificate lost localhost hostname verification: %v", err)
			}
			if err := tc.cert.VerifyHostname("127.0.0.1"); err != nil {
				t.Fatalf("certificate lost IP hostname verification: %v", err)
			}
			hasClientAuth := false
			for _, usage := range tc.cert.ExtKeyUsage {
				if usage == x509.ExtKeyUsageClientAuth {
					hasClientAuth = true
				}
			}
			if !hasClientAuth {
				t.Fatal("certificate does not permit client authentication")
			}
			roots := x509.NewCertPool()
			roots.AddCert(caCert)
			if _, err := tc.cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: "localhost", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
				t.Fatalf("certificate does not chain to development CA: %v", err)
			}
		})
	}
}

func testCertificateFiles(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-only CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test CA: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	nodeURI, err := url.Parse("spiffe://mini-orchestrator/node/worker-01")
	if err != nil {
		t.Fatalf("parse development node URI: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		URIs:         []*url.URL{nodeURI},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test leaf certificate: %v", err)
	}
	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal test leaf key: %v", err)
	}

	certFile := filepath.Join(dir, "certificate.pem")
	keyFile := filepath.Join(dir, "key.pem")
	caFile := filepath.Join(dir, "ca.pem")
	writePEM(t, certFile, "CERTIFICATE", leafDER)
	writePEM(t, keyFile, "PRIVATE KEY", leafKeyDER)
	writePEM(t, caFile, "CERTIFICATE", caDER)
	return certFile, keyFile, caFile
}

func writePEM(t *testing.T, path, kind string, data []byte) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create test certificate file: %v", err)
	}
	if err := pem.Encode(file, &pem.Block{Type: kind, Bytes: data}); err != nil {
		file.Close()
		t.Fatalf("write test certificate file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close test certificate file: %v", err)
	}
}
