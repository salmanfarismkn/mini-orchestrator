package transporttls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
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
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
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
