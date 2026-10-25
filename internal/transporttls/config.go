package transporttls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

const (
	CertFileEnv = "ORCHESTRATOR_TLS_CERT_FILE"
	KeyFileEnv  = "ORCHESTRATOR_TLS_KEY_FILE"
	CAFileEnv   = "ORCHESTRATOR_TLS_CA_FILE"
)

func LoadFromEnv() (*tls.Config, *tls.Config, error) {
	certFile := os.Getenv(CertFileEnv)
	keyFile := os.Getenv(KeyFileEnv)
	caFile := os.Getenv(CAFileEnv)
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, nil, fmt.Errorf(
			"%s, %s, and %s are required",
			CertFileEnv,
			KeyFileEnv,
			CAFileEnv,
		)
	}
	return Load(certFile, keyFile, caFile)
}

func Load(certFile, keyFile, caFile string) (*tls.Config, *tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load TLS certificate and key: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read TLS CA file: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("TLS CA file contains no valid certificates")
	}
	serverRoots := x509.NewCertPool()
	if !serverRoots.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("TLS CA file contains no valid certificates")
	}

	server := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	client := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      serverRoots,
	}
	return server, client, nil
}

func ValidateServerName(config *tls.Config, serverName string) error {
	if config == nil || len(config.Certificates) == 0 ||
		len(config.Certificates[0].Certificate) == 0 {
		return fmt.Errorf("TLS server certificate is not configured")
	}
	certificate, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
	if err != nil {
		return fmt.Errorf("parse TLS server certificate: %w", err)
	}
	if err := certificate.VerifyHostname(serverName); err != nil {
		return fmt.Errorf("TLS server certificate does not cover %q: %w", serverName, err)
	}
	return nil
}
