package deploy_test

import (
	"os"
	"strings"
	"testing"
)

func TestComposeRequiresPostgresSecretAndBindsLoopback(t *testing.T) {
	compose, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatalf("read Compose configuration: %v", err)
	}
	config := string(compose)
	if !strings.Contains(config, "POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?") {
		t.Fatal("Compose must require POSTGRES_PASSWORD interpolation")
	}
	if strings.Contains(config, "POSTGRES_PASSWORD: orchestrator") {
		t.Fatal("Compose must not contain a fixed PostgreSQL password")
	}
	if !strings.Contains(config, "127.0.0.1:${POSTGRES_PORT:-5432}:5432") {
		t.Fatal("PostgreSQL port must be published on loopback")
	}
}
