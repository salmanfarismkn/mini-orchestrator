package httpconfig

import (
	"net/http"
	"testing"
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
