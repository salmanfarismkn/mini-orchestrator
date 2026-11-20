package node

import (
	"net/http"

	"mini-orchestrator/internal/transporttls"
)

const targetNodeHeader = "X-Orchestrator-Target-Node-ID"

func RequireControlPlaneIdentity(nodeID string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 ||
			len(r.TLS.VerifiedChains[0]) == 0 || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "verified control-plane certificate is required", http.StatusForbidden)
			return
		}

		leaf := r.TLS.VerifiedChains[0][0]
		if len(leaf.URIs) != 1 || leaf.URIs[0] == nil ||
			leaf.URIs[0].String() != transporttls.ControlPlaneCertificateIdentity {
			http.Error(w, "control-plane certificate identity is required", http.StatusForbidden)
			return
		}
		if nodeID == "" || r.Header.Get(targetNodeHeader) != nodeID {
			http.Error(w, "request target does not match this node", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}
