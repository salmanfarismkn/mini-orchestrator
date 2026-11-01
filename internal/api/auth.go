package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"net/url"
	"strings"
)

func authenticatedNodeIdentity(r *http.Request) (string, error) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		return "", ErrUnverifiedNodeCertificate
	}

	if len(r.TLS.PeerCertificates) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return "", ErrUnverifiedNodeCertificate
	}
	leaf := r.TLS.VerifiedChains[0][0]

	var identities []string
	for _, uri := range leaf.URIs {
		if uri == nil {
			continue
		}
		identities = append(identities, uri.String())
	}
	if len(identities) != 1 {
		if len(identities) == 0 {
			return "", ErrMissingNodeIdentity
		}
		return "", ErrDuplicateNodeIdentity
	}

	return parseNodeIdentity(identities[0])
}

var (
	ErrUnverifiedNodeCertificate = &NodeIdentityError{"client certificate verification is required"}
	ErrMissingNodeIdentity       = &NodeIdentityError{"client certificate has no node URI SAN"}
	ErrMalformedNodeIdentity     = &NodeIdentityError{"client certificate has a malformed node URI SAN"}
	ErrDuplicateNodeIdentity     = &NodeIdentityError{"client certificate has duplicate or unexpected URI identities"}
)

type NodeIdentityError struct{ message string }

func (e *NodeIdentityError) Error() string { return e.message }

func parseNodeIdentity(identity string) (string, error) {
	parsed, err := url.Parse(identity)
	if err != nil || parsed.Scheme != "spiffe" || parsed.Host != "mini-orchestrator" {
		return "", ErrMalformedNodeIdentity
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return "", ErrMalformedNodeIdentity
	}
	if !strings.HasPrefix(parsed.Path, "/node/") {
		return "", ErrMalformedNodeIdentity
	}
	nodeID := strings.TrimPrefix(parsed.Path, "/node/")
	if nodeID == "" || strings.Contains(nodeID, "/") {
		return "", ErrMalformedNodeIdentity
	}
	for _, r := range nodeID {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return "", ErrMalformedNodeIdentity
		}
	}
	return nodeID, nil
}

func RequireNodeIdentity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := authenticatedNodeIdentity(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(withNodeIdentity(r.Context(), identity)))
	})
}

func withNodeIdentity(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, nodeIdentityContextKey{}, identity)
}

func nodeIdentityFromContext(ctx context.Context) (string, bool) {
	identity, ok := ctx.Value(nodeIdentityContextKey{}).(string)
	return identity, ok
}

type nodeIdentityContextKey struct{}

func RequireBearerToken(
	expectedToken string,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expectedToken == "" {
			http.Error(w, "authentication is not configured", http.StatusServiceUnavailable)
			return
		}

		authHeader := r.Header.Get("Authorization")
		const prefix = "Bearer "

		if !strings.HasPrefix(authHeader, prefix) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		providedToken := strings.TrimPrefix(authHeader, prefix)

		if providedToken == "" ||
			subtle.ConstantTimeCompare(
				[]byte(providedToken),
				[]byte(expectedToken),
			) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func RequireMutualTLS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			http.Error(w, "mutual TLS is required", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
