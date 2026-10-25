package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

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
