package api

import (
	"net/http"

	"mini-orchestrator/internal/observability"
)

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.Handle(
		"GET /metrics",
		RequireBearerToken(s.operatorToken, s.metrics.Handler()),
	)

	mux.Handle(
		"POST /nodes/register",
		RequireBearerToken(
			s.nodeToken,
			RequireMutualTLS(RequireNodeIdentity(http.HandlerFunc(s.registerNode))),
		),
	)

	mux.Handle(
		"POST /nodes/heartbeat",
		RequireBearerToken(
			s.nodeToken,
			RequireMutualTLS(RequireNodeIdentity(http.HandlerFunc(s.heartbeat))),
		),
	)

	mux.Handle(
		"GET /nodes/{id}",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.getNode)),
	)

	mux.Handle(
		"POST /nodes/{id}/drain",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.drainNode)),
	)

	mux.Handle(
		"POST /nodes/{id}/reactivate",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.reactivateNode)),
	)

	mux.Handle(
		"POST /services",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.createService)),
	)

	mux.Handle(
		"GET /services/{id}",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.getService)),
	)

	mux.Handle(
		"GET /services/{id}/deployment",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.getDeploymentOperationalStatus)),
	)

	mux.Handle(
		"PUT /services/{id}",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.updateService)),
	)

	mux.Handle(
		"DELETE /services/{id}",
		RequireBearerToken(s.operatorToken, http.HandlerFunc(s.deleteService)),
	)

	mux.Handle("GET /healthz", observability.HealthHandler())
	mux.Handle("GET /readyz", observability.ReadinessHandler(s.readiness))

	return mux

}
