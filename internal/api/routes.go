package api

import (
	"net/http"

	"mini-orchestrator/internal/observability"
)

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.Handle(
		"GET /metrics",
		s.metrics.Handler(),
	)

	mux.Handle(
		"POST /nodes/register",
		RequireBearerToken(
			s.nodeToken,
			RequireMutualTLS(http.HandlerFunc(s.registerNode)),
		),
	)

	mux.Handle(
		"POST /nodes/heartbeat",
		RequireBearerToken(
			s.nodeToken,
			RequireMutualTLS(http.HandlerFunc(s.heartbeat)),
		),
	)

	mux.Handle(
		"POST /services",
		RequireBearerToken(s.nodeToken, http.HandlerFunc(s.createService)),
	)

	mux.Handle(
		"GET /services/{id}",
		RequireBearerToken(s.nodeToken, http.HandlerFunc(s.getService)),
	)

	mux.Handle(
		"PUT /services/{id}",
		RequireBearerToken(s.nodeToken, http.HandlerFunc(s.updateService)),
	)

	mux.Handle(
		"DELETE /services/{id}",
		RequireBearerToken(s.nodeToken, http.HandlerFunc(s.deleteService)),
	)

	mux.Handle("GET /healthz", observability.HealthHandler())
	mux.Handle("GET /readyz", observability.ReadinessHandler(s.readiness))

	return mux

}
