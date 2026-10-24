package api

import (
	"net/http"
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
			http.HandlerFunc(s.registerNode),
		),
	)

	mux.Handle(
		"POST /nodes/heartbeat",
		RequireBearerToken(
			s.nodeToken,
			http.HandlerFunc(s.heartbeat),
		),
	)

	mux.HandleFunc(
		"POST /services",
		s.createService,
	)

	mux.HandleFunc(
		"GET /services/{id}",
		s.getService,
	)

	mux.HandleFunc(
		"PUT /services/{id}",
		s.updateService,
	)

	mux.HandleFunc(
		"DELETE /services/{id}",
		s.deleteService,
	)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return mux

}
