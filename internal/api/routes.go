package api

import "net/http"

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.Handle(
		"POST /nodes/register",
		http.HandlerFunc(s.registerNode),
	)

	mux.Handle(
		"POST /nodes/heartbeat",
		http.HandlerFunc(s.heartbeat),
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

	return RequireBearerToken(s.nodeToken, mux)
}
