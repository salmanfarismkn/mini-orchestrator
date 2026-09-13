package api

import "net/http"

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(
		"POST /nodes/register",
		s.registerNode,
	)

	mux.HandleFunc(
		"POST /nodes/heartbeat",
		s.heartbeat,
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

	return mux
}
