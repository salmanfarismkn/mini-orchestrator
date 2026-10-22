package api

import "net/http"

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.Handle(
		"/nodes/register",
		requireNodeToken(
			s.nodeToken,
			http.HandlerFunc(s.handleRegisterNode),
		),
	)

	mux.Handle(
		"/nodes/heartbeat",
		requireNodeToken(
			s.nodeToken,
			http.HandlerFunc(s.handleNodeHeartbeat),
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

	return mux
}

func (s *Server) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "node registration is not implemented", http.StatusNotImplemented)
}

func (s *Server) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "node heartbeat is not implemented", http.StatusNotImplemented)
}

func requireNodeToken(
	token string,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		expected := "Bearer " + token

		if r.Header.Get("Authorization") != expected {
			http.Error(
				w,
				"unauthorized",
				http.StatusUnauthorized,
			)
			return
		}

		next.ServeHTTP(w, r)
	})
}
