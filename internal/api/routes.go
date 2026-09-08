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

	return mux
}