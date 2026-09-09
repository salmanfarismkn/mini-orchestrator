package node

import "net/http"

func (a *Agent) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(
		"POST /containers",
		a.CreateContainer,
	)

	mux.HandleFunc(
		"GET /containers",
		a.ListContainers,
	)

	mux.HandleFunc(
		"GET /containers/{id}",
		a.InspectContainer,
	)

	mux.HandleFunc(
		"POST /containers/{id}/start",
		a.StartContainer,
	)

	mux.HandleFunc(
		"POST /containers/{id}/stop",
		a.StopContainer,
	)

	mux.HandleFunc(
		"DELETE /containers/{id}",
		a.RemoveContainer,
	)

	return mux
}
