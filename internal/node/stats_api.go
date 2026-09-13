package node

import (
	"encoding/json"
	"net/http"
)

func (a *Agent) stats(
	w http.ResponseWriter,
	r *http.Request,
) {
	containerID := r.PathValue("id")

	stats, err := a.Runtime.Stats(
		r.Context(),
		containerID,
	)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(stats); err != nil {
		return
	}
}
