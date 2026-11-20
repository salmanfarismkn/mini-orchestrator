package node

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"mini-orchestrator/internal/runtime"
)

func (a *Agent) stats(
	w http.ResponseWriter,
	r *http.Request,
) {
	containerID := r.PathValue("id")

	stats, err := a.Runtime.Stats(
		r.Context(),
		containerID,
		a.ID,
	)
	if err != nil {
		if errors.Is(err, runtime.ErrContainerNotFound) || errors.Is(err, runtime.ErrContainerNotOwned) {
			http.Error(w, "container not found", http.StatusNotFound)
			return
		}
		slog.Error("container stats failed", "container_id", containerID, "error", err)
		http.Error(
			w,
			"failed to retrieve container stats",
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
