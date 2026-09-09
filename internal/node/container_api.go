package node

import (
	"encoding/json"
	"net/http"
	"strings"

	"mini-orchestrator/internal/runtime"
)

type createContainerRequest struct {
	Name      string `json:"name"`
	Image     string `json:"image"`
	CPUMillis int    `json:"cpu_millis"`
	MemoryMB  int    `json:"memory_mb"`
}

type createContainerResponse struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Image  string `json:"image"`
	Status string `json:"status"`
}

func (a *Agent) CreateContainer(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req createContainerRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	if req.Name == "" ||
		req.Image == "" ||
		req.CPUMillis <= 0 ||
		req.MemoryMB <= 0 {
		http.Error(
			w,
			"invalid container configuration",
			http.StatusBadRequest,
		)
		return
	}

	container, err := a.Runtime.Create(
		r.Context(),
		runtime.ContainerConfig{
			Name:      req.Name,
			Image:     req.Image,
			CPUMillis: req.CPUMillis,
			MemoryMB:  req.MemoryMB,
		},
	)
	if err != nil {
		http.Error(
			w,
			"failed to create container",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(
		createContainerResponse{
			ID:     container.ID,
			Name:   container.Name,
			Image:  container.Image,
			Status: container.Status,
		},
	)
}

func (a *Agent) StartContainer(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := containerIDFromPath(r)

	if id == "" {
		http.Error(w, "container id is required", http.StatusBadRequest)
		return
	}

	if err := a.Runtime.Start(r.Context(), id); err != nil {
		http.Error(
			w,
			"failed to start container",
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) StopContainer(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := containerIDFromPath(r)

	if id == "" {
		http.Error(w, "container id is required", http.StatusBadRequest)
		return
	}

	if err := a.Runtime.Stop(r.Context(), id); err != nil {
		http.Error(
			w,
			"failed to stop container",
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) RemoveContainer(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := containerIDFromPath(r)

	if id == "" {
		http.Error(w, "container id is required", http.StatusBadRequest)
		return
	}

	if err := a.Runtime.Remove(r.Context(), id); err != nil {
		http.Error(
			w,
			"failed to remove container",
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) InspectContainer(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := containerIDFromPath(r)

	if id == "" {
		http.Error(w, "container id is required", http.StatusBadRequest)
		return
	}

	container, err := a.Runtime.Inspect(r.Context(), id)
	if err != nil {
		http.Error(
			w,
			"container not found",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(container)
}

func (a *Agent) ListContainers(
	w http.ResponseWriter,
	r *http.Request,
) {
	containers, err := a.Runtime.List(r.Context())
	if err != nil {
		http.Error(
			w,
			"failed to list containers",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(containers)
}

func containerIDFromPath(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	if path == "" {
		return ""
	}

	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[0] != "containers" {
		return ""
	}

	return parts[1]
}
