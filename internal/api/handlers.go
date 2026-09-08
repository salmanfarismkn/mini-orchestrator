package api

import (
	"encoding/json"
	"net/http"
	"time"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/store"
)

type Server struct {
	store *store.Postgres
}

func NewServer(store *store.Postgres) *Server {
	return &Server{
		store: store,
	}
}

type registerNodeRequest struct {
	ID             string `json:"id"`
	Address        string `json:"address"`
	CPUCapacity    int    `json:"cpu_capacity_millis"`
	MemoryCapacity int    `json:"memory_capacity_mb"`
}

type heartbeatRequest struct {
	ID string `json:"id"`
}

type heartbeatResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func (s *Server) heartbeat(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req heartbeatRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()

	if err := s.store.UpdateHeartbeat(
		r.Context(),
		req.ID,
		model.NodeReady,
		now,
	); err != nil {
		http.Error(
			w,
			"failed to update heartbeat",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(heartbeatResponse{
		ID:        req.ID,
		Status:    string(model.NodeReady),
		Timestamp: now,
	})
}

type registerNodeResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}


func (s *Server) registerNode(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req registerNodeRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.ID == "" ||
		req.Address == "" ||
		req.CPUCapacity <= 0 ||
		req.MemoryCapacity <= 0 {
		http.Error(w, "invalid node registration", http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()

	node := model.Node{
		ID:             req.ID,
		Address:        req.Address,
		CPUCapacity:    req.CPUCapacity,
		MemoryCapacity: req.MemoryCapacity,
		Status:         model.NodeReady,
		LastHeartbeat:  now,
	}

	if err := s.store.RegisterNode(r.Context(), node); err != nil {
		http.Error(
			w,
			"failed to register node",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(registerNodeResponse{
		ID:     node.ID,
		Status: string(node.Status),
	})
}