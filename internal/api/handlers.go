package api

import (
	"encoding/json"
	"net/http"
	"time"
	"log/slog"

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

type updateServiceRequest struct {
	Name             string `json:"name"`
	Image            string `json:"image"`
	Replicas         int    `json:"replicas"`
	CPURequestMillis int    `json:"cpu_request_millis"`
	MemoryRequestMB  int    `json:"memory_request_mb"`

	MaxSurge       int `json:"max_surge"`
	MaxUnavailable int `json:"max_unavailable"`
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
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type createServiceRequest struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Image            string `json:"image"`
	Replicas         int    `json:"replicas"`
	CPURequestMillis int    `json:"cpu_request_millis"`
	MemoryRequestMB  int    `json:"memory_request_mb"`

	MaxSurge       int `json:"max_surge"`
	MaxUnavailable int `json:"max_unavailable"`

	Autoscaling *autoscalingRequest `json:"autoscaling"`
}

type autoscalingRequest struct {
	Enabled                  bool    `json:"enabled"`
	MinReplicas              int     `json:"min_replicas"`
	MaxReplicas              int     `json:"max_replicas"`
	TargetCPU                float64 `json:"target_cpu"`
	ScaleUpCooldownSeconds   int     `json:"scale_up_cooldown_seconds"`
	ScaleDownCooldownSeconds int     `json:"scale_down_cooldown_seconds"`
	RequiredObservations     int     `json:"required_observations"`
	MaxScaleStep             int     `json:"max_scale_step"`
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

func (s *Server) createService(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req createServiceRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(
			w,
			"invalid JSON",
			http.StatusBadRequest,
		)
		return
	}

	if err := validateServiceRequest(req); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	now := time.Now().UTC()

	autoscaling := model.AutoscalingConfig{}

	if req.Autoscaling != nil {
		autoscaling = model.AutoscalingConfig{
			Enabled:                   req.Autoscaling.Enabled,
			MinReplicas:               req.Autoscaling.MinReplicas,
			MaxReplicas:               req.Autoscaling.MaxReplicas,
			TargetCPU:                 req.Autoscaling.TargetCPU,
			ScaleUpCooldownSeconds:    req.Autoscaling.ScaleUpCooldownSeconds,
			ScaleDownCooldownSeconds:  req.Autoscaling.ScaleDownCooldownSeconds,
			RequiredObservations:      req.Autoscaling.RequiredObservations,
			MaxScaleStep:              req.Autoscaling.MaxScaleStep,
		}
	}

	service := model.Service{
		ID:                req.ID,
		Name:              req.Name,
		Image:             req.Image,
		DesiredReplicas:   req.Replicas,
		CPURequestMillis:  req.CPURequestMillis,
		MemoryRequestMB:  req.MemoryRequestMB,
		DeploymentVersion: 1,

		MaxSurge:       req.MaxSurge,
		MaxUnavailable: req.MaxUnavailable,

		DeploymentStatus: model.DeploymentAvailable,

		Autoscaling: autoscaling,

		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.store.CreateService(
		r.Context(),
		service,
	); err != nil {
		slog.Error(
			"failed to create service",
			"error", err,
		)

		http.Error(
			w,
			"failed to create service",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(service)
}

func (s *Server) getService(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := r.PathValue("id")

	service, err := s.store.GetService(
		r.Context(),
		id,
	)
	if err != nil {
		http.Error(
			w,
			"service not found",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	_ = json.NewEncoder(w).Encode(service)
}

func (s *Server) updateService(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := r.PathValue("id")

	var req updateServiceRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(
			w,
			"invalid JSON",
			http.StatusBadRequest,
		)
		return
	}

	if req.Replicas < 0 ||
		req.CPURequestMillis <= 0 ||
		req.MemoryRequestMB <= 0 {
		http.Error(
			w,
			"invalid service configuration",
			http.StatusBadRequest,
		)
		return
	}

	service, err := s.store.GetService(
		r.Context(),
		id,
	)
	if err != nil {
		http.Error(
			w,
			"service not found",
			http.StatusNotFound,
		)
		return
	}

	service.Name = req.Name
	service.Image = req.Image
	service.DesiredReplicas = req.Replicas
	service.CPURequestMillis = req.CPURequestMillis
	service.MemoryRequestMB = req.MemoryRequestMB
	service.MaxSurge = req.MaxSurge
	service.MaxUnavailable = req.MaxUnavailable

	if err := s.store.UpdateService(
		r.Context(),
		service,
	); err != nil {
		slog.Error(
			"failed to update service",
			"service_id", id,
			"error", err,
		)

		http.Error(
			w,
			"failed to update service",
			http.StatusInternalServerError,
		)
		return
	}

	service, err = s.store.GetService(
		r.Context(),
		id,
	)
	if err != nil {
		http.Error(
			w,
			"failed to read updated service",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	_ = json.NewEncoder(w).Encode(service)
}