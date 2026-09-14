package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
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

type updateServiceRequest struct {
	Name             string `json:"name"`
	Image            string `json:"image"`
	Replicas         int    `json:"replicas"`
	CPURequestMillis int    `json:"cpu_request_millis"`
	MemoryRequestMB  int    `json:"memory_request_mb"`

	MaxSurge       int `json:"max_surge"`
	MaxUnavailable int `json:"max_unavailable"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(errorResponse{
		Error: message,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		return err
	}

	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("request body must contain a single JSON object")
	}

	return nil
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
	ID                string              `json:"id"`
	Name              string              `json:"name"`
	Image             string              `json:"image"`
	Replicas          int                 `json:"replicas"`
	DesiredReplicas   int                 `json:"desired_replicas"`
	CPURequestMillis  int                 `json:"cpu_request_millis"`
	MemoryRequestMB   int                 `json:"memory_request_mb"`
	DeploymentVersion int                 `json:"deployment_version"`
	DeploymentStatus  string              `json:"deployment_status"`
	MaxSurge          int                 `json:"max_surge"`
	MaxUnavailable    int                 `json:"max_unavailable"`
	Status            string              `json:"status"`
	Autoscaling       *autoscalingRequest `json:"autoscaling"`

	AutoscalingEnabled                  bool    `json:"autoscaling_enabled"`
	AutoscalingMinReplicas              int     `json:"autoscaling_min_replicas"`
	AutoscalingMaxReplicas              int     `json:"autoscaling_max_replicas"`
	AutoscalingTargetCPU                float64 `json:"autoscaling_target_cpu"`
	AutoscalingScaleUpCooldownSeconds   int     `json:"autoscaling_scale_up_cooldown_seconds"`
	AutoscalingScaleDownCooldownSeconds int     `json:"autoscaling_scale_down_cooldown_seconds"`
	AutoscalingRequiredObservations     int     `json:"autoscaling_required_observations"`
	AutoscalingMaxScaleStep             int     `json:"autoscaling_max_scale_step"`
}

func (r *createServiceRequest) normalize() {
	if r.Replicas == 0 && r.DesiredReplicas > 0 {
		r.Replicas = r.DesiredReplicas
	}

	if r.Autoscaling == nil && (r.AutoscalingEnabled ||
		r.AutoscalingMinReplicas > 0 ||
		r.AutoscalingMaxReplicas > 0 ||
		r.AutoscalingTargetCPU > 0 ||
		r.AutoscalingScaleUpCooldownSeconds > 0 ||
		r.AutoscalingScaleDownCooldownSeconds > 0 ||
		r.AutoscalingRequiredObservations > 0 ||
		r.AutoscalingMaxScaleStep > 0) {
		r.Autoscaling = &autoscalingRequest{
			Enabled:                  r.AutoscalingEnabled,
			MinReplicas:              r.AutoscalingMinReplicas,
			MaxReplicas:              r.AutoscalingMaxReplicas,
			TargetCPU:                r.AutoscalingTargetCPU,
			ScaleUpCooldownSeconds:   r.AutoscalingScaleUpCooldownSeconds,
			ScaleDownCooldownSeconds: r.AutoscalingScaleDownCooldownSeconds,
			RequiredObservations:     r.AutoscalingRequiredObservations,
			MaxScaleStep:             r.AutoscalingMaxScaleStep,
		}
	}

	if r.DeploymentVersion == 0 {
		r.DeploymentVersion = 1
	}
}

func (r createServiceRequest) effectiveReplicas() int {
	if r.Replicas > 0 {
		return r.Replicas
	}
	return r.DesiredReplicas
}

func (r createServiceRequest) effectiveDeploymentStatus() model.DeploymentStatus {
	if r.DeploymentStatus == "" {
		return model.DeploymentAvailable
	}
	return model.DeploymentStatus(strings.ToUpper(r.DeploymentStatus))
}

func (r createServiceRequest) effectiveServiceStatus() model.ServiceStatus {
	if r.Status == "" {
		return model.ServiceActive
	}
	return model.ServiceStatus(strings.ToUpper(r.Status))
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
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "node id is required")
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
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req registerNodeRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" ||
		req.Address == "" ||
		req.CPUCapacity <= 0 ||
		req.MemoryCapacity <= 0 {
		writeError(w, http.StatusBadRequest, "invalid node registration")
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
		writeError(w, http.StatusInternalServerError, "failed to register node")
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
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req createServiceRequest

	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	req.normalize()

	if err := validateServiceRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	_, err := s.store.GetService(r.Context(), req.ID)

	if err == nil {
		writeError(w, http.StatusConflict, "service already exists")
		return
	}
	now := time.Now().UTC()

	autoscaling := model.AutoscalingConfig{}

	if req.Autoscaling != nil {
		autoscaling = model.AutoscalingConfig{
			Enabled:                  req.Autoscaling.Enabled,
			MinReplicas:              req.Autoscaling.MinReplicas,
			MaxReplicas:              req.Autoscaling.MaxReplicas,
			TargetCPU:                req.Autoscaling.TargetCPU,
			ScaleUpCooldownSeconds:   req.Autoscaling.ScaleUpCooldownSeconds,
			ScaleDownCooldownSeconds: req.Autoscaling.ScaleDownCooldownSeconds,
			RequiredObservations:     req.Autoscaling.RequiredObservations,
			MaxScaleStep:             req.Autoscaling.MaxScaleStep,
		}
	}

	service := model.Service{
		ID:                req.ID,
		Name:              req.Name,
		Image:             req.Image,
		DesiredReplicas:   req.effectiveReplicas(),
		CPURequestMillis:  req.CPURequestMillis,
		MemoryRequestMB:   req.MemoryRequestMB,
		DeploymentVersion: req.DeploymentVersion,

		MaxSurge:       req.MaxSurge,
		MaxUnavailable: req.MaxUnavailable,

		DeploymentStatus: req.effectiveDeploymentStatus(),
		Status:           req.effectiveServiceStatus(),

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

		writeError(w, http.StatusInternalServerError, "failed to create service")
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
		writeError(w, http.StatusNotFound, "service not found")
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
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	id := r.PathValue("id")

	var req updateServiceRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if req.Replicas < 0 ||
		req.CPURequestMillis <= 0 ||
		req.MemoryRequestMB <= 0 {
		writeError(w, http.StatusBadRequest, "invalid service configuration")
		return
	}

	service, err := s.store.GetService(
		r.Context(),
		id,
	)
	if err != nil {
		writeError(w, http.StatusNotFound, "service not found")
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

		writeError(w, http.StatusInternalServerError, "failed to update service")
		return
	}

	service, err = s.store.GetService(
		r.Context(),
		id,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read updated service")
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	_ = json.NewEncoder(w).Encode(service)
}

func (s *Server) deleteService(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if id == "" {
		writeError(w, http.StatusBadRequest, "service id is required")
		return
	}

	service, err := s.store.GetService(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "service not found")
		return
	}

	if service.Status == model.ServiceDeleted {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if err := s.store.MarkServiceDeleting(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete service")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
