package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/observability"
	"mini-orchestrator/internal/store"
)

type Server struct {
	store         *store.Postgres
	nodeToken     string
	operatorToken string
	metrics       *observability.Metrics
	readiness     observability.DatabaseChecker
}

func NewServer(
	store *store.Postgres,
	nodeToken string,
	operatorToken string,
	metrics *observability.Metrics,
) *Server {
	if metrics == nil {
		metrics = &observability.Metrics{}
	}
	metrics.SetOperationalSnapshotProvider(func(ctx context.Context) (observability.OperationalSnapshot, error) {
		snapshot, err := store.GetOperationalMetrics(ctx)
		if err != nil {
			return observability.OperationalSnapshot{}, err
		}
		return observability.OperationalSnapshot{
			ActiveDeployments:      snapshot.ActiveDeployments,
			PendingDeployments:     snapshot.PendingDeployments,
			ProgressingDeployments: snapshot.ProgressingDeployments,
			AvailableDeployments:   snapshot.AvailableDeployments,
			FailedDeployments:      snapshot.FailedDeployments,
			DesiredReplicas:        snapshot.DesiredReplicas,
			RunningReplicas:        snapshot.RunningReplicas,
			FailedWorkloads:        snapshot.FailedWorkloads,
			ReadyNodes:             snapshot.ReadyNodes,
			UnhealthyNodes:         snapshot.UnhealthyNodes,
			DrainingNodes:          snapshot.DrainingNodes,
			ReservedCPUMillis:      snapshot.ReservedCPUMillis,
			ReservedMemoryMB:       snapshot.ReservedMemoryMB,
		}, nil
	})
	return &Server{
		store:         store,
		nodeToken:     nodeToken,
		operatorToken: operatorToken,
		metrics:       metrics,
		readiness:     store,
	}
}

type deploymentOperationalStatusResponse struct {
	ServiceID                   string     `json:"service_id"`
	DeploymentVersion           int        `json:"deployment_version"`
	Status                      string     `json:"status"`
	DesiredReplicas             int        `json:"desired_replicas"`
	CurrentVersionWorkloadCount int64      `json:"current_version_workload_count"`
	RunningReplicaCount         int64      `json:"running_replica_count"`
	FailedReplicaCount          int64      `json:"failed_replica_count"`
	ProgressPercent             float64    `json:"progress_percent"`
	MaxSurge                    int        `json:"max_surge"`
	MaxUnavailable              int        `json:"max_unavailable"`
	DeploymentStartedAt         *time.Time `json:"deployment_started_at,omitempty"`
	ServiceCreatedAt            time.Time  `json:"service_created_at"`
	FailureReason               string     `json:"failure_reason,omitempty"`
	ReadinessSupported          bool       `json:"readiness_supported"`
}

func (s *Server) getDeploymentOperationalStatus(w http.ResponseWriter, r *http.Request) {
	serviceID := r.PathValue("id")
	if serviceID == "" {
		writeError(w, http.StatusBadRequest, "service id is required")
		return
	}
	status, err := s.store.GetDeploymentOperationalStatus(r.Context(), serviceID)
	if err != nil {
		if errors.Is(err, store.ErrServiceNotFound) {
			writeError(w, http.StatusNotFound, "service not found")
			return
		}
		slog.Error("failed to read deployment status", "service_id", serviceID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to read deployment status")
		return
	}
	writeJSON(w, http.StatusOK, deploymentOperationalStatusResponse{
		ServiceID:                   status.ServiceID,
		DeploymentVersion:           status.DeploymentVersion,
		Status:                      string(status.DeploymentStatus),
		DesiredReplicas:             status.DesiredReplicas,
		CurrentVersionWorkloadCount: status.CurrentVersionWorkloadCount,
		RunningReplicaCount:         status.RunningReplicaCount,
		FailedReplicaCount:          status.FailedReplicaCount,
		ProgressPercent:             status.ProgressPercent,
		MaxSurge:                    status.MaxSurge,
		MaxUnavailable:              status.MaxUnavailable,
		DeploymentStartedAt:         status.DeploymentStartedAt,
		ServiceCreatedAt:            status.ServiceCreatedAt,
		FailureReason:               string(status.DeploymentFailureReason),
		ReadinessSupported:          false,
	})
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
	RegistrationID string `json:"registration_id"`
	Address        string `json:"address"`
	CPUCapacity    int    `json:"cpu_capacity_millis"`
	MemoryCapacity int    `json:"memory_capacity_mb"`
}

type heartbeatRequest struct {
	ID              string `json:"id"`
	LeaseGeneration int64  `json:"lease_generation"`
}

type heartbeatResponse struct {
	ID              string    `json:"id"`
	Status          string    `json:"status"`
	LeaseGeneration int64     `json:"lease_generation"`
	Timestamp       time.Time `json:"timestamp"`
}

type nodeStateResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type nodeOperationalStatusResponse struct {
	ID                    string    `json:"id"`
	Status                string    `json:"status"`
	Schedulable           bool      `json:"schedulable"`
	AssignedWorkloadCount int       `json:"assigned_workload_count"`
	ReservedCPU           int       `json:"reserved_cpu_millis"`
	ReservedMemory        int       `json:"reserved_memory_mb"`
	AvailableCPU          int       `json:"available_cpu_millis"`
	AvailableMemory       int       `json:"available_memory_mb"`
	Empty                 bool      `json:"empty"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

func (s *Server) getNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "node id is required")
		return
	}

	status, err := s.store.GetNodeOperationalStatus(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNodeNotFound) {
			writeError(w, http.StatusNotFound, "node not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to read node status")
		return
	}

	writeJSON(w, http.StatusOK, nodeOperationalStatusResponse{
		ID:                    status.Node.ID,
		Status:                string(status.Node.Status),
		Schedulable:           status.Schedulable,
		AssignedWorkloadCount: status.AssignedWorkloadCount,
		ReservedCPU:           status.ReservedCPU,
		ReservedMemory:        status.ReservedMemory,
		AvailableCPU:          status.AvailableCPU,
		AvailableMemory:       status.AvailableMemory,
		Empty:                 status.Empty,
		CreatedAt:             status.Node.CreatedAt,
		UpdatedAt:             status.Node.UpdatedAt,
	})
}

func (s *Server) drainNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "node id is required")
		return
	}
	node, err := s.store.DrainNode(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNodeNotFound):
			writeError(w, http.StatusNotFound, "node not found")
		case errors.Is(err, store.ErrInvalidNodeTransition):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "failed to drain node")
		}
		return
	}
	writeJSON(w, http.StatusOK, nodeStateResponse{ID: node.ID, Status: string(node.Status)})
}

func (s *Server) reactivateNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "node id is required")
		return
	}
	node, err := s.store.ReactivateNode(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNodeNotFound):
			writeError(w, http.StatusNotFound, "node not found")
		case errors.Is(err, store.ErrInvalidNodeTransition):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "failed to reactivate node")
		}
		return
	}
	writeJSON(w, http.StatusOK, nodeStateResponse{ID: node.ID, Status: string(node.Status)})
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
	certificateNodeID, ok := nodeIdentityFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusForbidden, "authenticated node identity is required")
		return
	}

	var req heartbeatRequest

	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "node id is required")
		return
	}
	if req.LeaseGeneration < 1 {
		writeError(w, http.StatusBadRequest, "lease generation is required")
		return
	}
	if req.ID != certificateNodeID {
		writeError(w, http.StatusForbidden, "certificate identity does not match node id")
		return
	}

	now := time.Now().UTC()
	certificateIdentity := "spiffe://mini-orchestrator/node/" + certificateNodeID

	if err := s.store.UpdateHeartbeat(
		r.Context(),
		req.ID,
		certificateIdentity,
		req.LeaseGeneration,
		model.NodeReady,
		now,
	); err != nil {
		switch {
		case errors.Is(err, store.ErrNodeLeaseStale):
			writeError(w, http.StatusConflict, "node lease generation is stale")
		case errors.Is(err, store.ErrNodeIdentityNotBound):
			writeError(w, http.StatusForbidden, "node identity is not enrolled")
		case errors.Is(err, store.ErrNodeIdentityMismatch):
			writeError(w, http.StatusForbidden, "certificate identity does not match enrolled node identity")
		default:
			writeError(w, http.StatusInternalServerError, "failed to update heartbeat")
		}
		return
	}

	persisted, err := s.store.GetNode(r.Context(), req.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read node status")
		return
	}

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(heartbeatResponse{
		ID:              req.ID,
		Status:          string(persisted.Status),
		LeaseGeneration: req.LeaseGeneration,
		Timestamp:       now,
	})
}

type registerNodeResponse struct {
	ID              string `json:"id"`
	Status          string `json:"status"`
	LeaseGeneration int64  `json:"lease_generation"`
}

func (s *Server) registerNode(
	w http.ResponseWriter,
	r *http.Request,
) {
	certificateNodeID, ok := nodeIdentityFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusForbidden, "authenticated node identity is required")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req registerNodeRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" ||
		req.RegistrationID == "" || len(req.RegistrationID) > 64 ||
		req.Address == "" ||
		req.CPUCapacity <= 0 ||
		req.MemoryCapacity <= 0 {
		writeError(w, http.StatusBadRequest, "invalid node registration")
		return
	}

	if req.ID != certificateNodeID {
		writeError(w, http.StatusForbidden, "certificate identity does not match node id")
		return
	}
	for _, r := range req.RegistrationID {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			writeError(w, http.StatusBadRequest, "invalid registration id")
			return
		}
	}

	now := time.Now().UTC()
	certificateIdentity := "spiffe://mini-orchestrator/node/" + certificateNodeID

	node := model.Node{
		ID:             req.ID,
		Address:        req.Address,
		CPUCapacity:    req.CPUCapacity,
		MemoryCapacity: req.MemoryCapacity,
		Status:         model.NodeReady,
		LastHeartbeat:  now,
	}

	leaseGeneration, err := s.store.RegisterNode(r.Context(), node, certificateIdentity, req.RegistrationID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNodeLeaseStale):
			writeError(w, http.StatusConflict, "node registration is stale")
		case errors.Is(err, store.ErrNodeIdentityNotBound):
			writeError(w, http.StatusForbidden, "node identity is not enrolled")
		case errors.Is(err, store.ErrNodeIdentityMismatch):
			writeError(w, http.StatusForbidden, "certificate identity does not match enrolled node identity")
		default:
			writeError(w, http.StatusInternalServerError, "failed to register node")
		}
		return
	}

	persisted, err := s.store.GetNode(r.Context(), node.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read node status")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(registerNodeResponse{
		ID:              node.ID,
		Status:          string(persisted.Status),
		LeaseGeneration: leaseGeneration,
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
			"service_id", service.ID,
			"error", err,
		)

		writeError(w, http.StatusInternalServerError, "failed to create service")
		return
	}

	slog.Info(
		"deployment started",
		"service_id", service.ID,
		"deployment_version", service.DeploymentVersion,
		"state", service.DeploymentStatus,
	)

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

	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if req.Replicas < 0 ||
		req.CPURequestMillis <= 0 ||
		req.MemoryRequestMB <= 0 ||
		req.MaxSurge < 0 ||
		req.MaxUnavailable < 0 ||
		(req.MaxSurge == 0 && req.MaxUnavailable == 0 && req.Replicas > 0) {
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

	oldDeploymentVersion := service.DeploymentVersion
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
	if service.DeploymentVersion > oldDeploymentVersion {
		slog.Info(
			"deployment started",
			"service_id", service.ID,
			"deployment_version", service.DeploymentVersion,
			"state", service.DeploymentStatus,
		)
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
