package reconciler

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"mini-orchestrator/internal/autoscaler"
	"mini-orchestrator/internal/controller"
	"mini-orchestrator/internal/metrics"
	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/scheduler"
)

type acceptanceMetrics struct {
	values map[string]metrics.WorkloadMetrics
	errors map[string]error
}

func (m acceptanceMetrics) GetWorkloadMetrics(
	_ context.Context,
	workloadID string,
) (metrics.WorkloadMetrics, error) {
	if err := m.errors[workloadID]; err != nil {
		return metrics.WorkloadMetrics{}, err
	}
	value, ok := m.values[workloadID]
	if !ok {
		return metrics.WorkloadMetrics{}, fmt.Errorf("missing fake metrics for %q", workloadID)
	}
	return value, nil
}

func TestRollingDeploymentAcceptanceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	service := acceptanceService(integrationID(t, "rollout"), 1)
	env.cleanupTestData(t, service.ID)
	if err := env.store.CreateService(ctx, service); err != nil {
		t.Fatalf("create service: %v", err)
	}

	old := acceptanceWorkload(service, "old", 1, model.WorkloadRunning)
	createAcceptanceWorkload(t, ctx, env, old)
	service.Image = "integration/test:v2"
	if err := env.store.UpdateService(ctx, service); err != nil {
		t.Fatalf("update service image: %v", err)
	}
	service, err := env.store.GetService(ctx, service.ID)
	if err != nil {
		t.Fatalf("read updated service: %v", err)
	}
	if service.DeploymentVersion != 2 ||
		service.DeploymentStatus != model.DeploymentProgressing ||
		service.DeploymentStartedAt == nil {
		t.Fatalf("updated deployment = version %d, status %q, started %v; want version 2 progressing with start time",
			service.DeploymentVersion, service.DeploymentStatus, service.DeploymentStartedAt)
	}

	deployment := controller.NewDeploymentController(env.store)
	replicas := controller.NewReplicaController(env.store)
	if err := deployment.ReconcileService(ctx, service); err != nil {
		t.Fatalf("reconcile deployment: %v", err)
	}
	workloads := listAcceptanceWorkloads(t, ctx, env, service.ID)
	if len(workloads) != 2 {
		t.Fatalf("workload count after first deployment step = %d; want 2", len(workloads))
	}
	var replacement model.Workload
	var oldAfter model.Workload
	for _, workload := range workloads {
		switch workload.DeploymentVersion {
		case 1:
			oldAfter = workload
		case 2:
			replacement = workload
		}
	}
	if replacement.ID == "" || replacement.Image != "integration/test:v2" ||
		replacement.ActualState != model.WorkloadPending {
		t.Fatalf("replacement = %+v; want pending version 2 image integration/test:v2", replacement)
	}
	if oldAfter.ActualState != model.WorkloadRunning ||
		oldAfter.DesiredState == model.WorkloadStopped {
		t.Fatalf("old workload changed before replacement became ready: %+v", oldAfter)
	}
	if err := replicas.ReconcileService(ctx, service); err != nil {
		t.Fatalf("reconcile replicas during rollout: %v", err)
	}
	if got := len(listAcceptanceWorkloads(t, ctx, env, service.ID)); got != 2 {
		t.Fatalf("replica controller created duplicate during rollout: workload count %d; want 2", got)
	}

	setAcceptanceWorkloadState(t, ctx, env, replacement.ID, model.WorkloadRunning)
	if err := deployment.ReconcileService(ctx, service); err != nil {
		t.Fatalf("advance rollout after replacement is running: %v", err)
	}
	workloads = listAcceptanceWorkloads(t, ctx, env, service.ID)
	oldAfter = workloadByID(t, workloads, old.ID)
	replacement = workloadByID(t, workloads, replacement.ID)
	if oldAfter.ActualState != model.WorkloadRunning ||
		oldAfter.DesiredState != model.WorkloadStopped {
		t.Fatalf("old workload should be retained until stop reconciliation, then marked stopped: %+v", oldAfter)
	}
	if replacement.ActualState != model.WorkloadRunning ||
		replacement.DeploymentVersion != service.DeploymentVersion {
		t.Fatalf("replacement state/version = %q/%d; want running/version %d",
			replacement.ActualState, replacement.DeploymentVersion, service.DeploymentVersion)
	}
	if active := countAcceptanceActive(workloads); active > service.DesiredReplicas+service.MaxSurge {
		t.Fatalf("active workload count %d exceeds desired + maxSurge = %d",
			active, service.DesiredReplicas+service.MaxSurge)
	}
	if running := countAcceptanceRunning(workloads); running <
		service.DesiredReplicas-service.MaxUnavailable {
		t.Fatalf("stopping old workload would violate maxUnavailable: running=%d desired=%d maxUnavailable=%d",
			running, service.DesiredReplicas, service.MaxUnavailable)
	}

	setAcceptanceWorkloadState(t, ctx, env, old.ID, model.WorkloadStopped)
	status := controller.NewDeploymentStatusController(env.store, time.Minute)
	if err := status.ReconcileService(ctx, service); err != nil {
		t.Fatalf("mark rollout available: %v", err)
	}
	service, err = env.store.GetService(ctx, service.ID)
	if err != nil {
		t.Fatalf("read completed service: %v", err)
	}
	if service.DeploymentStatus != model.DeploymentAvailable {
		t.Fatalf("deployment status = %q; want AVAILABLE", service.DeploymentStatus)
	}

	unavailable := acceptanceService(integrationID(t, "rollout-unavailable"), 1)
	unavailable.MaxSurge = 0
	unavailable.MaxUnavailable = 1
	env.cleanupTestData(t, unavailable.ID)
	if err := env.store.CreateService(ctx, unavailable); err != nil {
		t.Fatalf("create maxUnavailable service: %v", err)
	}
	oldUnavailable := acceptanceWorkload(unavailable, "old", 1, model.WorkloadRunning)
	createAcceptanceWorkload(t, ctx, env, oldUnavailable)
	unavailable.Image = "integration/test:v2"
	if err := env.store.UpdateService(ctx, unavailable); err != nil {
		t.Fatalf("update maxUnavailable service: %v", err)
	}
	unavailable, err = env.store.GetService(ctx, unavailable.ID)
	if err != nil {
		t.Fatalf("read maxUnavailable service: %v", err)
	}
	if err := deployment.ReconcileService(ctx, unavailable); err != nil {
		t.Fatalf("reconcile maxUnavailable rollout: %v", err)
	}
	workloads = listAcceptanceWorkloads(t, ctx, env, unavailable.ID)
	oldUnavailable = workloadByID(t, workloads, oldUnavailable.ID)
	if oldUnavailable.DesiredState != model.WorkloadStopped {
		t.Fatalf("maxUnavailable=1 did not permit stopping the old replica: %+v", oldUnavailable)
	}
	if len(workloads) != 1 {
		t.Fatalf("maxSurge=0 created a replacement before stopping old replica: count %d; want 1",
			len(workloads))
	}
	if err := controller.NewReplicaController(env.store).ReconcileService(ctx, unavailable); err != nil {
		t.Fatalf("create replacement after maxUnavailable stop: %v", err)
	}
	workloads = listAcceptanceWorkloads(t, ctx, env, unavailable.ID)
	if len(workloads) != 2 {
		t.Fatalf("replacement count after old replica stop intent = %d; want 2", len(workloads))
	}
}

func TestFailedAndTimedOutDeploymentAcceptanceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	service := acceptanceService(integrationID(t, "failed-rollout"), 1)
	env.cleanupTestData(t, service.ID)
	if err := env.store.CreateService(ctx, service); err != nil {
		t.Fatalf("create service: %v", err)
	}
	old := acceptanceWorkload(service, "old", 1, model.WorkloadRunning)
	createAcceptanceWorkload(t, ctx, env, old)
	service.Image = "integration/test:v2"
	if err := env.store.UpdateService(ctx, service); err != nil {
		t.Fatalf("update service image: %v", err)
	}
	service, err := env.store.GetService(ctx, service.ID)
	if err != nil {
		t.Fatalf("read updated service: %v", err)
	}
	failed := acceptanceWorkload(service, "failed-new", service.DeploymentVersion, model.WorkloadFailed)
	createAcceptanceWorkload(t, ctx, env, failed)

	deployment := controller.NewDeploymentController(env.store)
	if err := deployment.ReconcileService(ctx, service); err != nil {
		t.Fatalf("reconcile failed deployment: %v", err)
	}
	workloads := listAcceptanceWorkloads(t, ctx, env, service.ID)
	if len(workloads) != 2 {
		t.Fatalf("failed replacement caused extra workload creation: count %d; want 2", len(workloads))
	}
	if got := workloadByID(t, workloads, old.ID); got.DesiredState == model.WorkloadStopped {
		t.Fatalf("healthy old workload was marked stopped after replacement failure: %+v", got)
	}

	status := controller.NewDeploymentStatusController(env.store, time.Minute)
	if err := status.ReconcileService(ctx, service); err != nil {
		t.Fatalf("mark failed deployment terminal: %v", err)
	}
	service, err = env.store.GetService(ctx, service.ID)
	if err != nil {
		t.Fatalf("read failed deployment: %v", err)
	}
	if service.DeploymentStatus != model.DeploymentFailed {
		t.Fatalf("deployment status = %q; want FAILED", service.DeploymentStatus)
	}
	if err := deployment.ReconcileService(ctx, service); err != nil {
		t.Fatalf("reconcile terminal failed deployment: %v", err)
	}
	if got := len(listAcceptanceWorkloads(t, ctx, env, service.ID)); got != 2 {
		t.Fatalf("terminal failed deployment continued creating replacements: count %d; want 2", got)
	}

	failedOnly := acceptanceService(integrationID(t, "failed-only"), 1)
	failedOnly.DeploymentStatus = model.DeploymentFailed
	env.cleanupTestData(t, failedOnly.ID)
	if err := env.store.CreateService(ctx, failedOnly); err != nil {
		t.Fatalf("create terminal failed service: %v", err)
	}
	failedWorkload := acceptanceWorkload(failedOnly, "failed", 1, model.WorkloadFailed)
	createAcceptanceWorkload(t, ctx, env, failedWorkload)
	if err := controller.NewReplicaController(env.store).ReconcileService(ctx, failedOnly); err != nil {
		t.Fatalf("reconcile replicas for terminal failed deployment: %v", err)
	}
	if got := len(listAcceptanceWorkloads(t, ctx, env, failedOnly.ID)); got != 1 {
		t.Fatalf("terminal failed deployment created replacement workload: count %d; want 1", got)
	}

	timedOut := acceptanceService(integrationID(t, "timed-out"), 1)
	env.cleanupTestData(t, timedOut.ID)
	if err := env.store.CreateService(ctx, timedOut); err != nil {
		t.Fatalf("create timeout service: %v", err)
	}
	timedOut.Image = "integration/test:v2"
	if err := env.store.UpdateService(ctx, timedOut); err != nil {
		t.Fatalf("update timeout service image: %v", err)
	}
	timedOut, err = env.store.GetService(ctx, timedOut.ID)
	if err != nil {
		t.Fatalf("read timeout service: %v", err)
	}
	if _, err := env.database.ExecContext(
		ctx,
		`UPDATE services SET deployment_started_at = $1 WHERE id = $2`,
		time.Now().Add(-2*time.Minute),
		timedOut.ID,
	); err != nil {
		t.Fatalf("expire deployment start time: %v", err)
	}
	timedOut.DeploymentStartedAt = timePtr(time.Now().Add(-2 * time.Minute))
	if err := controller.NewDeploymentStatusController(env.store, time.Minute).
		ReconcileService(ctx, timedOut); err != nil {
		t.Fatalf("reconcile timed-out deployment: %v", err)
	}
	timedOut, err = env.store.GetService(ctx, timedOut.ID)
	if err != nil {
		t.Fatalf("read timed-out deployment: %v", err)
	}
	if timedOut.DeploymentStatus != model.DeploymentFailed {
		t.Fatalf("timed-out deployment status = %q; want FAILED", timedOut.DeploymentStatus)
	}
}

func TestZeroDesiredReplicasAcceptanceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	service := acceptanceService(integrationID(t, "zero-replicas"), 0)
	env.cleanupTestData(t, service.ID)
	if err := env.store.CreateService(ctx, service); err != nil {
		t.Fatalf("create zero-replica service: %v", err)
	}
	workload := acceptanceWorkload(service, "running", 1, model.WorkloadRunning)
	createAcceptanceWorkload(t, ctx, env, workload)

	if err := controller.NewReplicaController(env.store).ReconcileService(ctx, service); err != nil {
		t.Fatalf("reconcile zero-replica service: %v", err)
	}
	workloads := listAcceptanceWorkloads(t, ctx, env, service.ID)
	if len(workloads) != 1 || workloads[0].DesiredState != model.WorkloadStopped {
		t.Fatalf("zero-replica reconciliation workloads = %+v; want the existing workload stopped",
			workloads)
	}
}

func TestAutoscalingAcceptanceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	env := newMultiNodeEnvironment(t)
	createService := func(service model.Service, usage int, running int, metricErr bool) *autoscaler.Autoscaler {
		t.Helper()
		env.cleanupTestData(t, service.ID)
		if err := env.store.CreateService(ctx, service); err != nil {
			t.Fatalf("create autoscaling service %q: %v", service.ID, err)
		}
		values := make(map[string]metrics.WorkloadMetrics)
		errorsByID := make(map[string]error)
		for index := 0; index < running; index++ {
			workload := acceptanceWorkload(service, fmt.Sprintf("running-%d", index), service.DeploymentVersion, model.WorkloadRunning)
			createAcceptanceWorkload(t, ctx, env, workload)
			if metricErr && index == running-1 {
				errorsByID[workload.ID] = errors.New("metrics unavailable")
			} else {
				values[workload.ID] = metrics.WorkloadMetrics{CPUUsageMillis: usage}
			}
		}
		return autoscaler.New(env.store, acceptanceMetrics{values: values, errors: errorsByID})
	}

	disabled := acceptanceService(integrationID(t, "autoscale-disabled"), 2)
	disabled.Autoscaling = model.AutoscalingConfig{Enabled: false}
	scaler := createService(disabled, 1000, 1, false)
	if err := scaler.ReconcileService(ctx, disabled); err != nil {
		t.Fatalf("reconcile disabled autoscaler: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, disabled.ID, 2)

	scaleUp := acceptanceService(integrationID(t, "autoscale-up"), 2)
	scaleUp.Autoscaling = model.AutoscalingConfig{
		Enabled: true, MinReplicas: 1, MaxReplicas: 4, TargetCPU: 50,
		RequiredObservations: 2, MaxScaleStep: 1,
	}
	scaler = createService(scaleUp, 200, 2, false)
	if err := scaler.ReconcileService(ctx, scaleUp); err != nil {
		t.Fatalf("first scale-up observation: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, scaleUp.ID, 2)
	if err := scaler.ReconcileService(ctx, scaleUp); err != nil {
		t.Fatalf("second scale-up observation: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, scaleUp.ID, 3)
	scaleUp, err := env.store.GetService(ctx, scaleUp.ID)
	if err != nil {
		t.Fatalf("read scaled-up service: %v", err)
	}
	if err := controller.NewReplicaController(env.store).ReconcileService(ctx, scaleUp); err != nil {
		t.Fatalf("create replica after autoscale: %v", err)
	}
	scaleUpWorkloads := listAcceptanceWorkloads(t, ctx, env, scaleUp.ID)
	var pending model.Workload
	for _, workload := range scaleUpWorkloads {
		if workload.ActualState == model.WorkloadPending {
			pending = workload
			break
		}
	}
	if pending.ID == "" {
		t.Fatal("autoscaled replica was not created as a pending workload")
	}
	schedulerService := scheduler.NewService(env.store, scheduler.New())
	if err := schedulerService.ScheduleWorkload(ctx, pending); err != nil {
		t.Fatalf("schedule autoscaled replica: %v", err)
	}
	scheduled, err := env.store.GetWorkload(ctx, pending.ID)
	if err != nil {
		t.Fatalf("read scheduled autoscaled replica: %v", err)
	}
	if scheduled.NodeID == nil ||
		scheduled.DesiredState != model.WorkloadScheduled ||
		scheduled.ActualState != model.WorkloadPending {
		t.Fatalf("autoscaled workload state = %q, node %v; want scheduled and reserved",
			scheduled.ActualState, scheduled.NodeID)
	}
	reservedNode, err := env.store.GetNode(ctx, *scheduled.NodeID)
	if err != nil {
		t.Fatalf("read autoscaled workload node: %v", err)
	}
	if reservedNode.CPUAllocated != scheduled.CPURequestMillis ||
		reservedNode.MemoryAllocated != scheduled.MemoryRequestMB ||
		reservedNode.CPUAllocated > reservedNode.CPUCapacity ||
		reservedNode.MemoryAllocated > reservedNode.MemoryCapacity {
		t.Fatalf("autoscaled replica reservation CPU=%d/%d memory=%d/%d does not respect capacity",
			reservedNode.CPUAllocated, reservedNode.CPUCapacity,
			reservedNode.MemoryAllocated, reservedNode.MemoryCapacity)
	}

	maxBound := acceptanceService(integrationID(t, "autoscale-max"), 2)
	maxBound.Autoscaling = model.AutoscalingConfig{
		Enabled: true, MinReplicas: 1, MaxReplicas: 4, TargetCPU: 50,
		RequiredObservations: 1, MaxScaleStep: 10,
	}
	scaler = createService(maxBound, 5000, 2, false)
	if err := scaler.ReconcileService(ctx, maxBound); err != nil {
		t.Fatalf("reconcile maximum replica bound: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, maxBound.ID, 4)

	scaleDown := acceptanceService(integrationID(t, "autoscale-down"), 3)
	scaleDown.Autoscaling = model.AutoscalingConfig{
		Enabled: true, MinReplicas: 1, MaxReplicas: 5, TargetCPU: 50,
		ScaleDownCooldownSeconds: 60, RequiredObservations: 2, MaxScaleStep: 10,
	}
	lastScaled := time.Now()
	scaleDown.AutoscalingLastScaledAt = &lastScaled
	scaler = createService(scaleDown, 0, 3, false)
	if err := scaler.ReconcileService(ctx, scaleDown); err != nil {
		t.Fatalf("reconcile during scale-down cooldown: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, scaleDown.ID, 3)
	if _, err := env.database.ExecContext(
		ctx,
		`UPDATE services SET autoscaling_last_scaled_at = $1 WHERE id = $2`,
		time.Now().Add(-24*time.Hour),
		scaleDown.ID,
	); err != nil {
		t.Fatalf("expire scale-down cooldown: %v", err)
	}
	scaleDown, err = env.store.GetService(ctx, scaleDown.ID)
	if err != nil {
		t.Fatalf("read scale-down service: %v", err)
	}
	if err := scaler.ReconcileService(ctx, scaleDown); err != nil {
		t.Fatalf("first scale-down observation: %v", err)
	}
	if err := scaler.ReconcileService(ctx, scaleDown); err != nil {
		t.Fatalf("second scale-down observation: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, scaleDown.ID, 1)

	scaleToZero := acceptanceService(integrationID(t, "autoscale-zero"), 2)
	scaleToZero.Autoscaling = model.AutoscalingConfig{
		Enabled: true, MinReplicas: 0, MaxReplicas: 5, TargetCPU: 50,
		RequiredObservations: 1, MaxScaleStep: 5,
	}
	scaler = createService(scaleToZero, 0, 2, false)
	if err := scaler.ReconcileService(ctx, scaleToZero); err != nil {
		t.Fatalf("reconcile autoscaler to zero: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, scaleToZero.ID, 0)
	scaleToZero, err = env.store.GetService(ctx, scaleToZero.ID)
	if err != nil {
		t.Fatalf("read zero-scaled service: %v", err)
	}
	if err := controller.NewReplicaController(env.store).ReconcileService(ctx, scaleToZero); err != nil {
		t.Fatalf("reconcile zero-scaled replicas: %v", err)
	}
	for _, workload := range listAcceptanceWorkloads(t, ctx, env, scaleToZero.ID) {
		if workload.DesiredState != model.WorkloadStopped {
			t.Errorf("zero-scaled workload %q desired state = %q; want STOPPED",
				workload.ID, workload.DesiredState)
		}
	}

	missingMetrics := acceptanceService(integrationID(t, "autoscale-metrics-error"), 2)
	missingMetrics.Autoscaling = model.AutoscalingConfig{
		Enabled: true, MinReplicas: 0, MaxReplicas: 5, TargetCPU: 50,
		RequiredObservations: 1, MaxScaleStep: 5,
	}
	scaler = createService(missingMetrics, 0, 2, true)
	if err := scaler.ReconcileService(ctx, missingMetrics); err == nil {
		t.Fatal("metrics failure did not surface an error")
	}
	assertDesiredReplicas(t, ctx, env, missingMetrics.ID, 2)

	progressing := acceptanceService(integrationID(t, "autoscale-rollout"), 1)
	progressing.DeploymentStatus = model.DeploymentProgressing
	progressing.Autoscaling = model.AutoscalingConfig{
		Enabled: true, MinReplicas: 1, MaxReplicas: 5, TargetCPU: 50,
		RequiredObservations: 1, MaxScaleStep: 5,
	}
	scaler = createService(progressing, 1000, 1, false)
	if err := scaler.ReconcileService(ctx, progressing); err != nil {
		t.Fatalf("reconcile autoscaler during rollout: %v", err)
	}
	assertDesiredReplicas(t, ctx, env, progressing.ID, 1)

	for _, status := range []model.ServiceStatus{
		model.ServiceDeleting,
		model.ServiceDeleted,
	} {
		inactive := acceptanceService(integrationID(t, "autoscale-inactive"), 2)
		inactive.Status = status
		inactive.Autoscaling = model.AutoscalingConfig{
			Enabled: true, MinReplicas: 1, MaxReplicas: 5, TargetCPU: 50,
			RequiredObservations: 1, MaxScaleStep: 5,
		}
		scaler = createService(inactive, 1000, 1, false)
		if err := scaler.ReconcileService(ctx, inactive); err != nil {
			t.Fatalf("reconcile autoscaler for %s service: %v", status, err)
		}
		assertDesiredReplicas(t, ctx, env, inactive.ID, 2)
	}
}

func acceptanceService(id string, replicas int) model.Service {
	now := time.Now().UTC()
	return model.Service{
		ID: id, Name: id, Image: "integration/test:v1",
		DesiredReplicas: replicas, CPURequestMillis: 100, MemoryRequestMB: 64,
		DeploymentVersion: 1, MaxSurge: 1, MaxUnavailable: 0,
		DeploymentStatus: model.DeploymentAvailable, Status: model.ServiceActive,
		CreatedAt: now, UpdatedAt: now,
	}
}

func acceptanceWorkload(
	service model.Service,
	suffix string,
	version int,
	state model.WorkloadState,
) model.Workload {
	return model.Workload{
		ID: integrationIDForWorkload(service.ID, suffix), ServiceID: service.ID,
		Image: service.Image, CPURequestMillis: service.CPURequestMillis,
		MemoryRequestMB: service.MemoryRequestMB, DeploymentVersion: version,
		DesiredState: state, ActualState: state,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
}

func integrationIDForWorkload(serviceID, suffix string) string {
	hash := sha256.Sum256([]byte(serviceID + "-" + suffix))
	return "wl-" + fmt.Sprintf("%x", hash[:16])
}

func createAcceptanceWorkload(
	t *testing.T,
	ctx context.Context,
	env *multiNodeEnvironment,
	workload model.Workload,
) {
	t.Helper()
	if err := env.store.CreateWorkload(ctx, workload); err != nil {
		t.Fatalf("create workload %q: %v", workload.ID, err)
	}
}

func setAcceptanceWorkloadState(
	t *testing.T,
	ctx context.Context,
	env *multiNodeEnvironment,
	workloadID string,
	state model.WorkloadState,
) {
	t.Helper()
	if err := env.store.UpdateWorkloadActualState(ctx, workloadID, state); err != nil {
		t.Fatalf("set workload %q state: %v", workloadID, err)
	}
}

func listAcceptanceWorkloads(
	t *testing.T,
	ctx context.Context,
	env *multiNodeEnvironment,
	serviceID string,
) []model.Workload {
	t.Helper()
	workloads, err := env.store.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("list service workloads %q: %v", serviceID, err)
	}
	return workloads
}

func workloadByID(
	t *testing.T,
	workloads []model.Workload,
	id string,
) model.Workload {
	t.Helper()
	for _, workload := range workloads {
		if workload.ID == id {
			return workload
		}
	}
	t.Fatalf("workload %q not found", id)
	return model.Workload{}
}

func countAcceptanceActive(workloads []model.Workload) int {
	count := 0
	for _, workload := range workloads {
		if workload.ActualState == model.WorkloadPending ||
			workload.ActualState == model.WorkloadScheduled ||
			workload.ActualState == model.WorkloadRunning {
			if workload.DesiredState != model.WorkloadStopped {
				count++
			}
		}
	}
	return count
}

func countAcceptanceRunning(workloads []model.Workload) int {
	count := 0
	for _, workload := range workloads {
		if workload.ActualState == model.WorkloadRunning &&
			workload.DesiredState != model.WorkloadStopped {
			count++
		}
	}
	return count
}

func assertDesiredReplicas(
	t *testing.T,
	ctx context.Context,
	env *multiNodeEnvironment,
	serviceID string,
	want int,
) {
	t.Helper()
	service, err := env.store.GetService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read service %q: %v", serviceID, err)
	}
	if service.DesiredReplicas != want {
		t.Fatalf("service %q desired replicas = %d; want %d",
			serviceID, service.DesiredReplicas, want)
	}
}

func timePtr(value time.Time) *time.Time {
	return &value
}
