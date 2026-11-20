package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"mini-orchestrator/internal/controller"
	"mini-orchestrator/internal/model"
	"mini-orchestrator/internal/observability"
	"mini-orchestrator/internal/store"
)

func TestDeploymentOperationalStatusAPIIntegration(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to a dedicated migrated PostgreSQL test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to PostgreSQL test database: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("close PostgreSQL test store: %v", err)
		}
	})
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL cleanup connection: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL cleanup connection: %v", err)
		}
	})

	handler := NewServer(p, "deployment-test-token", "deployment-test-token", &observability.Metrics{}).Routes()
	serviceID := fmt.Sprintf("api-deployment-%d", time.Now().UnixNano())
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	service := model.Service{
		ID: serviceID, Name: serviceID, Image: "integration/test:latest",
		DesiredReplicas: 2, CPURequestMillis: 100, MemoryRequestMB: 128,
		DeploymentVersion: 2, MaxSurge: 1, MaxUnavailable: 0,
		DeploymentStatus: model.DeploymentProgressing, DeploymentStartedAt: &startedAt,
		Status: model.ServiceActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := p.CreateService(ctx, service); err != nil {
		t.Fatalf("create deployment API fixture service: %v", err)
	}
	t.Cleanup(func() {
		if _, err := database.ExecContext(context.Background(), `DELETE FROM services WHERE id = $1`, serviceID); err != nil {
			t.Errorf("clean up deployment API fixture: %v", err)
		}
	})
	workloadIDs := []string{
		createDeploymentAPIWorkload(t, ctx, p, serviceID, 1, model.WorkloadRunning),
		createDeploymentAPIWorkload(t, ctx, p, serviceID, 2, model.WorkloadRunning),
		createDeploymentAPIWorkload(t, ctx, p, serviceID, 2, model.WorkloadFailed),
	}
	if err := p.UpdateDeploymentFailure(ctx, serviceID, model.DeploymentFailureWorkloadFailed); err != nil {
		t.Fatalf("persist deployment failure reason: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/services/"+serviceID+"/deployment", nil)
	request.Header.Set("Authorization", "Bearer deployment-test-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("deployment status response = %d: %s", response.Code, response.Body.String())
	}
	var status deploymentOperationalStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatalf("decode deployment status response: %v", err)
	}
	if status.ServiceID != serviceID || status.DeploymentVersion != 2 ||
		status.Status != string(model.DeploymentFailed) ||
		status.DesiredReplicas != 2 || status.CurrentVersionWorkloadCount != 2 ||
		status.RunningReplicaCount != 1 || status.FailedReplicaCount != 1 ||
		status.ProgressPercent != 50 || status.MaxSurge != 1 ||
		status.MaxUnavailable != 0 ||
		status.FailureReason != string(model.DeploymentFailureWorkloadFailed) ||
		status.ReadinessSupported {
		t.Fatalf("deployment status response = %+v", status)
	}
	if strings.Contains(response.Body.String(), "private-runtime-detail") {
		t.Fatal("deployment status exposed raw workload execution error")
	}

	beforeService, err := p.GetService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read service before status request: %v", err)
	}
	beforeWorkloads, err := p.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read workloads before status request: %v", err)
	}
	request = httptest.NewRequest(http.MethodGet, "/services/"+serviceID+"/deployment", nil)
	request.Header.Set("Authorization", "Bearer deployment-test-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("repeated deployment status response = %d", response.Code)
	}
	afterService, err := p.GetService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read service after status request: %v", err)
	}
	afterWorkloads, err := p.ListWorkloadsByService(ctx, serviceID)
	if err != nil {
		t.Fatalf("read workloads after status request: %v", err)
	}
	if !beforeService.UpdatedAt.Equal(afterService.UpdatedAt) ||
		len(beforeWorkloads) != len(afterWorkloads) {
		t.Fatalf("deployment inspection changed service/workload records")
	}
	for i := range beforeWorkloads {
		if beforeWorkloads[i].ID != afterWorkloads[i].ID ||
			beforeWorkloads[i].ActualState != afterWorkloads[i].ActualState ||
			beforeWorkloads[i].DesiredState != afterWorkloads[i].DesiredState ||
			beforeWorkloads[i].ResourceReserved != afterWorkloads[i].ResourceReserved {
			t.Fatalf("deployment inspection changed workload: before=%+v after=%+v",
				beforeWorkloads[i], afterWorkloads[i])
		}
	}

	overlapID := fmt.Sprintf("api-deployment-overlap-%d", time.Now().UnixNano())
	overlap := model.Service{
		ID: overlapID, Name: overlapID, Image: "integration/test:latest",
		DesiredReplicas: 1, CPURequestMillis: 100, MemoryRequestMB: 128,
		DeploymentVersion: 1, MaxSurge: 1, DeploymentStatus: model.DeploymentProgressing,
		Status: model.ServiceActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := p.CreateService(ctx, overlap); err != nil {
		t.Fatalf("create reconciliation overlap fixture: %v", err)
	}
	t.Cleanup(func() {
		if _, err := database.ExecContext(context.Background(), `DELETE FROM services WHERE id = $1`, overlapID); err != nil {
			t.Errorf("clean up reconciliation overlap fixture: %v", err)
		}
	})
	createDeploymentAPIWorkload(t, ctx, p, overlapID, 1, model.WorkloadRunning)
	start := make(chan struct{})
	errs := make(chan error, 11)
	var wg sync.WaitGroup
	wg.Add(11)
	go func() {
		defer wg.Done()
		<-start
		errs <- controller.NewDeploymentStatusController(p, time.Minute).ReconcileService(ctx, overlap)
	}()
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			<-start
			request := httptest.NewRequest(http.MethodGet, "/services/"+overlapID+"/deployment", nil)
			request.Header.Set("Authorization", "Bearer deployment-test-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				errs <- fmt.Errorf("overlapping status request returned %d", response.Code)
				return
			}
			var snapshot deploymentOperationalStatusResponse
			if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
				errs <- err
				return
			}
			if (snapshot.Status != string(model.DeploymentProgressing) &&
				snapshot.Status != string(model.DeploymentAvailable)) ||
				snapshot.RunningReplicaCount != 1 || snapshot.ProgressPercent != 100 {
				errs <- fmt.Errorf("inconsistent status during reconciliation: %+v", snapshot)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}

	zeroReplicasID := fmt.Sprintf("api-deployment-zero-%d", time.Now().UnixNano())
	zero := service
	zero.ID = zeroReplicasID
	zero.Name = zeroReplicasID
	zero.DesiredReplicas = 0
	zero.DeploymentVersion = 1
	zero.DeploymentStatus = model.DeploymentAvailable
	zero.DeploymentFailureReason = ""
	zero.DeploymentStartedAt = nil
	if err := p.CreateService(ctx, zero); err != nil {
		t.Fatalf("create zero-replica API fixture: %v", err)
	}
	t.Cleanup(func() {
		if _, err := database.ExecContext(context.Background(), `DELETE FROM services WHERE id = $1`, zeroReplicasID); err != nil {
			t.Errorf("clean up zero-replica API fixture: %v", err)
		}
	})
	request = httptest.NewRequest(http.MethodGet, "/services/"+zeroReplicasID+"/deployment", nil)
	request.Header.Set("Authorization", "Bearer deployment-test-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("zero-replica status response = %d: %s", response.Code, response.Body.String())
	}
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatalf("decode zero-replica deployment status: %v", err)
	}
	if status.DesiredReplicas != 0 || status.ProgressPercent != 100 ||
		status.CurrentVersionWorkloadCount != 0 {
		t.Fatalf("zero-replica status = %+v", status)
	}

	request = httptest.NewRequest(http.MethodGet, "/services/unknown-deployment/deployment", nil)
	request.Header.Set("Authorization", "Bearer deployment-test-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown deployment status = %d; want %d", response.Code, http.StatusNotFound)
	}

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRequest.Header.Set("Authorization", "Bearer deployment-test-token")
	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, metricsRequest)
	if metricsResponse.Code != http.StatusOK {
		t.Fatalf("metrics response = %d: %s", metricsResponse.Code, metricsResponse.Body.String())
	}
	for _, identifier := range append(workloadIDs, serviceID) {
		if strings.Contains(metricsResponse.Body.String(), identifier) {
			t.Fatalf("metrics exposed workload identifier %q", identifier)
		}
	}
}

func createDeploymentAPIWorkload(
	t *testing.T,
	ctx context.Context,
	p *store.Postgres,
	serviceID string,
	version int,
	state model.WorkloadState,
) string {
	t.Helper()
	id := fmt.Sprintf("api-workload-%d", time.Now().UnixNano())
	now := time.Now().UTC()
	if err := p.CreateWorkload(ctx, model.Workload{
		ID: id, ServiceID: serviceID, Image: "integration/test:latest",
		CPURequestMillis: 100, MemoryRequestMB: 128, DeploymentVersion: version,
		DesiredState: state, ActualState: state, ExecutionError: "private-runtime-detail",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create deployment API fixture workload: %v", err)
	}
	return id
}
