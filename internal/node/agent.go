package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"mini-orchestrator/internal/runtime"
)

type Agent struct {
	ID              string
	Address         string
	CPUCapacity     int
	MemoryCapacity  int
	ControlPlaneURL string

	Client  *http.Client
	Runtime runtime.Runtime

	Token           string
	RegistrationID  string
	LeaseGeneration int64
	Fenced          bool
	leaseMu         sync.RWMutex
}

type registrationRequest struct {
	ID             string `json:"id"`
	RegistrationID string `json:"registration_id"`
	Address        string `json:"address"`
	CPUCapacity    int    `json:"cpu_capacity_millis"`
	MemoryCapacity int    `json:"memory_capacity_mb"`
}

func (a *Agent) Register(ctx context.Context) error {
	if err := a.validateControlPlaneURL(); err != nil {
		return err
	}

	a.leaseMu.Lock()
	if a.Fenced {
		a.leaseMu.Unlock()
		return fmt.Errorf("node agent %q is fenced and must not re-register", a.ID)
	}
	if a.RegistrationID == "" {
		randomID := make([]byte, 16)
		if _, err := rand.Read(randomID); err != nil {
			a.leaseMu.Unlock()
			return fmt.Errorf("generate registration id: %w", err)
		}
		a.RegistrationID = hex.EncodeToString(randomID)
	}
	registrationID := a.RegistrationID
	a.leaseMu.Unlock()

	payload := registrationRequest{
		ID:             a.ID,
		RegistrationID: registrationID,
		Address:        a.Address,
		CPUCapacity:    a.CPUCapacity,
		MemoryCapacity: a.MemoryCapacity,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal registration: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(a.ControlPlaneURL, "/")+"/nodes/register",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create registration request: %w", err)
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+a.Token,
	)
	req.Header.Set("Content-Type", "application/json")

	client := a.Client

	if client == nil {
		client = &http.Client{
			Timeout: 5 * time.Second,
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send registration: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf(
			"registration failed with status %d",
			resp.StatusCode,
		)
	}

	var response struct {
		LeaseGeneration int64 `json:"lease_generation"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("decode registration response: %w", err)
	}
	if response.LeaseGeneration < 1 {
		return fmt.Errorf("registration returned invalid lease generation %d", response.LeaseGeneration)
	}

	a.leaseMu.Lock()
	a.LeaseGeneration = response.LeaseGeneration
	a.Fenced = false
	a.leaseMu.Unlock()
	return nil
}

type heartbeatRequest struct {
	ID              string `json:"id"`
	LeaseGeneration int64  `json:"lease_generation"`
}

func (a *Agent) Heartbeat(ctx context.Context) error {
	if err := a.validateControlPlaneURL(); err != nil {
		return err
	}

	a.leaseMu.RLock()
	if a.Fenced {
		a.leaseMu.RUnlock()
		return fmt.Errorf("node agent %q is fenced; heartbeat stopped", a.ID)
	}
	leaseGeneration := a.LeaseGeneration
	a.leaseMu.RUnlock()
	if leaseGeneration < 1 {
		return fmt.Errorf("node agent %q is not registered", a.ID)
	}

	payload := heartbeatRequest{
		ID:              a.ID,
		LeaseGeneration: leaseGeneration,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal heartbeat: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(a.ControlPlaneURL, "/")+"/nodes/heartbeat",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create heartbeat request: %w", err)
	}

	req.Header.Set(
		"Authorization",
		"Bearer "+a.Token,
	)
	req.Header.Set("Content-Type", "application/json")

	client := a.Client

	if client == nil {
		client = &http.Client{
			Timeout: 5 * time.Second,
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send heartbeat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		a.leaseMu.Lock()
		a.Fenced = true
		a.leaseMu.Unlock()
		return fmt.Errorf("node agent %q heartbeat rejected as stale generation %d", a.ID, leaseGeneration)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"heartbeat failed with status %d",
			resp.StatusCode,
		)
	}
	return nil
}

func (a *Agent) validateControlPlaneURL() error {
	parsed, err := url.Parse(a.ControlPlaneURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("control plane URL must be an HTTPS URL")
	}
	return nil
}

func (a *Agent) RunHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		a.leaseMu.RLock()
		fenced := a.Fenced
		a.leaseMu.RUnlock()
		if fenced {
			return
		}
		if err := a.Heartbeat(ctx); err != nil {
			slog.Error(
				"node heartbeat failed",
				"node_id", a.ID,
				"error", err,
			)
		}

		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
		}
	}
}
