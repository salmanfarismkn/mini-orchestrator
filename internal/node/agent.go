package node

import "log"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Agent struct {
	ID             string
	Address        string
	CPUCapacity    int
	MemoryCapacity int

	ControlPlaneURL string
	Client           *http.Client
}

type registrationRequest struct {
	ID             string `json:"id"`
	Address        string `json:"address"`
	CPUCapacity    int    `json:"cpu_capacity_millis"`
	MemoryCapacity int    `json:"memory_capacity_mb"`
}



func (a *Agent) Register(ctx context.Context) error {
	payload := registrationRequest{
		ID:             a.ID,
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
		a.ControlPlaneURL+"/nodes/register",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create registration request: %w", err)
	}

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

	return nil
}

type heartbeatRequest struct {
	ID string `json:"id"`
}

func (a *Agent) Heartbeat(ctx context.Context) error {
	payload := heartbeatRequest{
		ID: a.ID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal heartbeat: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		a.ControlPlaneURL+"/nodes/heartbeat",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create heartbeat request: %w", err)
	}

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

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"heartbeat failed with status %d",
			resp.StatusCode,
		)
	}

	return nil
}


func (a *Agent) RunHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		if err := a.Heartbeat(ctx); err != nil {
			log.Printf(
				"heartbeat failed for node %s: %v",
				a.ID,
				err,
			)
		}

		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
		}
	}
}