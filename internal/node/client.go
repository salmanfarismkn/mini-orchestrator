package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"mini-orchestrator/internal/runtime"
)

type Client struct {
	httpClient *http.Client
}

func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	return &Client{
		httpClient: httpClient,
	}
}

type CreateContainerRequest struct {
	Name      string `json:"name"`
	Image     string `json:"image"`
	CPUMillis int    `json:"cpu_millis"`
	MemoryMB  int    `json:"memory_mb"`
}

type CreateContainerResponse struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Image   string `json:"image"`
	Status  string `json:"status"`
}

func (c *Client) CreateContainer(
	ctx context.Context,
	nodeAddress string,
	config runtime.ContainerConfig,
) (runtime.Container, error) {
	body, err := json.Marshal(CreateContainerRequest{
		Name:      config.Name,
		Image:     config.Image,
		CPUMillis: config.CPUMillis,
		MemoryMB:  config.MemoryMB,
	})
	if err != nil {
		return runtime.Container{}, fmt.Errorf("encode create request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://"+nodeAddress+"/containers",
		bytes.NewReader(body),
	)
	if err != nil {
		return runtime.Container{}, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return runtime.Container{}, fmt.Errorf("send create request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return runtime.Container{}, fmt.Errorf(
			"node returned status %d",
			resp.StatusCode,
		)
	}

	var result CreateContainerResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return runtime.Container{}, fmt.Errorf(
			"decode create response: %w",
			err,
		)
	}

	return runtime.Container{
		ID:     result.ID,
		Name:   result.Name,
		Image:  result.Image,
		Status: result.Status,
	}, nil
}

func (c *Client) StartContainer(
	ctx context.Context,
	nodeAddress string,
	containerID string,
) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://"+nodeAddress+"/containers/"+containerID+"/start",
		nil,
	)
	if err != nil {
		return fmt.Errorf("create start request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send start request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf(
			"node returned status %d while starting container",
			resp.StatusCode,
		)
	}

	return nil
}

func (c *Client) ListContainers(
	ctx context.Context,
	nodeAddress string,
) ([]runtime.Container, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://"+nodeAddress+"/containers",
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create list containers request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send list containers request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"node returned status %d while listing containers",
			resp.StatusCode,
		)
	}

	var containers []runtime.Container

	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return nil, fmt.Errorf(
			"decode container list: %w",
			err,
		)
	}

	return containers, nil
}