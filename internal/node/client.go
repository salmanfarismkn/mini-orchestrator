package node

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"mini-orchestrator/internal/runtime"
	"mini-orchestrator/internal/transporttls"
)

type Client struct {
	httpClient  *http.Client
	token       string
	mu          sync.Mutex
	nodeClients map[string]*http.Client
}

func NewClient(
	httpClient *http.Client,
	token string,
) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	if httpClient.Transport == nil {
		httpClient.Transport = http.DefaultTransport.(*http.Transport).Clone()
	}

	return &Client{
		httpClient:  httpClient,
		token:       token,
		nodeClients: make(map[string]*http.Client),
	}
}

type CreateContainerRequest struct {
	Name       string `json:"name"`
	WorkloadID string `json:"workload_id"`
	Image      string `json:"image"`
	CPUMillis  int    `json:"cpu_millis"`
	MemoryMB   int    `json:"memory_mb"`
}

type CreateContainerResponse struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Image  string `json:"image"`
	Status string `json:"status"`
}

func (c *Client) CreateContainer(
	ctx context.Context,
	nodeID string,
	nodeAddress string,
	config runtime.ContainerConfig,
) (runtime.Container, error) {
	body, err := json.Marshal(CreateContainerRequest{
		Name:       config.Name,
		WorkloadID: config.WorkloadID,
		Image:      config.Image,
		CPUMillis:  config.CPUMillis,
		MemoryMB:   config.MemoryMB,
	})
	if err != nil {
		return runtime.Container{}, fmt.Errorf("encode create request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://"+nodeAddress+"/containers",
		bytes.NewReader(body),
	)
	if err != nil {
		return runtime.Container{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+c.token,
	)
	req.Header.Set("Content-Type", "application/json")
	c.setTargetNode(req, nodeID)

	resp, err := c.do(req, nodeID)
	if err != nil {
		return runtime.Container{}, fmt.Errorf("send create request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		if resp.StatusCode == http.StatusConflict {
			return runtime.Container{}, fmt.Errorf("%w: node returned container conflict", runtime.ErrContainerConflict)
		}
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
	nodeID string,
	nodeAddress string,
	containerID string,
) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://"+nodeAddress+"/containers/"+containerID+"/start",
		nil,
	)
	if err != nil {
		return fmt.Errorf("create start request: %w", err)
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+c.token,
	)
	c.setTargetNode(req, nodeID)
	resp, err := c.do(req, nodeID)
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
	nodeID string,
	nodeAddress string,
) ([]runtime.Container, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://"+nodeAddress+"/containers",
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create list containers request: %w", err)
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+c.token,
	)
	c.setTargetNode(req, nodeID)
	resp, err := c.do(req, nodeID)
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

func (c *Client) StopContainer(
	ctx context.Context,
	nodeID string,
	nodeAddress string,
	containerID string,
) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://"+nodeAddress+"/containers/"+containerID+"/stop",
		nil,
	)
	if err != nil {
		return fmt.Errorf("create stop request: %w", err)
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+c.token,
	)
	c.setTargetNode(req, nodeID)
	resp, err := c.do(req, nodeID)
	if err != nil {
		return fmt.Errorf("send stop request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf(
			"node returned status %d while stopping container",
			resp.StatusCode,
		)
	}

	return nil
}

func (c *Client) DeleteContainer(
	ctx context.Context,
	nodeID string,
	nodeAddress string,
	containerID string,
) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		"https://"+nodeAddress+"/containers/"+containerID,
		nil,
	)
	if err != nil {
		return fmt.Errorf("create delete request: %w", err)
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+c.token,
	)
	c.setTargetNode(req, nodeID)
	resp, err := c.do(req, nodeID)
	if err != nil {
		return fmt.Errorf("send delete request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf(
			"node returned status %d while deleting container",
			resp.StatusCode,
		)
	}

	return nil
}

func (c *Client) GetContainer(
	ctx context.Context,
	nodeID string,
	nodeAddress string,
	containerID string,
) (runtime.Container, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://"+nodeAddress+"/containers/"+containerID,
		nil,
	)
	if err != nil {
		return runtime.Container{}, fmt.Errorf(
			"create inspect request: %w",
			err,
		)
	}
	req.Header.Set(
		"Authorization",
		"Bearer "+c.token,
	)
	c.setTargetNode(req, nodeID)
	resp, err := c.do(req, nodeID)
	if err != nil {
		return runtime.Container{}, fmt.Errorf(
			"send inspect request: %w",
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return runtime.Container{}, fmt.Errorf(
			"node returned status %d",
			resp.StatusCode,
		)
	}

	var container runtime.Container

	if err := json.NewDecoder(resp.Body).Decode(&container); err != nil {
		return runtime.Container{}, fmt.Errorf(
			"decode container: %w",
			err,
		)
	}

	return container, nil
}

func (c *Client) GetContainerStats(
	ctx context.Context,
	nodeID string,
	nodeAddress string,
	containerID string,
) (runtime.ContainerStats, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://"+nodeAddress+
			"/containers/"+containerID+"/stats",
		nil,
	)
	if err != nil {
		return runtime.ContainerStats{}, fmt.Errorf(
			"create stats request: %w",
			err,
		)
	}

	req.Header.Set(
		"Authorization",
		"Bearer "+c.token,
	)
	c.setTargetNode(req, nodeID)

	resp, err := c.do(req, nodeID)
	if err != nil {
		return runtime.ContainerStats{}, fmt.Errorf(
			"send stats request: %w",
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return runtime.ContainerStats{}, fmt.Errorf(
			"node returned status %d while getting stats",
			resp.StatusCode,
		)
	}

	var stats runtime.ContainerStats

	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return runtime.ContainerStats{}, fmt.Errorf(
			"decode container stats: %w",
			err,
		)
	}

	return stats, nil
}

func (c *Client) setTargetNode(req *http.Request, nodeID string) {
	req.Header.Set(targetNodeHeader, nodeID)
}

func (c *Client) do(req *http.Request, nodeID string) (*http.Response, error) {
	client, err := c.clientForNode(nodeID)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Client) clientForNode(nodeID string) (*http.Client, error) {
	if nodeID == "" {
		return nil, errors.New("target node ID is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if client, ok := c.nodeClients[nodeID]; ok {
		return client, nil
	}

	baseTransport, ok := c.httpClient.Transport.(*http.Transport)
	if !ok {
		return nil, errors.New("node client requires an HTTP transport that supports TLS identity verification")
	}
	transport := baseTransport.Clone()
	tlsConfig := transport.TLSClientConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		tlsConfig = tlsConfig.Clone()
	}
	previousVerify := tlsConfig.VerifyConnection
	expectedIdentity := transporttls.NodeCertificateIdentity(nodeID)
	tlsConfig.InsecureSkipVerify = false
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if previousVerify != nil {
			if err := previousVerify(state); err != nil {
				return err
			}
		}
		if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
			return errors.New("node server certificate was not verified")
		}
		leaf := state.VerifiedChains[0][0]
		if len(leaf.URIs) != 1 || leaf.URIs[0] == nil ||
			leaf.URIs[0].String() != expectedIdentity {
			return fmt.Errorf("node server certificate identity does not match target node %q", nodeID)
		}
		return nil
	}
	transport.TLSClientConfig = tlsConfig

	client := *c.httpClient
	client.Transport = transport
	c.nodeClients[nodeID] = &client
	return &client, nil
}

func (c *Client) CloseIdleConnections() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, client := range c.nodeClients {
		client.CloseIdleConnections()
	}
}
