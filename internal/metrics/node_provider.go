package metrics

import (
	"context"
	"fmt"

	"mini-orchestrator/internal/node"
	"mini-orchestrator/internal/store"
)

type NodeProvider struct {
	store      *store.Postgres
	nodeClient *node.Client
}

func NewNodeProvider(
	store *store.Postgres,
	nodeClient *node.Client,
) *NodeProvider {
	return &NodeProvider{
		store:      store,
		nodeClient: nodeClient,
	}
}

func (p *NodeProvider) GetWorkloadMetrics(
	ctx context.Context,
	workloadID string,
) (WorkloadMetrics, error) {
	workload, err := p.store.GetWorkload(
		ctx,
		workloadID,
	)
	if err != nil {
		return WorkloadMetrics{}, fmt.Errorf(
			"get workload: %w",
			err,
		)
	}

	if workload.NodeID == nil {
		return WorkloadMetrics{}, fmt.Errorf(
			"workload %q has no node",
			workloadID,
		)
	}

	if workload.ContainerID == nil {
		return WorkloadMetrics{}, fmt.Errorf(
			"workload %q has no container",
			workloadID,
		)
	}
	assignedNode, err := p.store.GetNode(ctx, *workload.NodeID)
	if err != nil {
		return WorkloadMetrics{}, fmt.Errorf("get assigned node: %w", err)
	}

	stats, err := p.nodeClient.GetContainerStats(
		ctx,
		assignedNode.ID,
		assignedNode.Address,
		*workload.ContainerID,
	)
	if err != nil {
		return WorkloadMetrics{}, fmt.Errorf(
			"get container stats: %w",
			err,
		)
	}

	return WorkloadMetrics{
		CPUUsageMillis: stats.CPUUsageMillis,
	}, nil
}
