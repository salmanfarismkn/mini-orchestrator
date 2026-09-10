ALTER TABLE services
ADD COLUMN deployment_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE workloads
ADD COLUMN deployment_version INTEGER NOT NULL DEFAULT 1;

CREATE INDEX idx_workloads_service_deployment
ON workloads(service_id, deployment_version);