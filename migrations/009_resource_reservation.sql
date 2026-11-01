ALTER TABLE workloads
ADD COLUMN resource_reserved BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE workloads
SET resource_reserved = TRUE
WHERE node_id IS NOT NULL
  AND actual_state IN ('PENDING', 'SCHEDULED', 'RUNNING', 'FAILED');

CREATE INDEX idx_workloads_resource_reserved
ON workloads(resource_reserved);
