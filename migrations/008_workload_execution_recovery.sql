ALTER TABLE workloads
ADD COLUMN execution_state VARCHAR(32) NOT NULL DEFAULT 'IDLE';

ALTER TABLE workloads
ADD COLUMN execution_error TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_workloads_execution_state
ON workloads(execution_state);
