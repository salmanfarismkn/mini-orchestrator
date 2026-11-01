ALTER TABLE nodes
ADD COLUMN lease_generation BIGINT NOT NULL DEFAULT 1
    CHECK (lease_generation >= 1);

ALTER TABLE nodes
ADD COLUMN registration_id VARCHAR(64);

CREATE TABLE node_registration_history (
    node_id VARCHAR(64) NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    registration_id VARCHAR(64) NOT NULL,
    lease_generation BIGINT NOT NULL CHECK (lease_generation >= 1),
    PRIMARY KEY (node_id, registration_id)
);

CREATE INDEX idx_node_registration_history_generation
ON node_registration_history(node_id, lease_generation);
