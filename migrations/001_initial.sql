CREATE TABLE nodes (
    id VARCHAR(64) PRIMARY KEY,
    address VARCHAR(255) NOT NULL,

    cpu_capacity_millis INTEGER NOT NULL CHECK (cpu_capacity_millis > 0),
    memory_capacity_mb INTEGER NOT NULL CHECK (memory_capacity_mb > 0),

    cpu_allocated_millis INTEGER NOT NULL DEFAULT 0
        CHECK (cpu_allocated_millis >= 0),

    memory_allocated_mb INTEGER NOT NULL DEFAULT 0
        CHECK (memory_allocated_mb >= 0),

    status VARCHAR(32) NOT NULL,

    last_heartbeat TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE services (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE,
    image VARCHAR(512) NOT NULL,

    desired_replicas INTEGER NOT NULL
        CHECK (desired_replicas >= 0),

    cpu_request_millis INTEGER NOT NULL
        CHECK (cpu_request_millis > 0),

    memory_request_mb INTEGER NOT NULL
        CHECK (memory_request_mb > 0),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE workloads (
    id VARCHAR(64) PRIMARY KEY,

    service_id VARCHAR(64) NOT NULL
        REFERENCES services(id)
        ON DELETE CASCADE,

    node_id VARCHAR(64)
        REFERENCES nodes(id)
        ON DELETE SET NULL,

    container_id VARCHAR(128),

    image VARCHAR(512) NOT NULL,

    cpu_request_millis INTEGER NOT NULL
        CHECK (cpu_request_millis > 0),

    memory_request_mb INTEGER NOT NULL
        CHECK (memory_request_mb > 0),

    desired_state VARCHAR(32) NOT NULL,
    actual_state VARCHAR(32) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX idx_workloads_service_id
    ON workloads(service_id);

CREATE INDEX idx_workloads_node_id
    ON workloads(node_id);

CREATE INDEX idx_workloads_actual_state
    ON workloads(actual_state);