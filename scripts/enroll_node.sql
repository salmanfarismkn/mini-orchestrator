-- Controlled node enrollment for node certificate identity binding.
-- Run with psql variables:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
--     -v node_id=worker-01 -v certificate_identity=spiffe://mini-orchestrator/node/worker-01 \
--     -v address=127.0.0.1:9001 -v cpu_capacity_millis=4000 -v memory_capacity_mb=8192 \
--     -f scripts/enroll_node.sql

BEGIN;

WITH enrolled AS (
    UPDATE nodes
    SET certificate_identity = :'certificate_identity', updated_at = NOW()
    WHERE id = :'node_id'
      AND certificate_identity IS NULL
    RETURNING id
)
INSERT INTO nodes (
    id, address, cpu_capacity_millis, memory_capacity_mb,
    status, last_heartbeat, certificate_identity
)
SELECT
    :'node_id', :'address', :'cpu_capacity_millis'::INTEGER,
    :'memory_capacity_mb'::INTEGER, 'READY', NOW(), :'certificate_identity'
WHERE NOT EXISTS (SELECT 1 FROM enrolled)
  AND NOT EXISTS (SELECT 1 FROM nodes WHERE id = :'node_id');

DO $$
BEGIN
    IF :'certificate_identity' <> 'spiffe://mini-orchestrator/node/' || :'node_id' THEN
        RAISE EXCEPTION 'certificate identity must match spiffe://mini-orchestrator/node/% for node %', :'node_id', :'node_id';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM nodes
        WHERE id = :'node_id'
          AND certificate_identity = :'certificate_identity'
    ) THEN
        RAISE EXCEPTION 'node enrollment failed or identity is already assigned to another node';
    END IF;
END $$;

COMMIT;
