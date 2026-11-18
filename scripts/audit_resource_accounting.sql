\set ON_ERROR_STOP on

-- Read-only pre-migration audit for migration 009_resource_reservation.sql.
-- Migration 009 treats node_id plus actual_state IN
-- ('PENDING', 'SCHEDULED', 'RUNNING', 'FAILED') as reservation ownership.
-- That predicate is legacy-migration inference, not proof that node counters
-- are currently correct. In particular, FAILED may reflect a pre-release state.
-- This transaction prevents accidental writes by enforcing READ ONLY.
BEGIN TRANSACTION READ ONLY;

\echo '1. Per-node recorded allocation counters'
SELECT
    id AS node_id,
    status,
    cpu_capacity_millis,
    cpu_allocated_millis AS recorded_cpu_allocated,
    memory_capacity_mb,
    memory_allocated_mb AS recorded_memory_allocated
FROM nodes
ORDER BY id;

\echo '2-3. Recorded versus reconstructed allocated totals for workloads matching migration 009 backfill'
WITH reconstructed AS (
    SELECT
        node_id,
        SUM(cpu_request_millis) AS reconstructed_cpu_allocated,
        SUM(memory_request_mb) AS reconstructed_memory_allocated,
        COUNT(*) AS matching_workload_count
    FROM workloads
    WHERE node_id IS NOT NULL
      AND actual_state IN ('PENDING', 'SCHEDULED', 'RUNNING', 'FAILED')
    GROUP BY node_id
)
SELECT
    n.id AS node_id,
    n.cpu_allocated_millis AS recorded_cpu_allocated,
    COALESCE(r.reconstructed_cpu_allocated, 0) AS reconstructed_cpu_allocated,
    n.cpu_allocated_millis - COALESCE(r.reconstructed_cpu_allocated, 0) AS cpu_difference,
    n.memory_allocated_mb AS recorded_memory_allocated,
    COALESCE(r.reconstructed_memory_allocated, 0) AS reconstructed_memory_allocated,
    n.memory_allocated_mb - COALESCE(r.reconstructed_memory_allocated, 0) AS memory_difference,
    COALESCE(r.matching_workload_count, 0) AS migration_009_matching_workload_count
FROM nodes n
LEFT JOIN reconstructed r ON r.node_id = n.id
ORDER BY n.id;

\echo '4. Workloads with node assignments but states excluded by migration 009'
SELECT
    w.id AS workload_id,
    w.node_id,
    w.desired_state,
    w.actual_state,
    w.container_id,
    w.cpu_request_millis,
    w.memory_request_mb
FROM workloads w
WHERE w.node_id IS NOT NULL
  AND w.actual_state NOT IN ('PENDING', 'SCHEDULED', 'RUNNING', 'FAILED')
ORDER BY w.node_id, w.id;

\echo '5. Active workloads without node assignments'
SELECT
    w.id AS workload_id,
    w.desired_state,
    w.actual_state,
    w.container_id,
    w.cpu_request_millis,
    w.memory_request_mb
FROM workloads w
WHERE w.node_id IS NULL
  AND (
       w.desired_state IN ('PENDING', 'SCHEDULED', 'RUNNING')
       OR w.actual_state IN ('PENDING', 'SCHEDULED', 'RUNNING')
      )
ORDER BY w.id;

\echo '6. Invalid allocation counters: negative values or allocation above capacity'
SELECT
    id AS node_id,
    status,
    cpu_capacity_millis,
    cpu_allocated_millis,
    memory_capacity_mb,
    memory_allocated_mb,
    (cpu_allocated_millis < 0) AS negative_cpu,
    (memory_allocated_mb < 0) AS negative_memory,
    (cpu_allocated_millis > cpu_capacity_millis) AS cpu_above_capacity,
    (memory_allocated_mb > memory_capacity_mb) AS memory_above_capacity
FROM nodes
WHERE cpu_allocated_millis < 0
   OR memory_allocated_mb < 0
   OR cpu_allocated_millis > cpu_capacity_millis
   OR memory_allocated_mb > memory_capacity_mb
ORDER BY id;

\echo '7a. Workloads assigned to nodes that do not exist'
SELECT
    w.id AS workload_id,
    w.node_id,
    w.desired_state,
    w.actual_state,
    w.cpu_request_millis,
    w.memory_request_mb
FROM workloads w
LEFT JOIN nodes n ON n.id = w.node_id
WHERE w.node_id IS NOT NULL
  AND n.id IS NULL
ORDER BY w.id;

\echo '7b. Ambiguous or inconsistent workload ownership state'
SELECT
    w.id AS workload_id,
    w.node_id,
    w.desired_state,
    w.actual_state,
    w.container_id,
    w.cpu_request_millis,
    w.memory_request_mb,
    CASE
        WHEN w.node_id IS NULL
         AND w.desired_state IN ('SCHEDULED', 'RUNNING') THEN
            'scheduled/running desired state without node'
        WHEN w.node_id IS NULL
         AND w.actual_state IN ('SCHEDULED', 'RUNNING', 'FAILED') THEN
            'active/failed actual state without node'
        WHEN w.node_id IS NOT NULL
         AND w.actual_state = 'STOPPED' THEN
            'node assigned while workload is stopped'
        WHEN w.node_id IS NOT NULL
         AND w.actual_state = 'FAILED' THEN
            'node assigned while workload is failed; legacy release semantics may or may not have cleared it'
        WHEN w.node_id IS NOT NULL
         AND w.actual_state IN ('PENDING', 'SCHEDULED', 'RUNNING') THEN
            'migration 009 would mark reservation owned'
        WHEN w.node_id IS NOT NULL THEN
            'node assigned with an unknown or unexpected state'
        ELSE 'unexpected node/ownership combination'
    END AS ownership_anomaly
FROM workloads w
WHERE
      (w.node_id IS NULL AND (
          w.desired_state IN ('SCHEDULED', 'RUNNING')
          OR w.actual_state IN ('SCHEDULED', 'RUNNING', 'FAILED')
      ))
   OR (w.node_id IS NOT NULL AND (
          w.actual_state IN ('PENDING', 'STOPPED', 'FAILED')
          OR w.actual_state NOT IN ('PENDING', 'SCHEDULED', 'RUNNING', 'FAILED', 'STOPPED')
      ))
ORDER BY w.id;

COMMIT;
