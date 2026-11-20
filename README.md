# Mini Container Orchestrator

A small Go project that demonstrates container workload orchestration concepts:
PostgreSQL-backed desired and actual state, node registration and heartbeats,
capacity-aware scheduling, Docker execution through HTTP node agents, rolling
deployments, service deletion, and CPU-based autoscaling.

This is a learning and portfolio project, not a production container platform.
The repository does not provide a packaged multi-host deployment, a production
PKI lifecycle, or a claim of production security.

## Scope and current limitations

- The control plane persists node, service, and workload state in PostgreSQL.
- Node agents expose Docker operations over HTTPS with mutual TLS and bearer
  authentication.
- The scheduler reserves requested CPU and memory against node capacity.
- Reconciliation manages workload execution, missing containers, deployment
  transitions, and deletion.
- Autoscaling consumes CPU usage obtained from the node-agent metrics endpoint.
- Compose currently defines PostgreSQL only. It does not define control-plane
  or agent services.
- The node-agent binary supports per-instance ID, listener, advertised address,
  and capacities through environment variables. The opt-in process acceptance
  test starts two actual node-agent executables, but both processes on one host
  still connect to the same Docker daemon; it does not emulate separate hosts
  or Docker Engines.

## Architecture

```text
                         +------------------------+
                         |       Operator/API     |
                         | bearer auth for service|
                         | management endpoints   |
                         +-----------+------------+
                                     |
                          HTTPS; bearer token
                          client cert where used
                                     |
             +-----------------------v----------------------+
             | Control plane (:8080)                        |
             | HTTP API, scheduler, controllers,            |
             | reconciliation, health/readiness, metrics     |
             +-----------+----------------------+-----------+
                         |                      |
                         | PostgreSQL           | HTTPS + mTLS
                         |                      | + bearer token
               +---------v---------+     +------v------------------+
               | PostgreSQL        |     | Node agent (:9001)      |
               | services, nodes,  |     | authenticated Docker   |
               | workloads         |     | runtime API            |
               +-------------------+     +-----------+-------------+
                                                    |
                                                    | Docker Engine API
                                                    v
                                               Containers
```

### Responsibilities

- `cmd/control-plane`: PostgreSQL connection, controllers, scheduler,
  reconciler, autoscaler, and HTTPS API server.
- `cmd/node-agent`: authenticated HTTPS API, node registration/heartbeat, and
  Docker runtime adapter.
- `internal/store`: PostgreSQL models, state transitions, and resource
  reservations.
- `internal/scheduler`: node eligibility and placement.
- `internal/controller`: replicas, rolling deployments, deletion, and status.
- `internal/reconciler`: scheduled execution and runtime drift recovery.
- `internal/autoscaler`: stabilization, cooldown, bounds, and scaling steps.
- `internal/transporttls`: TLS certificate/CA loading for both binaries.
- `migrations`: ordered SQL migrations. Startup does not apply migrations
  automatically.

## Technology and design choices

- Go version declared in `go.mod`; PostgreSQL via pgx; Docker Engine client
  library; standard-library `net/http`.
- PostgreSQL is the durable state store. Resource reservation and workload
  failure/stop transitions are persisted transactionally.
- Node communication uses HTTPS and configured trust material. Agent routes
  require a client certificate and bearer token. Control-plane registration
  and heartbeat require verified client TLS and bearer token. Service
  management uses bearer authentication.
- `/healthz` is process liveness. `/readyz` performs a bounded database ping.
- The autoscaler has an injectable metrics provider, enabling deterministic
  unit/acceptance tests without pretending fake values are Docker measurements.

## Prerequisites

- Go matching the project `go.mod`.
- PostgreSQL 18 for the Compose setup, plus `psql` for applying migrations.
- Docker Engine access for the node-agent binary and direct runtime test.
- Docker Compose v2 for the PostgreSQL service.
- OpenSSL for local development certificates.
- TLS certificate files whose SANs match the hostnames/IPs clients actually use.

Docker Engine access can grant substantial host privileges. Restrict access to
the node agent host and protect the Docker socket according to the host's
security policy. Do not expose an unauthenticated Docker TCP listener.

## Configuration

The control plane and node agent both require:

| Variable | Required | Description |
|---|---:|---|
| `ORCHESTRATOR_NODE_TOKEN` | Yes | Shared bearer token for node registration/heartbeat and node-agent routes. Do not use it for operator APIs. Supply a unique secret through a secret manager or local environment; never commit it. |
| `ORCHESTRATOR_OPERATOR_TOKEN` | Yes | Control-plane-only bearer token for operator APIs, node drain/reactivation, metrics, and service/deployment APIs. It must differ from `ORCHESTRATOR_NODE_TOKEN`. |
| `ORCHESTRATOR_TLS_CERT_FILE` | Yes | PEM leaf certificate used for HTTPS and as the presented client certificate. The control plane requires the control-plane URI SAN; each node agent requires the URI SAN for its configured node ID. |
| `ORCHESTRATOR_TLS_KEY_FILE` | Yes | Private key corresponding to the leaf certificate. Keep it secret and outside version control. |
| `ORCHESTRATOR_TLS_CA_FILE` | Yes | PEM CA bundle trusted for peer certificates. |

Additional variables:

| Variable | Required | Used by | Description |
|---|---:|---|---|
| `DATABASE_URL` | Yes | Control plane | PostgreSQL URL/DSN. |
| `ORCHESTRATOR_CONTROL_PLANE_URL` | Yes | Node agent | HTTPS URL of the control plane, including a hostname/IP present in its certificate SAN. |
| `ORCHESTRATOR_NODE_ADDRESS` | Yes | Node agent | Advertised HTTPS address for this agent; hostname/IP must match its certificate SAN. |
| `ORCHESTRATOR_NODE_ID` | No | Node agent | Node ID; default `worker-01` for a single-agent development setup. Set a unique ID per process. |
| `ORCHESTRATOR_LISTEN_ADDR` | No | Node agent | Local bind address; default `:9001`. |
| `ORCHESTRATOR_NODE_CPU_MILLIS` | No | Node agent | Advertised CPU capacity in millicores; default `4000`. |
| `ORCHESTRATOR_NODE_MEMORY_MB` | No | Node agent | Advertised memory capacity in MB; default `8192`. |
| `ORCHESTRATOR_SHUTDOWN_TIMEOUT` | No | Control plane and node agent | Positive integer number of seconds allowed for HTTP and background-component shutdown, with a maximum of `300`; default `30`. |
| `POSTGRES_PASSWORD` | Yes for Compose | Compose | PostgreSQL password; Compose fails configuration when unset. |
| `POSTGRES_PORT` | No | Compose | Host port, default `5432`, bound to `127.0.0.1`. |

Both processes handle `SIGINT` and `SIGTERM`. The control plane stops its HTTP listener, cancels background loops, waits for bounded loop completion, and closes PostgreSQL after the loops stop. The node agent cancels heartbeat activity, shuts down its HTTP listener, and closes the Docker client without stopping or removing containers. Shutdown timeout expiration force-closes HTTP connections and is reported as a non-graceful shutdown.

For multiple local agents, configure a distinct node ID and listen address for
each process. The node listener and the control-plane-advertised address are
separate settings: bind to `:9001` while registering `127.0.0.1:9001`, for
example. The advertised address must be reachable from the control plane and
its hostname/IP must appear in the node certificate SANs. The control-plane
listener is fixed at `:8080`.
The token, TLS paths, HTTPS control-plane URL, and advertised address are
required. Listener and capacity defaults are for one development agent; node
IDs must be unique when multiple agents register.

## Local development certificate provisioning

Use the repository script to generate a development-only CA, a control-plane
leaf certificate, and a separate leaf certificate/private key for every node:

```powershell
.\scripts\provision_dev_certs.ps1
```

The default output directory is `$env:USERPROFILE\.mini-orchestrator\local-certs`.
It provisions `worker-01` and `worker-02` by default. Provision additional nodes
with:

```powershell
.\scripts\provision_dev_certs.ps1 -NodeIds worker-01,worker-02,worker-03
```

The script refuses to overwrite existing credentials unless `-Force` is supplied.
The control-plane leaf has URI SAN
`spiffe://mini-orchestrator/control-plane`. It issues one leaf certificate per node with:

- URI SAN `spiffe://mini-orchestrator/node/<node-id>`
- DNS SAN `localhost`
- IP SAN `127.0.0.1`
- `serverAuth` and `clientAuth` extended key usage

Every leaf is signed by the single development CA. The CA private key is kept
separate at `ca-key.pem` and is not embedded in control-plane or node credentials.
Generated PEM files and private keys are ignored by Git. Do not commit or share the
CA private key or node private keys. These certificates are development-only and
do not provide certificate rotation, revocation, or production PKI support.

The control plane presents its control-plane identity when calling agents. Agents
present their node identities only when registering and heartbeating with the
control plane. Agent APIs require a verified control-plane URI identity and a
target-node header matching the receiving agent. The control plane also verifies
that each agent's server certificate URI identity matches the target node ID.
Node registration and heartbeat continue to bind node IDs to node certificate
identities and lease generations.

Containers created by an agent are labeled with that agent's configured node ID.
Container inspection, statistics, listing, start, stop, and removal are limited to
containers carrying the local node ID and workload labels. Containers created
before this label was introduced are not automatically adopted or relabeled;
follow the controlled upgrade procedure below before upgrading agents that manage
existing workloads.

The application loads a certificate/key pair as both an HTTPS server identity
and client identity. The control plane trusts the development CA for client
certificates, and node-agent HTTPS servers require verified client certificates.
In each later PowerShell terminal, set `$certDir` again to the same path before
using the certificate files.

## Upgrade from unlabeled containers

This is a maintenance-window procedure; it does not migrate or adopt containers.
Unlabeled containers are discoverable from the host's Docker daemon (`docker ps
-a` and `docker inspect`), but not from the node-agent list API. A create request
can inspect an existing deterministic `workload-<workload-id>` name, but adopts
it only if the orchestrator, workload, and local-node labels, exact image,
resource limits, and name all match. An unlabeled container is rejected; the
name alone is not proof of ownership. Direct inspect, stats, start, stop, and
remove operations also reject containers without the local node and workload
labels. Only workloads safely stopped and removed through the existing
deployment before upgrade are eligible for controlled recreation.

The control plane therefore cannot safely manage a legacy container through the
new agent API. Runtime reconciliation may treat an omitted legacy container as
missing, fail its database workload, and release its reservation without
stopping the still-running Docker container. A replacement workload could then
be started beside it. Do not upgrade agents while they are responsible for
active unlabeled workloads.

1. **Back up and check PostgreSQL.** Take a restorable backup and retain it
   outside the database host. For example:

   ```powershell
   pg_dump --format=custom --file="orchestrator-before-agent-upgrade.dump" "$env:DATABASE_URL"
   ```

   Verify that the backup can be read with `pg_restore --list
   orchestrator-before-agent-upgrade.dump`. Check that reserved workloads have
   node assignments and that persisted node counters equal reserved workload
   totals before making changes:

   ```powershell
   psql "$env:DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT id FROM workloads WHERE resource_reserved AND node_id IS NULL"
   psql "$env:DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT n.id, n.cpu_allocated_millis, COALESCE(SUM(w.cpu_request_millis) FILTER (WHERE w.resource_reserved), 0) AS reserved_cpu, n.memory_allocated_mb, COALESCE(SUM(w.memory_request_mb) FILTER (WHERE w.resource_reserved), 0) AS reserved_memory FROM nodes n LEFT JOIN workloads w ON w.node_id = n.id GROUP BY n.id, n.cpu_allocated_millis, n.memory_allocated_mb HAVING n.cpu_allocated_millis <> COALESCE(SUM(w.cpu_request_millis) FILTER (WHERE w.resource_reserved), 0) OR n.memory_allocated_mb <> COALESCE(SUM(w.memory_request_mb) FILTER (WHERE w.resource_reserved), 0)"
   ```

   Both queries should return no rows. Resolve inconsistencies before proceeding;
   do not repair counters by hand based on container names.
2. **Record service configuration.** Save each service's configuration and
   deployment settings from the operator API so the services can be recreated
   after the upgrade. Protect this export as operational data.
3. **Quiesce through the currently working version.** With the existing control
   plane and agents still running, drain each affected node with the operator
   API (`POST /nodes/<node-id>/drain`) to prevent new reservations. Then delete
   every service that still has workloads on those nodes through
   `DELETE /services/<service-id>` and wait for each service to reach `DELETED`.
   Service deletion is service-wide and may stop replicas on other nodes too.
   The old agent can still stop and remove its containers through its existing
   protocol. Expect workload downtime; do not proceed if service deletion does
   not finish.
4. **Verify the old workloads are gone.** On each node, inspect Docker directly
   (`docker ps -a` and `docker inspect`) and correlate full container IDs with
   database workload records. Names are search hints only. Confirm that no
   legacy service container remains running, workload reservations are released,
   and node counters agree with the database query in step 1. Do not manually
   alter labels or database ownership to make a container appear adopted.
5. **Stop processes and stage credentials.** Stop the control plane and agents.
   Keep the old binaries and credentials secured for rollback. Configure a
   distinct control-plane certificate identity
   (`spiffe://mini-orchestrator/control-plane`) and one node identity
   (`spiffe://mini-orchestrator/node/<node-id>`) per agent, all under the
   configured trusted CA. Use separate, protected control-plane and node
   private keys. Set `ORCHESTRATOR_OPERATOR_TOKEN` to a secret distinct from
   `ORCHESTRATOR_NODE_TOKEN`; the node token remains shared by nodes in the
   current design.

   The development certificate script creates a new development CA and leaf
   certificates. Use a new output directory and update the trust bundle and
   certificates for every process together; do not use `-Force` as an
   incremental node-certificate operation. Production certificate issuance and
   CA distribution must be provided by the operator's PKI.
6. **Upgrade as a coordinated identity change.** Install the new binaries and
   credentials while processes are stopped. Start the control plane first, then
   start each node agent with its matching node ID, certificate, and address.
   Do not run a mixed old/new certificate-identity deployment. Confirm node
   registration and health; keep nodes draining until checks pass, then
   reactivate them deliberately.
7. **Recreate and verify workloads.** Recreate services from the saved
   configuration. For each new container, verify Docker labels directly:

   ```powershell
   docker inspect --format '{{json .Config.Labels}}' <container-id>
   ```

   Confirm `mini-orchestrator.node-id` equals the assigned node and
   `mini-orchestrator.workload-id` equals the database workload ID. Re-run the
   reservation/counter checks from step 1 and verify service health before
   returning nodes to normal operation.

**Recovery limits:** If the old agent or service shutdown cannot safely remove a
legacy container, leave that node drained and do not let the new controller
reconcile its active legacy workload. Use an application-specific recovery plan
to stop or replace it, then verify both Docker state and PostgreSQL accounting.
Do not relabel or adopt it based on its name. A database restore does not undo
Docker side effects; stop orchestration and reconcile container state before
restoring a backup. This procedure does not provide automatic migration,
execution fencing, or exactly-once execution.

## Database and startup

Compose only starts PostgreSQL. It uses a fixed container name and a persistent
named volume, so use it only in an isolated local Docker environment where
`mini-orchestrator-postgres` and the Compose volume are known to belong to this
project. Do not run `docker compose down -v` against data you need. The
integration tests below instead create a unique PostgreSQL container without a
volume and remove only that test-created container.

In an isolated local environment, set a generated local-only password and start
PostgreSQL:

```powershell
$env:POSTGRES_PASSWORD = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
docker compose -f deploy\docker-compose.yml up -d postgres
```

Apply all migrations in filename order to a **fresh database**. This does not
drop or reset data:

```powershell
$env:DATABASE_URL = "postgres://orchestrator:$env:POSTGRES_PASSWORD@127.0.0.1:5432/orchestrator?sslmode=disable"
Get-ChildItem .\migrations\*.sql | Sort-Object Name | ForEach-Object {
  psql "$env:DATABASE_URL" -v ON_ERROR_STOP=1 -f $_.FullName
}
```

Node registration is deliberately limited to pre-enrolled certificate
identities; the registration API does not create its own trust binding. Before
starting each new agent, insert its expected node ID, advertised address,
capacities, and exact node URI identity. Start it as `UNHEALTHY` so it cannot be
scheduled before its authenticated registration succeeds. For the two example
agents (adjust addresses and capacities to match their agent configuration):

```powershell
psql "$env:DATABASE_URL" -v ON_ERROR_STOP=1 -c "INSERT INTO nodes (id, address, cpu_capacity_millis, memory_capacity_mb, status, certificate_identity) VALUES ('worker-01', '127.0.0.1:9001', 4000, 8192, 'UNHEALTHY', 'spiffe://mini-orchestrator/node/worker-01'), ('worker-02', '127.0.0.1:9002', 4000, 8192, 'UNHEALTHY', 'spiffe://mini-orchestrator/node/worker-02')"
```

For subsequent starts, keep the existing enrollment row and identity; do not
reinsert or change certificate identity as part of ordinary agent restart.
Certificate rotation and safe identity rebinding are not implemented.

Build and start the control plane in one terminal:

```powershell
$env:ORCHESTRATOR_NODE_TOKEN = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
$env:ORCHESTRATOR_OPERATOR_TOKEN = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
$certDir = Join-Path $env:USERPROFILE '.mini-orchestrator\local-certs'
$env:ORCHESTRATOR_TLS_CERT_FILE = Join-Path $certDir 'control-plane.pem'
$env:ORCHESTRATOR_TLS_KEY_FILE = Join-Path $certDir 'control-plane-key.pem'
$env:ORCHESTRATOR_TLS_CA_FILE = Join-Path $certDir 'ca.pem'
New-Item -ItemType Directory -Force .\bin | Out-Null
go build -o .\bin\control-plane.exe .\cmd\control-plane
.\bin\control-plane.exe
```

Build the node agent executable once:

```powershell
go build -o .\bin\node-agent.exe .\cmd\node-agent
```

The angle-bracket token above is a placeholder, not a secret value. Do not
include quotes/brackets in the actual token. Two agent processes on one host
share the bearer token but use distinct certificate/private-key pairs and ports.
Open a separate terminal for each process and set the corresponding values below.
The agent processes require access to Docker Engine. On one development host they
share that Engine and are not independent Docker hosts.

First agent:

```powershell
$env:ORCHESTRATOR_NODE_TOKEN = '<same locally generated token>'
$certDir = Join-Path $env:USERPROFILE '.mini-orchestrator\local-certs'
$env:ORCHESTRATOR_TLS_CERT_FILE = Join-Path $certDir 'worker-01.pem'
$env:ORCHESTRATOR_TLS_KEY_FILE = Join-Path $certDir 'worker-01-key.pem'
$env:ORCHESTRATOR_TLS_CA_FILE = Join-Path $certDir 'ca.pem'
$env:ORCHESTRATOR_CONTROL_PLANE_URL = 'https://127.0.0.1:8080'
$env:ORCHESTRATOR_NODE_ID = 'worker-01'
$env:ORCHESTRATOR_LISTEN_ADDR = ':9001'
$env:ORCHESTRATOR_NODE_ADDRESS = '127.0.0.1:9001'
.\bin\node-agent.exe
```

Second agent, in another terminal:

```powershell
$env:ORCHESTRATOR_NODE_TOKEN = '<same locally generated token>'
$certDir = Join-Path $env:USERPROFILE '.mini-orchestrator\local-certs'
$env:ORCHESTRATOR_TLS_CERT_FILE = Join-Path $certDir 'worker-02.pem'
$env:ORCHESTRATOR_TLS_KEY_FILE = Join-Path $certDir 'worker-02-key.pem'
$env:ORCHESTRATOR_TLS_CA_FILE = Join-Path $certDir 'ca.pem'
$env:ORCHESTRATOR_CONTROL_PLANE_URL = 'https://127.0.0.1:8080'
$env:ORCHESTRATOR_NODE_ID = 'worker-02'
$env:ORCHESTRATOR_LISTEN_ADDR = ':9002'
$env:ORCHESTRATOR_NODE_ADDRESS = '127.0.0.1:9002'
.\bin\node-agent.exe
```

The server certificate is checked against each agent's advertised address
before it listens; client connections verify the control-plane certificate
against `ORCHESTRATOR_CONTROL_PLANE_URL`. For Docker bridge/container networks,
do not assume that `127.0.0.1` is reachable from the control plane. Use an
address reachable in that network and include it in the appropriate SAN.

## API examples

Operator APIs require `Authorization: Bearer <operator-token>`, configured as
`ORCHESTRATOR_OPERATOR_TOKEN`; this must be distinct from the node token.
Registration and heartbeat use `ORCHESTRATOR_NODE_TOKEN` and additionally
require a verified client certificate. API examples below use `curl.exe`; pass local development
certificates where appropriate and never paste real tokens into shared logs.
Set the private certificate directory for each terminal before running the
commands:

```powershell
$env:ORCHESTRATOR_OPERATOR_TOKEN = '<same control-plane operator token>'
$certDir = Join-Path $env:USERPROFILE '.mini-orchestrator\local-certs'
```

Create a service:

```powershell
curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_NODE_TOKEN" `
  -H "Content-Type: application/json" `
  -d '{"id":"demo-api","name":"demo-api","image":"nginx:alpine","replicas":2,"cpu_request_millis":100,"memory_request_mb":128,"max_surge":1,"max_unavailable":0}' `
  https://localhost:8080/services
```

Read the service and observe status:

```powershell
curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_NODE_TOKEN" `
  https://localhost:8080/services/demo-api
```

Update the image to initiate a rolling deployment:

```powershell
curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_NODE_TOKEN" `
  -H "Content-Type: application/json" `
  -X PUT `
  -d '{"name":"demo-api","image":"nginx:1.27-alpine","replicas":2,"cpu_request_millis":100,"memory_request_mb":128,"max_surge":1,"max_unavailable":0}' `
  https://localhost:8080/services/demo-api
```

Inspect deployment progress and node capacity:

```powershell
curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_OPERATOR_TOKEN" `
  https://localhost:8080/services/demo-api/deployment

curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_OPERATOR_TOKEN" `
  https://localhost:8080/nodes/worker-01
```

Drain a node to stop new workload reservations, then reactivate it when it is
ready to schedule again:

```powershell
curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_OPERATOR_TOKEN" `
  -X POST https://localhost:8080/nodes/worker-01/drain

curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_OPERATOR_TOKEN" `
  -X POST https://localhost:8080/nodes/worker-01/reactivate
```

Draining does not stop or move workloads already assigned to the node. Check
the node's assigned workload count and resource counters before maintenance;
this API is not a workload evacuation mechanism.

Delete:

```powershell
curl.exe --cacert "$certDir\ca.pem" `
  -H "Authorization: Bearer $env:ORCHESTRATOR_NODE_TOKEN" `
  -X DELETE https://localhost:8080/services/demo-api
```

Liveness, readiness, and metrics:

```powershell
curl.exe --cacert "$certDir\ca.pem" https://localhost:8080/healthz
curl.exe --cacert "$certDir\ca.pem" https://localhost:8080/readyz
curl.exe --cacert "$certDir\ca.pem" -H "Authorization: Bearer $env:ORCHESTRATOR_NODE_TOKEN" https://localhost:8080/metrics
curl.exe --cacert "$certDir\ca.pem" -H "Authorization: Bearer $env:ORCHESTRATOR_NODE_TOKEN" https://localhost:8080/services/demo-api/deployment
```

The deployment detail endpoint reports the persisted deployment version and
status and desired/current-version workload counts. The current-version
workload count includes every workload row with that version, including
stopped rows. Running and failed counts include current-version rows whose
persisted `actual_state` is exactly `RUNNING` or `FAILED`, respectively,
regardless of desired stop intent; older versions are excluded. Progress is
`min(running count / desired replicas, 1) * 100`; zero desired replicas is
100%. This measures persisted RUNNING target attainment, not application
health or full rollout completion. The current model has no separate workload
readiness signal, so no ready-replica count is returned. Failure reasons are
bounded persisted codes, not raw runtime error text. The endpoint is read-only
and uses the service-management bearer token. A standalone deployment ID and
a deployment-specific update timestamp are not stored; the service ID and
deployment version identify the current rollout, and `deployment_started_at`
is the persisted start time. `service_created_at` is the service creation
time, not the current rollout creation time.

The authenticated `/metrics` scrape includes low-cardinality gauges aggregated
from one PostgreSQL statement: deployment counts by fixed status, desired and
running replicas, failed workload count, node counts by fixed status, and
resource allocation totals from node counters. It does not expose IDs or
arbitrary error strings. Desired/running replica gauges are for active services;
failed workload gauge counts persisted workload rows, and allocation gauges sum
persisted node counters. Reconciliation errors remain a process-local counter.

## Tests and reproducible demonstrations

Run standard checks:

```powershell
$unformatted = Get-ChildItem -Recurse -Filter *.go | ForEach-Object {
  gofmt -l $_.FullName
}
if ($unformatted) { $unformatted; exit 1 }
go test ./...
go vet ./...
go test -race ./...
```

Opt-in integration tests require Docker daemon access and permission to pull
`postgres:18` if it is not cached. They create a uniquely named PostgreSQL test
container, use a random published loopback port, generate temporary test-only
TLS credentials, apply every migration to the fresh test database, and clean up
the container they created. They do not use Compose's persistent volume or a
developer database.

```powershell
$env:MINI_ORCHESTRATOR_INTEGRATION = '1'
go test ./internal/reconciler -run 'Integration$' -count=1 -v
```

For the store-level drain/reservation transaction checks, point
`DATABASE_URL` at a migrated PostgreSQL database and run:

```powershell
$env:DATABASE_URL = 'postgres://user:password@127.0.0.1:5432/orchestrator?sslmode=disable'
go test ./internal/store -run 'Integration$' -count=1 -v
```

Deployment operational-status integration checks also use a migrated,
dedicated PostgreSQL test database:

```powershell
go test ./internal/store ./internal/api -run 'OperationalStatus.*Integration|OperationalMetricsIntegration' -count=1 -v
```

The standard integration suite demonstrates two in-process agents with unique
node IDs, mTLS and bearer-protected registration/heartbeat,
scheduling/capacity accounting, service deletion, simulated container
execution, missing-container recovery, rolling deployment state transitions,
and deterministic fake-metrics autoscaling.

The real-process acceptance test additionally builds and starts the actual
control-plane and two node-agent executables. It requires Docker access and
uses an isolated PostgreSQL container, temporary TLS files, random agent
ports, a unique service, and real test-created Docker workloads. It covers
authenticated registration, real heartbeat updates, scheduling/resource
accounting, removal/replacement, heartbeat expiry/recovery, and deletion.
Before simulating a node loss it removes that node's test-owned containers
through the local Docker Engine; terminating an agent process alone does not
stop Docker containers on the same host.

```powershell
$env:MINI_ORCHESTRATOR_INTEGRATION = '1'
$env:MINI_ORCHESTRATOR_PROCESS_INTEGRATION = '1'
go test ./internal/reconciler -run '^TestRealAgentsProcessIntegration$' -count=1 -v
```

This process-level demo uses two independent agent processes on one host, not
separate machines or Docker daemon endpoints. The tests clean only their
randomly named PostgreSQL container and Docker containers whose names correspond
to the test's unique workload IDs.

The direct Docker runtime lifecycle test uses a unique test-created container
name and bounded cleanup:

```powershell
go test ./internal/runtime -run '^TestDockerRuntime$' -count=1 -v
```

It exercises actual Docker create/start/inspect/stats/remove for one test
container. The process-level test above exercises control-plane scheduling
through two real node-agent processes and Docker containers.

Autoscaling acceptance uses deterministic fake CPU readings to test policy.
The Docker test only asserts that a real stats call returns a non-negative
value; it does not verify autoscaler decisions against real Docker CPU
statistics. The process-level test verifies the node transition to `UNHEALTHY`
after agent heartbeats stop and checks that workloads are rescheduled onto the
remaining healthy agent.

## Acceptance results recorded for this worktree

The verification run on Windows with Go 1.27.1 and Docker Desktop reported:

- `go test ./...`: passed.
- `go vet ./...`: passed.
- Opt-in reconciler integration suite: passed using PostgreSQL plus simulated
  node runtimes.
- Opt-in real-process acceptance test: passed using an actual control-plane
  process, two actual node-agent processes, isolated PostgreSQL, mTLS,
  heartbeats, and real Docker containers on the local shared Docker daemon.
- The real-process test exercised replacement after container removal,
  heartbeat loss, transition to `UNHEALTHY`, workload recovery to the surviving
  agent, service deletion, and resource accounting.
- The real-process run exposed and fixed pending workloads not being scheduled
  by the background reconciler and Docker container state being read from the
  human-readable status rather than the machine state.
- Missing-container stop/removal is idempotent only for the recognized
  not-found condition. Other Docker failures remain errors.
- Direct Docker runtime lifecycle test: passed with a unique test container.
- Compose validation: rejected missing `POSTGRES_PASSWORD`; resolved the
  PostgreSQL host binding to `127.0.0.1` with test-only interpolation values.
- `go test -race ./...`: blocked by the installed MinGW.org GCC toolchain
  (`cc1.exe: sorry, unimplemented: 64-bit mode not compiled in`).
- Use a supported 64-bit cgo toolchain for race detection.

Treat those as recorded results for the specific environment/run, not a
guarantee that another machine has the same tools or Docker state.

## Security assumptions and limitations

- mTLS and bearer authentication are implemented. Nodes share a node bearer
  token, while operator APIs use a separate control-plane bearer token. This
  is not per-node bearer-token authorization, secret rotation, certificate
  revocation, or production PKI.
- Node-agent container APIs require the control-plane URI certificate identity
  and a target-node ID matching the receiving agent. The control plane verifies
  each target agent's URI certificate identity; Docker operations are limited
  to containers labeled with that local node ID and a workload ID. The shared
  CA must be protected and issuance must preserve these identity distinctions.
- Existing Docker containers without the node-ownership label are intentionally
  not manageable through the hardened agent API. They require a controlled
  upgrade/replacement plan; the system does not automatically infer ownership.
- Certificate issuance, SAN lifecycle, CA distribution, and rotation must be
  designed and operated externally. Local certificate commands above are for
  development only.
- Operator APIs, including node drain/reactivation, `/metrics`, and
  service/deployment APIs, use `ORCHESTRATOR_OPERATOR_TOKEN`. The node token is
  limited to node authentication and node-agent routes. Operator tokens still
  have no roles or per-service permissions, so restrict network access and
  protect the operator credential.
- Compose publishes PostgreSQL only on loopback, but its named volume persists
  across restarts. Backups, retention, and access controls are operator
  responsibilities.
- The same-host two-agent test shares one Docker daemon. It does not verify
  multi-host routing, isolated Docker Engines, or host/network failure modes.
- Docker socket access is highly privileged. The host's Docker daemon settings
  and socket permissions are outside repository verification.
- Node certificate identity and lease checks do not fence a stale agent's
  already-running Docker workload. There is no atomic fencing between
  PostgreSQL and Docker, exactly-once execution, certificate revocation, or
  workload execution fencing.
- The race suite requires a supported 64-bit cgo compiler. Use Linux x86-64
  with a compatible GCC/Go setup when the Windows MinGW compiler cannot build
  the race runtime.
- Release acceptance here is not production certification.

## Repository hygiene

`.env` is ignored and must remain local. Do not commit local certificates,
private keys, generated binaries, database exports, or test credentials.
Dependencies are declared in `go.mod`/`go.sum`; no dependency was removed as
part of release preparation.
