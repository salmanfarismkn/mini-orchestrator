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
| `ORCHESTRATOR_NODE_TOKEN` | Yes | Shared bearer token for node authentication and service-management API routes. Supply a unique secret through a secret manager or local environment; never commit it. |
| `ORCHESTRATOR_TLS_CERT_FILE` | Yes | PEM leaf certificate used for HTTPS and as the presented client certificate. |
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
| `POSTGRES_PASSWORD` | Yes for Compose | Compose | PostgreSQL password; Compose fails configuration when unset. |
| `POSTGRES_PORT` | No | Compose | Host port, default `5432`, bound to `127.0.0.1`. |

For multiple local agents, configure a distinct node ID and listen address for
each process. The node listener and the control-plane-advertised address are
separate settings: bind to `:9001` while registering `127.0.0.1:9001`, for
example. The advertised address must be reachable from the control plane and
its hostname/IP must appear in the node certificate SANs. The control-plane
listener is fixed at `:8080`.
The token, TLS paths, HTTPS control-plane URL, and advertised address are
required. Listener and capacity defaults are for one development agent; node
IDs must be unique when multiple agents register.

## Local development certificate example

This creates a **development-only** CA and one leaf certificate. Use a
certificate SAN for every DNS name/IP used by the control plane and agents.
For a same-host, one-agent setup, `localhost` and `127.0.0.1` are sufficient.
For separate hosts, add each real endpoint name/IP to `subjectAltName` before
issuing the certificate. Do not use this shared local leaf/key as a production
PKI or certificate-rotation strategy.

Run in PowerShell in a private directory outside the repository:

```powershell
$certDir = Join-Path $env:USERPROFILE '.mini-orchestrator\local-certs'
New-Item -ItemType Directory -Force $certDir | Out-Null
Set-Location $certDir

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 `
  -keyout ca-key.pem -out ca.pem -days 30 -nodes `
  -subj "/CN=mini-orchestrator-local-test-ca"

openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 `
  -keyout orchestrator-key.pem -out orchestrator.csr -nodes `
  -subj "/CN=localhost"

@'
subjectAltName = DNS:localhost,IP:127.0.0.1
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
'@ | Set-Content -Encoding ascii .\leaf.ext

openssl x509 -req -in orchestrator.csr -CA ca.pem -CAkey ca-key.pem `
  -CAcreateserial -out orchestrator.pem -days 30 -sha256 -extfile leaf.ext
```

Do not print, commit, or share `ca-key.pem` or `orchestrator-key.pem`. The
application loads one cert/key pair for server and client roles. The control
plane accepts client certificates optionally at the listener, but node
registration and heartbeat handlers require a verified certificate. The
node-agent HTTPS server requires a verified client certificate for its routes.
In each later PowerShell terminal, set `$certDir` again to the same path before
using the certificate files.

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

Build and start the control plane in one terminal:

```powershell
$env:ORCHESTRATOR_NODE_TOKEN = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
$certDir = Join-Path $env:USERPROFILE '.mini-orchestrator\local-certs'
$env:ORCHESTRATOR_TLS_CERT_FILE = Join-Path $certDir 'orchestrator.pem'
$env:ORCHESTRATOR_TLS_KEY_FILE = Join-Path $certDir 'orchestrator-key.pem'
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
include quotes/brackets in the actual token. Two agent processes on one host can share the token and certificate while
providing distinct IDs and ports. Open a separate terminal for each process;
set the corresponding values below in each. The agent processes require access
to Docker Engine. On one development host they share that Engine and are not
independent Docker hosts.

First agent:

```powershell
$env:ORCHESTRATOR_NODE_TOKEN = '<same locally generated token>'
$certDir = Join-Path $env:USERPROFILE '.mini-orchestrator\local-certs'
$env:ORCHESTRATOR_TLS_CERT_FILE = Join-Path $certDir 'orchestrator.pem'
$env:ORCHESTRATOR_TLS_KEY_FILE = Join-Path $certDir 'orchestrator-key.pem'
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
$env:ORCHESTRATOR_TLS_CERT_FILE = Join-Path $certDir 'orchestrator.pem'
$env:ORCHESTRATOR_TLS_KEY_FILE = Join-Path $certDir 'orchestrator-key.pem'
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

Service-management routes require `Authorization: Bearer <token>`. The
registration and heartbeat routes additionally require a verified client
certificate. API examples below use `curl.exe`; pass local development
certificates where appropriate and never paste real tokens into shared logs.
Set the private certificate directory for each terminal before running the
commands:

```powershell
$env:ORCHESTRATOR_NODE_TOKEN = '<same locally generated token>'
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
curl.exe --cacert "$certDir\ca.pem" https://localhost:8080/metrics
```

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

- mTLS and bearer authentication are implemented, but the project uses one
  configured cert/key pair and a shared bearer token. This is not a per-node
  identity, secret rotation, certificate revocation, or production PKI system.
- Certificate issuance, SAN lifecycle, CA distribution, and rotation must be
  designed and operated externally. Local certificate commands above are for
  development only.
- Service-management authorization currently uses the same bearer token as
  node authentication; there are no roles or per-service permissions.
- `/metrics` is unauthenticated and exposes aggregate counters. Restrict network
  access at deployment level if that is unsuitable.
- Compose publishes PostgreSQL only on loopback, but its named volume persists
  across restarts. Backups, retention, and access controls are operator
  responsibilities.
- The same-host two-agent test shares one Docker daemon. It does not verify
  multi-host routing, isolated Docker Engines, or host/network failure modes.
- Docker socket access is highly privileged. The host's Docker daemon settings
  and socket permissions are outside repository verification.
- The race suite requires a supported 64-bit cgo compiler. Use Linux x86-64
  with a compatible GCC/Go setup when the Windows MinGW compiler cannot build
  the race runtime.
- Release acceptance here is not production certification.

## Repository hygiene

`.env` is ignored and must remain local. Do not commit local certificates,
private keys, generated binaries, database exports, or test credentials.
Dependencies are declared in `go.mod`/`go.sum`; no dependency was removed as
part of release preparation.
