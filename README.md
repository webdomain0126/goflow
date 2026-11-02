# GoFlow

**A distributed job processing engine in Go, backed by PostgreSQL and Redis.**

Submit jobs over a REST API; a pool of workers (on one machine or many) claims
and runs them concurrently, with priorities, scheduling, automatic retries with
exponential backoff, crash recovery, rate limiting, and graceful shutdown.

![CI](https://github.com/webdomain0126/goflow/actions/workflows/ci.yml/badge.svg)

```
POST /v1/jobs {"type":"send_email","payload":{...}}   →   201 Created, status "pending"
        ... a worker claims it a few milliseconds later ...
GET  /v1/jobs/1                                       →   status "succeeded", attempts 1
```

## Features

- **REST API**: create, get, list (cursor pagination, filters), cancel, retry, stats, live workers
- **Concurrent worker pool**: goroutines and channels, configurable concurrency
- **Safe distributed claiming**: `SELECT … FOR UPDATE SKIP LOCKED`, so any number of workers share one queue with no job claimed twice (proven by a concurrency test)
- **Priorities and scheduling**: `priority` 0–100 and `run_at` for delayed jobs
- **Retries**: exponential backoff with jitter; `job.Permanent(err)` skips retries for errors that retrying cannot fix
- **Crash recovery**: leases plus heartbeats; a reaper re-queues jobs from dead workers
- **Idempotency keys**: duplicate requests return the original job instead of creating a second one
- **Redis coordination**: instant wake-ups (pub/sub), per-type rate limiting (atomic Lua script), worker registry (heartbeat keys with expiry)
- **Graceful degradation**: if Redis goes down, everything still works, just slower (tested by stopping Redis)
- **Graceful shutdown**: on SIGTERM or Ctrl+C, running jobs finish; jobs still running after the timeout are handed back to the queue
- **Observability**: structured JSON logs (`log/slog`), Prometheus metrics, `/healthz` and `/readyz`
- **Small dependency footprint**: standard library HTTP routing, plus only `pgx` and `go-redis`

## Architecture

```
                 ┌──────────────┐
  HTTP clients ─▶│  goflow-api  │── INSERT job ─────────────┐
                 │  (REST)      │── PUBLISH wake-up ──┐     │
                 └──────────────┘                     ▼     ▼
                                              ┌───────┐ ┌────────────┐
                                              │ Redis │ │ PostgreSQL │
                                              │pub/sub│ │ jobs table │
                                              │limits │ │ = queue +  │
                                              │workers│ │   history  │
                                              └───┬───┘ └─────▲──────┘
                                       wake-ups   │           │ claim (SKIP LOCKED)
                                                  ▼           │ heartbeat / complete
                 ┌────────────────────────────────────────────┴──┐
                 │ goflow-worker  (run 1..N processes)           │
                 │   fetcher ──chan Job──▶ executor goroutines   │
                 │   heartbeat (renews leases)   reaper          │
                 └───────────────────────────────────────────────┘
```

PostgreSQL is the single source of truth: a job's state changes in one
transaction, so there is no "written to the database but not to the queue"
problem. Redis only makes things faster and smoother, and is never required
for correctness.

Details: [docs/architecture.md](docs/architecture.md)

## Quick start

You need Go 1.24+, Docker, and (optionally) `make`.

**1. Start PostgreSQL and Redis**

```bash
docker compose up -d --wait
```

**2. Run the API and a worker** (two terminals)

```bash
go run ./cmd/goflow-api
go run ./cmd/goflow-worker
```

**3. Send a job**

PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/jobs -ContentType "application/json" `
  -Body '{"type":"send_email","payload":{"to":"you@example.com","subject":"Hi"}}'
```

bash / curl:

```bash
curl -X POST localhost:8080/v1/jobs -d '{"type":"send_email","payload":{"to":"you@example.com","subject":"Hi"}}'
curl localhost:8080/v1/jobs/1
```

Or run the full demo: `./examples/demo.ps1` (Windows) or open
[examples/requests.http](examples/requests.http) in VS Code with the REST Client extension.

**Everything in Docker** (API + worker as containers too):

```bash
docker compose --profile app up -d --build --wait
docker compose --profile app up -d --scale worker=3   # three workers sharing the queue
```

## Built-in example job types

| Type | Payload | What it does |
|---|---|---|
| `noop` | anything | Nothing (used for benchmarks) |
| `echo` | anything | Logs the payload |
| `sleep` | `{"ms": 500}` | Waits, stopping early if cancelled |
| `flaky` | `{"fail_attempts": 2}` | Fails the first N attempts, then succeeds (shows retries) |
| `send_email` | `{"to": "...", "subject": "..."}` | Simulated email; ~10% temporary failures; invalid address = permanent failure |

Adding your own handler takes one line: `w.Handle("resize_image", myFunc)`.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/jobs` | Create a job (`Idempotency-Key` header supported) |
| `GET` | `/v1/jobs/{id}` | Get one job |
| `GET` | `/v1/jobs?status=&type=&limit=&before=` | List jobs, newest first, cursor pagination |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel a pending job |
| `POST` | `/v1/jobs/{id}/retry` | Re-queue a dead or cancelled job |
| `GET` | `/v1/stats` | Job counts by status |
| `GET` | `/v1/workers` | Live workers (from Redis heartbeats) |
| `GET` | `/healthz`, `/readyz` | Liveness and readiness probes |
| `GET` | `/metrics` | Prometheus metrics (worker: port 9091) |

Full reference with real request and response examples: [docs/api.md](docs/api.md) ·
OpenAPI spec: [docs/openapi.yaml](docs/openapi.yaml)

## Configuration

All settings are environment variables with working defaults for local development.

| Variable | Default | Meaning |
|---|---|---|
| `GOFLOW_HTTP_ADDR` | `:8080` | API listen address |
| `GOFLOW_METRICS_ADDR` | `:9091` | Worker metrics and health address |
| `GOFLOW_DATABASE_URL` | `postgres://goflow:goflow@localhost:5432/goflow?sslmode=disable` | PostgreSQL |
| `GOFLOW_REDIS_URL` | `redis://localhost:6379/0` | Redis, or `disabled` |
| `GOFLOW_WORKER_CONCURRENCY` | `10` | Jobs one worker runs at once (1–1000) |
| `GOFLOW_POLL_INTERVAL` | `1s` | How often an idle worker checks for jobs |
| `GOFLOW_JOB_TIMEOUT` | `5m` | Maximum run time per job |
| `GOFLOW_SHUTDOWN_TIMEOUT` | `30s` | Grace period for running jobs on shutdown |
| `GOFLOW_RATE_LIMITS` | (none) | Per-type starts per second, e.g. `send_email=50,sms=10` |
| `GOFLOW_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

Invalid values stop the program at startup with a message listing every problem.

## Testing

```bash
make test               # unit tests, no database needed
make up                 # start PostgreSQL + Redis
make test-integration   # unit + integration + end-to-end tests
make test-all           # the same, with the race detector (Linux/macOS)
make lint               # staticcheck
```

On Windows, `-race` needs a C compiler (cgo), so use `make test-integration`
there. CI runs the full suite with the race detector on Linux for every push.

The suite has 120+ tests. Highlights:

- **No double-claiming**: 20 goroutines race to claim 2,000 jobs; every job must be claimed exactly once. The test was checked by swapping in a naive "SELECT, then UPDATE" claim, which it caught.
- **Crash recovery**: a "crashed" worker's jobs are reclaimed by a surviving worker.
- **Three workers, flaky handlers**: 300 jobs with a 30% failure rate all reach a final state, and no job ever runs on two workers at once.
- **Graceful shutdown**: running jobs finish; after the timeout they are released without losing an attempt.
- **Redis down**: rate limits fail open and errors return fast.
- **End-to-end**: HTTP → PostgreSQL → Redis wake-up → worker → result visible over HTTP.

Integration tests use the `integration` build tag and run with `-p 1`, because
packages share one database.

## Performance

Measured on a 2-vCPU cloud VM, with PostgreSQL 16 on the same machine and a
handler that does nothing (so this is GoFlow's own overhead):

| Setup | Throughput |
|---|---|
| 1 worker, concurrency 1 | ~650 jobs/s |
| 1 worker, concurrency 10 | ~2,300 jobs/s |
| 1 worker, concurrency 50 | ~2,900 jobs/s |
| 3 workers × concurrency 20 | ~2,700 jobs/s |
| Job created → finished (Redis wake-up) | ~16 ms |

Method, all results, and what limits throughput: [docs/benchmarks.md](docs/benchmarks.md).
Run them yourself with `make bench` and `make bench-integration`.

## Project layout

```
cmd/goflow-api/         API program (main.go only wires things together)
cmd/goflow-worker/      worker program
internal/job/           domain types, state machine, validation (pure Go)
internal/backoff/       exponential backoff with jitter (pure Go)
internal/store/postgres store: migrations, SKIP LOCKED claiming, leases, reaper
internal/worker/        worker pool: fetcher, executors, heartbeat, reaper, shutdown
internal/api/           REST handlers and middleware
internal/coord/         Redis: wake-ups, rate limiter, worker registry
internal/metrics/       small Prometheus-format metrics library
internal/handlers/      example job handlers
internal/config/        environment configuration
internal/app/           shared startup code
internal/e2e/           end-to-end tests
docs/                   architecture, API, OpenAPI, benchmarks, learning guide
```

## Design decisions and trade-offs

- **PostgreSQL as the queue** instead of Redis lists: one source of truth and
  transactional state changes, at the cost of lower peak throughput than a
  dedicated broker. Thousands of jobs per second is enough for most systems.
- **At-least-once delivery**: if a worker finishes a job but crashes before
  recording it, the job runs again. Handlers must be idempotent. Exactly-once
  delivery is not achievable in general, so the system documents this
  guarantee instead of hiding it.
- **Database clock for all timing** (`now()`), so workers whose clocks
  disagree cannot cause early or late runs.
- **Own metrics package** instead of `prometheus/client_golang`, to keep
  dependencies minimal and show how the format works. In a company codebase,
  the official client would be the usual choice.

Known limitations and next steps are listed in [docs/architecture.md](docs/architecture.md#limitations-and-next-steps).
Planned stretch goal: **workflow dependencies** (job B runs only after job A succeeds).

## License

MIT. See [LICENSE](LICENSE).
