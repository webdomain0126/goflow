# GoFlow architecture

This document explains how GoFlow works and why it is built this way.

## 1. Components

| Component | Process | Responsibility |
|---|---|---|
| API | `goflow-api` | Validates and stores jobs, answers queries, publishes wake-ups |
| Worker | `goflow-worker` (1..N) | Claims jobs, runs handlers, records results |
| PostgreSQL | | Source of truth: the `jobs` table is both queue and history |
| Redis | | Optional helpers: wake-ups, rate limits, worker registry |

The API and the worker are separate programs built from one codebase, so they
scale independently: one API and ten workers on different machines is a
normal deployment.

## 2. Why PostgreSQL is the queue

The alternative, "Redis as the queue, PostgreSQL as the record", stores each
job in two places. If the API writes to PostgreSQL and then crashes before
writing to Redis, the job is lost; the reverse order risks running a job that
was never recorded. Fixing that needs a transactional outbox: more moving parts.

With PostgreSQL as the queue, every state change is one transaction on one
row. There is nothing to keep in sync.

## 3. Job lifecycle

```
            create
              │
              ▼
  ┌──────▶ PENDING ──(run_at reached, claimed)──▶ RUNNING
  │           │                                     │
  │        cancel                    ┌──────────────┼───────────────┐
  │           ▼                      ▼              ▼               ▼
  │      CANCELLED               SUCCEEDED    error, attempts   error, attempts
  │           │                                left / lease      exhausted, or
  │           │ manual retry                   expired           job.Permanent
  └───────────┴── run_at = now + backoff ◀──────────┘                │
                                                                     ▼
                     manual retry ◀──────────────────────────────  DEAD
```

The allowed transitions live in `internal/job/job.go` (`CanTransition`), and
every UPDATE in the store includes the expected current status in its
`WHERE` clause, so an illegal transition simply changes zero rows.

**Scheduling is just a column.** `run_at` decides when a job may run. A job
scheduled for tomorrow and a retry delayed by backoff use the same mechanism,
so there is no separate scheduler process.

## 4. Claiming work: `FOR UPDATE SKIP LOCKED`

```sql
WITH next AS (
    SELECT id FROM jobs
    WHERE status = 'pending' AND run_at <= now()
      AND ($4::text[] IS NULL OR type = ANY($4))      -- only types this worker handles
    ORDER BY priority DESC, run_at, id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs SET status = 'running', attempts = attempts + 1,
       locked_by = $2, locked_until = now() + $3::interval
FROM next WHERE jobs.id = next.id
RETURNING ...
```

- `FOR UPDATE` locks the selected rows until the transaction ends.
- `SKIP LOCKED` makes other workers **skip** those rows instead of waiting,
  so 20 workers claim 20 different batches in parallel.
- Selecting and updating in **one statement** matters. The naive version,
  "SELECT some ids, then UPDATE them" in two queries, lets two workers read the
  same ids before either updates. `TestConcurrentClaimNeverDoublesUp` caught
  exactly that when the naive version was swapped in.
- A partial index `(priority DESC, run_at, id) WHERE status = 'pending'`
  keeps the claim fast even with millions of finished jobs in the table.

## 5. Leases, heartbeats, and the reaper (crash recovery)

A claimed job gets a **lease**: `locked_by = worker id`, `locked_until = now() + 30s`.

- **Heartbeat**: every `lease / 3`, the worker extends the lease of all its
  running jobs in one query. If a job's lease could not be extended (someone
  else owns it now), the worker cancels that job's `context`.
- **Reaper**: every worker periodically runs one query that finds `running`
  jobs whose lease has expired, which means their worker crashed or lost its
  network. Jobs with attempts left go back to `pending`; the others become `dead`.
  `SKIP LOCKED` makes it safe for every worker to run the reaper.
- **Fencing**: completing, retrying, or failing a job uses
  `WHERE id = $1 AND locked_by = $me AND status = 'running'`. A slow worker
  whose lease was taken away cannot overwrite the new owner's result; it
  gets `ErrLeaseLost`.

### Delivery guarantee: at-least-once

If a worker finishes the work and then crashes before it records "succeeded",
the lease expires and the job runs again. **Handlers must be idempotent**:
for example, they can check "was this email already sent for job 42?" before
sending. No distributed queue can guarantee exactly-once execution of side
effects in general; GoFlow documents its guarantee instead of hiding it.

## 6. Retries and backoff

`internal/backoff`: delay = `Base × 2^(attempt-1)`, capped at `Max` (default 1s, 2s, 4s, … up to 1h).

**Jitter** ("equal jitter": half fixed, half random) spreads retries out. Without
it, 1,000 jobs that failed together because a dependency was briefly down would
all retry at the same instant and could cause the same outage again.

A handler returns `job.Permanent(err)` for errors a retry cannot fix (invalid
email address, malformed payload). Those jobs go straight to `dead` without
wasting attempts. `job.IsPermanent` uses `errors.As`, so it still works after
the error has been wrapped with `fmt.Errorf("...: %w", err)`.

A handler panic is recovered, logged with its stack trace, and treated as an
ordinary (retryable) failure. One buggy handler cannot crash the worker.

## 7. Inside a worker process

```
              ┌──────────────── fetchLoop ────────────────┐
  wake-up ───▶│ free = concurrency - busy                  │
  poll tick ─▶│ claim(limit = free) ── jobs chan ──┐       │
  slot freed ▶│                                     │      │
              └─────────────────────────────────────┼──────┘
                                                    ▼
                        executor goroutines (N, reading the channel)
                        run handler with ctx: job timeout | lease lost | shutdown
                        then Complete / ScheduleRetry / MarkDead / Release

              heartbeatLoop: extend leases of running jobs, cancel lost ones
              reapLoop:      re-queue jobs with expired leases
```

Key Go techniques:

- **Unbuffered channel** as a hand-off: the fetcher only claims as many jobs as
  there are free executors, so jobs never sit claimed in a buffer (where their
  lease would tick away).
- **`sync/atomic` counter** for the number of busy executors.
- **`context.WithCancelCause`**: a job's context records *why* it was
  cancelled (shutdown or lease lost), and the worker reacts differently to each.
- **`sync.WaitGroup`** to wait for every goroutine during shutdown.

## 8. Graceful shutdown

1. SIGTERM (Docker, Kubernetes) or Ctrl+C cancels the root context (`signal.NotifyContext`).
2. The fetcher stops claiming and closes the jobs channel.
3. Running jobs keep going. Their contexts are deliberately **not** children
   of the root context, so they are not cancelled instantly.
4. The heartbeat keeps renewing leases while jobs drain.
5. After `GOFLOW_SHUTDOWN_TIMEOUT`, remaining jobs are cancelled with cause
   "shutdown" and **released**: they go back to `pending` with the attempt not
   counted, so another worker can take them immediately.
6. Results are written with a fresh context, because the root context is
   already cancelled.

In Docker, `stop_grace_period` must be longer than the shutdown timeout, or
Docker kills the process before it finishes draining.

## 9. Redis: helpful, never required

| Feature | How | If Redis is down |
|---|---|---|
| Wake-ups | API `PUBLISH`es on job creation; workers `SUBSCRIBE` and claim immediately | Workers find jobs on their next poll (default 1s) |
| Rate limiting | Lua script: `INCR` a per-type key, start a 1s expiry on first use; atomic across all workers | Fails open: jobs run unthrottled, warning logged |
| Worker registry | Each worker `SET`s `goflow:workers:<id>` with a 15s expiry every 5s | `/v1/workers` returns 503; nothing else is affected |

go-redis reconnects by itself, so these features recover without a restart
when Redis comes back. All three were verified by stopping and restarting a
real Redis server during a run.

A rate-limited job is **released** (back to `pending` with a short delay,
attempt not counted) rather than holding a worker slot while it waits.

## 10. Observability

- **Logs**: JSON via `log/slog`, one line per request (method, route, status,
  duration, request id) and per significant job event.
- **Request IDs**: an incoming `X-Request-ID` is reused if it is safe, otherwise
  one is generated, and it is echoed in the response.
- **Metrics** (Prometheus text format):
  - API: `goflow_http_requests_total{method,route,code}`, `goflow_http_request_duration_seconds{route}`, `goflow_jobs{status}`
  - Worker (`:9091/metrics`): `goflow_jobs_processed_total{type,outcome}`, `goflow_job_duration_seconds{type}`, `goflow_worker_active_jobs`
  - Routes are labelled by pattern (`GET /v1/jobs/{id}`), not by raw path, so
    label cardinality stays bounded.
- **Health**: `/healthz` (process alive, never checks dependencies) and
  `/readyz` (database reachable). Keeping them separate prevents a database
  outage from making an orchestrator restart every healthy API instance.

## 11. API design notes

- Strict JSON decoding: unknown fields, trailing data, and bodies over 1 MiB are rejected.
- Errors share one shape: `{"error": {"code", "message", "field"}}`. Internal
  errors return a generic message; details only go to the log.
- **Idempotency keys** (header or body): enforced by a `UNIQUE` column and
  `INSERT … ON CONFLICT DO NOTHING`, so it is atomic even for concurrent requests.
  A replay returns `200` with `Idempotent-Replayed: true`.
- **Cursor pagination** (`?before=<id>`) instead of `OFFSET`: page 1,000 is as
  fast as page 1, and new jobs do not shift pages.
- Job IDs are unique and increasing but **may have gaps**: PostgreSQL consumes
  an identity value even when `ON CONFLICT DO NOTHING` skips the insert.

## Limitations and next steps

- **Workflows (planned)**: job dependencies (B runs after A succeeds).
- **Table growth**: finished jobs stay forever. Production would add a retention
  job (delete or archive after N days) or partition the table by time.
- **Fixed-window rate limiting** can allow up to 2× the limit across a window
  boundary. A sliding-window or token-bucket script would smooth that out.
- **Released jobs cause extra queries** under heavy rate limiting (each
  denied job is released and claimed again).
- **One commit per finished job** is the main throughput limit (see
  [benchmarks.md](benchmarks.md)); batching results would raise it.
- **No authentication**: the API is meant to run behind a gateway or on a
  private network. Adding an API-key middleware would be the next step.
- **JSONB normalizes payloads**: key order and whitespace are not preserved.
