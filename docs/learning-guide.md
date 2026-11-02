# GoFlow learning guide

A short, plain-language tour of the project, one part at a time. Each part
has: **what** it is, **why** it exists, the **Go ideas** it uses, a one-line
**interview answer**, and questions an interviewer might ask.

Read it in order. Each part builds on the one before.

---

## Part 1 · The project in one minute

**What:** a system that runs background jobs. Someone says "send this email"
through an API; a worker program does it a moment later. If it fails, it is
retried automatically.

**Why it matters:** almost every company has slow or unreliable work (emails,
payments, reports) that should not block a web request.

**Interview answer:**
> "GoFlow is a distributed job queue in Go. Jobs come in through a REST API,
> are stored in PostgreSQL, and are processed by a pool of workers that can
> run on many machines. It handles priorities, scheduling, retries with
> exponential backoff, crash recovery, rate limiting through Redis, and
> graceful shutdown."

---

## Part 2 · Config (`internal/config`)

**What:** reads settings from environment variables, with defaults.
**Why:** the same program runs on your laptop and in production; only the
environment changes. Bad settings stop the program at startup.
**Go ideas:** structs, multiple return values `(Config, error)`, `errors.Join`,
passing a function (`lookupFunc`) so tests can fake the environment,
table-driven tests.

> "Configuration comes from environment variables, is validated at startup,
> and reports every problem at once. The environment lookup is injected as a
> function, so tests never touch real system state."

**Q: Why fail at startup instead of using a default for a bad value?**
A wrong value that silently becomes a default is a bug you discover in
production. Failing immediately with a clear message is safer.

---

## Part 3 · The job model (`internal/job`)

**What:** what a job looks like (`Job` struct), its statuses, which status
changes are allowed, and validation of new jobs.
**Why:** every other part depends on these rules, so they live in one pure
package with no database, which makes them easy to test.
**Go ideas:** named types (`type Status string`), constants, maps as a state
machine, custom error types (`ValidationError`), sentinel errors
(`ErrNotFound`), `errors.Is` / `errors.As`, error wrapping with `%w`.

> "The domain package is pure Go: types, a state machine of allowed
> transitions, and typed errors that the API maps to HTTP status codes."

**Q: What is the difference between `errors.Is` and `errors.As`?**
`Is` asks "is this error (or anything it wraps) this specific value?", such
as `ErrNotFound`. `As` asks "is there an error of this type in the chain?" and
gives it to you, such as `*ValidationError`, so you can read its `Field`.

---

## Part 4 · Backoff (`internal/backoff`)

**What:** how long to wait before a retry: 1s, 2s, 4s, 8s … up to 1 hour.
**Why:** retrying immediately hammers a service that is already struggling.
**Jitter** adds randomness so a thousand failed jobs do not all retry at the
same second.
**Go ideas:** `time.Duration`, guarding against overflow, injecting
randomness for testable code.

> "Retries use exponential backoff with jitter, which avoids the thundering
> herd problem when many jobs fail at once."

---

## Part 5 · PostgreSQL store (`internal/store/postgres`) ★ most important

**What:** all database code. The `jobs` table is both the queue and the history.

**The key idea: `SELECT … FOR UPDATE SKIP LOCKED`.**
Many workers ask for jobs at the same moment. `FOR UPDATE` locks the rows a
worker picks; `SKIP LOCKED` makes other workers skip those rows instead of
waiting. Every worker gets different jobs, and nobody blocks anybody.

**Leases:** a claimed job gets `locked_until = now + 30s`. The worker keeps
extending it (heartbeat). If the worker dies, the lease expires and the
**reaper** puts the job back in the queue.

**Go ideas:** `context.Context` on every call, `pgxpool` (a connection pool
shared by goroutines), `go:embed` for migration files, integration tests with
a build tag.

> "Workers claim jobs with `FOR UPDATE SKIP LOCKED` in a single statement, so
> no job is ever claimed twice and workers don't block each other. Leases with
> heartbeats handle crashed workers. I proved the no-double-claim property with
> a test where 20 goroutines race for 2,000 jobs, and I checked that the test
> fails if the claim query is written the naive way."

**Q (the challenge question from day 1): What goes wrong with plain `FOR UPDATE`, and with no locking?**
- Plain `FOR UPDATE`: correct, but slow. Workers queue up waiting for the
  same locked rows, so 20 workers behave almost like 1.
- No locking, as "SELECT ids, then UPDATE" in two queries: two workers read
  the same ids before either updates, and the **same job runs twice**.

**Q: Why use the database clock (`now()`) instead of Go's `time.Now()`?**
Workers on different machines have slightly different clocks. One shared
clock avoids jobs running early or leases expiring at the wrong time.

**Q: What delivery guarantee do you give?**
At-least-once. If a worker finishes a job but crashes before saving
"succeeded", the job runs again. So handlers must be idempotent.

---

## Part 6 · Worker pool (`internal/worker`) ★ Go concurrency

**What:** one worker process runs several goroutines:
- **fetcher**: claims jobs when there is free capacity
- **executors** (N goroutines): run jobs received from a channel
- **heartbeat**: extends leases
- **reaper**: rescues jobs from crashed workers

**Go ideas:**
- goroutines and **channels**: the fetcher sends jobs on an unbuffered
  channel, and executors receive them
- `select` to wait for several things at once (wake-up, timer, free slot, shutdown)
- `sync.WaitGroup` to wait for goroutines to finish
- `sync/atomic` for a counter shared by goroutines
- `context.WithCancelCause` to cancel one job and record **why**
- `recover()` so a panicking handler does not crash the process
- interfaces (`Store`), so tests use an in-memory fake

> "The worker is a fetcher goroutine feeding N executor goroutines through a
> channel, with heartbeat and reaper goroutines alongside. Each job runs with
> its own context, which can be cancelled for a timeout, a lost lease, or
> shutdown, and the worker treats each cause differently."

**Q: How does graceful shutdown work?**
On SIGTERM, stop claiming new jobs, let running jobs finish for up to 30
seconds, then cancel any that are left and put them back in the queue without
counting the attempt. Results are saved with a fresh context, because the
main one is already cancelled.

**Q: Why is the jobs channel unbuffered?**
The fetcher only claims as many jobs as there are free executors. A buffer
would let claimed jobs sit waiting while their lease runs down.

---

## Part 7 · REST API (`internal/api`)

**What:** HTTP endpoints to create, get, list, cancel, and retry jobs.
**Go ideas:** `net/http` with Go 1.22 route patterns (`GET /v1/jobs/{id}`),
middleware as functions that wrap handlers, `httptest` for testing, strict
JSON decoding, `http.MaxBytesReader`.
**Design points:** one error format; idempotency keys; cursor pagination;
internal errors never leak to clients; `/healthz` vs `/readyz`.

> "The API uses only the standard library. Middleware adds request IDs,
> logging, metrics, and panic recovery. Idempotency keys are enforced by a
> unique index, so duplicate requests are safe even when they arrive at the
> same moment."

**Q: Why cursor pagination instead of `OFFSET`?**
`OFFSET 100000` makes the database walk past 100,000 rows. `WHERE id < cursor`
uses the index directly, and new rows do not shift the pages.

**Q: What is the difference between liveness and readiness?**
Liveness means the process is alive (otherwise, restart it). Readiness means it
can serve traffic right now (otherwise, stop sending requests). Checking the
database in liveness would restart every healthy API instance during a
database outage.

---

## Part 8 · Redis (`internal/coord`)

**What:** three helpers. **Pub/sub** wakes workers instantly. A **rate
limiter** caps starts per second per job type. **Heartbeat keys** list live
workers.
**Why it's optional:** PostgreSQL holds the truth. If Redis dies, workers
poll, rate limits fail open, and the worker list is empty, but nothing breaks.
**Go/Redis ideas:** a Lua script makes "increment and check" atomic across all
workers; keys that expire on their own (TTL).

> "Redis is an optimization, not a dependency for correctness. I tested this
> by stopping Redis mid-run: jobs kept completing by polling, and wake-ups
> resumed by themselves when Redis came back."

---

## Part 9 · Metrics and logs (`internal/metrics`)

**What:** Prometheus counters, gauges, and histograms, plus JSON logs.
**Why own code:** keeps dependencies small and shows how metrics work.
**Key detail:** label routes by **pattern** (`/v1/jobs/{id}`), not by real
path (`/v1/jobs/1`, `/v1/jobs/2`, …), otherwise you get unlimited metric series.

---

## Part 10 · Testing and benchmarks

- **Unit tests**: fast, no database, using fakes behind interfaces.
- **Integration tests** (`-tags=integration`): real PostgreSQL and Redis.
- **End-to-end**: HTTP to database to worker and back.
- **Race detector** (`-race`) on everything.
- **Benchmarks**: about 650 to 2,900 jobs/s on a 2-CPU machine.

> "I don't trust a concurrency test until I've seen it fail. I swapped in a
> buggy claim query to confirm my test catches double-claims. And while
> benchmarking, I found my own benchmark was slowing the system down by
> polling too often, and fixed the measurement."

---

## Part 11 · Docker

- **Multi-stage Dockerfile**: build with the full Go image, ship only the
  binaries on a tiny "distroless" image that runs as a non-root user.
- **docker-compose**: PostgreSQL and Redis with health checks; the app
  services are behind a profile.
- **`stop_grace_period`** is longer than the app's shutdown timeout, so
  Docker doesn't kill workers mid-drain.
