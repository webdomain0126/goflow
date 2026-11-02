# Benchmarks

All numbers come from real runs. They depend heavily on hardware, so treat
them as relative, not absolute, and re-run them on your own machine.

## Environment

| | |
|---|---|
| Machine | Cloud VM, 2 vCPU (Intel Xeon @ 2.80 GHz) |
| Go | 1.24.7, linux/amd64 |
| PostgreSQL | 16.15, same machine, default settings (`synchronous_commit = on`) |
| Redis | 7.0.15, same machine |

## End-to-end throughput

`BenchmarkThroughput` (internal/worker): insert 3,000 jobs, start the workers,
and measure until all 3,000 have succeeded. The handler does nothing, so the
result is GoFlow's own per-job overhead: claim, lease, record the result.
Three runs each:

| Workers × concurrency | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| 1 × 1 | 616 jobs/s | 652 jobs/s | 673 jobs/s |
| 1 × 10 | 2,505 jobs/s | 2,287 jobs/s | 2,184 jobs/s |
| 1 × 50 | 3,022 jobs/s | 2,897 jobs/s | 2,781 jobs/s |
| 3 × 20 | 2,669 jobs/s | 2,689 jobs/s | 2,857 jobs/s |

**Reading the results**

- Concurrency 1 → 10 gives about 3.5×: while one job's result is being
  committed, other executors keep working.
- Gains flatten after that, and 3 workers are no faster than 1. On this
  machine the limit is PostgreSQL on 2 shared CPUs, not the Go code. More
  workers just compete for the same database.
- Real handlers (sending email, calling APIs) take milliseconds to seconds,
  so in practice the handler, not GoFlow, sets the throughput, and concurrency
  is what lets one worker overlap many slow jobs.

## Store operations

`go test -tags=integration -bench . -benchtime=3000x ./internal/store/postgres`

| Benchmark | Result |
|---|---|
| `Create` (one INSERT, sequential) | 0.55 ms/op |
| `ClaimComplete` batch = 1 | 740 jobs/s |
| `ClaimComplete` batch = 10 | 1,584 jobs/s |
| `ClaimComplete` batch = 50 | 1,799 jobs/s |

Bigger claim batches help, but returns shrink because every job still needs
its own `Complete` UPDATE and commit. **Batching completions** (one UPDATE for
many finished jobs) is the most promising next optimization.

## Latency

End-to-end test: job created over HTTP → status `succeeded`, with polling
turned off so only the Redis wake-up can trigger the worker: **16 ms**.
Without Redis, the expected latency is up to one poll interval (default 1s).

## Micro-benchmarks

`go test -run=NONE -bench=. -benchmem ./...`

| Benchmark | Time | Allocations |
|---|---|---|
| `backoff.Delay` | 21 ns/op | 0 |
| `job.CreateParams.Normalize` | 702 ns/op | 1 alloc (24 B) |
| `metrics` CounterVec.Inc, 2 labels, parallel | 209 ns/op | 2 allocs (56 B) |
| `metrics` HistogramVec.Observe, 1 label, parallel | 60 ns/op | 0 |
| `api` create-job handler (routing, middleware, JSON, validation; in-memory store) | 26 µs/op | 63 allocs |

The HTTP layer costs about 26 µs per request, small next to the ~0.5 ms
database INSERT behind it.

## Lessons learned while measuring

1. **The benchmark can be the bottleneck.** The first version of
   `BenchmarkThroughput` checked progress with a `COUNT(*)` query every 5 ms.
   On 2 CPUs that competed with the workers and cut the single-worker result
   from about 650 to about 250 jobs/s. Checking every 50 ms fixed it. The first
   numbers measured the test, not GoFlow.
2. **Allocations explain surprising numbers.** The 2-label counter is about
   3× slower than the 1-label histogram because joining two label values into
   a map key allocates a new string (`-benchmem` shows 2 allocs vs 0).
   Pre-resolved label handles, which `prometheus/client_golang` offers, would
   remove that cost. It is not worth it here, at 200 ns per finished job.

## Reproduce

```bash
make up                  # PostgreSQL + Redis
make bench               # micro-benchmarks
make bench-integration   # store + end-to-end throughput
```
