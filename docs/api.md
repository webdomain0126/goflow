# GoFlow REST API

Base URL (local): `http://localhost:8080`. All bodies are JSON. Every response
includes an `X-Request-ID` header (your own is reused if you send a safe one).

All examples below are real responses captured from a running GoFlow
(timestamps are in the database server's time zone).

Machine-readable spec: [openapi.yaml](openapi.yaml)

## The Job object

| Field | Type | Notes |
|---|---|---|
| `id` | integer | Unique and increasing; may have gaps |
| `type` | string | Selects the handler. `^[a-z][a-z0-9_.-]{0,63}$` |
| `payload` | JSON | Any JSON value, up to 256 KiB. Default `{}` |
| `priority` | integer | 0–100, higher runs first. Default 0 |
| `status` | string | `pending`, `running`, `succeeded`, `cancelled`, `dead` |
| `attempts` | integer | Runs started so far |
| `max_attempts` | integer | 1–25. Default 3 |
| `run_at` | timestamp | Not run before this time |
| `locked_by`, `locked_until` | string, timestamp | Lease of the worker running it; `null` otherwise |
| `last_error` | string | Error from the most recent failed run |
| `idempotency_key` | string | Present only if one was given |
| `created_at`, `updated_at`, `finished_at` | timestamp | `finished_at` is set for final states |

## Errors

Every error uses the same shape:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "must be 1-64 characters: lowercase letters, digits, '_', '.', '-', starting with a letter",
    "field": "type"
  }
}
```

| HTTP | `code` | When |
|---|---|---|
| 400 | `validation_failed` | A field has a bad value (`field` says which) |
| 400 | `bad_request` | Malformed JSON, unknown field, bad id |
| 404 | `not_found` | No job with that id |
| 409 | `invalid_state` | The job's status does not allow the action |
| 413 | `body_too_large` | Body over 1 MiB |
| 500 | `internal` | Unexpected error (details are only in the server log) |
| 503 | `redis_disabled`, `redis_unavailable` | `/v1/workers` without Redis |

---

## Create a job

`POST /v1/jobs`

| Body field | Required | Default |
|---|---|---|
| `type` | yes | |
| `payload` | no | `{}` |
| `priority` | no | `0` |
| `max_attempts` | no | `3` |
| `run_at` | no | now (a past time also means now) |
| `idempotency_key` | no | (or use the `Idempotency-Key` header) |

Returns `201 Created` with a `Location` header, or `200 OK` with
`Idempotent-Replayed: true` if a job with the same idempotency key exists.

```
$ curl -i -X POST localhost:8080/v1/jobs \
    -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: welcome-tisa' \
    -d '{"type":"send_email","payload":{"to":"tisa@example.com","subject":"Welcome"},"priority":5}'

HTTP/1.1 201 Created
Content-Type: application/json
Location: /v1/jobs/1

{
  "id": 1,
  "type": "send_email",
  "payload": {
    "to": "tisa@example.com",
    "subject": "Welcome"
  },
  "priority": 5,
  "status": "pending",
  "attempts": 0,
  "max_attempts": 3,
  "run_at": "2026-11-02T15:02:06.970465+06:00",
  "locked_by": null,
  "locked_until": null,
  "last_error": null,
  "idempotency_key": "welcome-tisa",
  "created_at": "2026-11-02T15:02:06.971568+06:00",
  "updated_at": "2026-11-02T15:02:06.971568+06:00",
  "finished_at": null
}
```

Same request again (a client retry after a network timeout, for example):

```
HTTP/1.1 200 OK
Idempotent-Replayed: true

{ "id": 1, ... }            ← the original job, not a new one
```

Scheduled for later:

```
$ curl -X POST localhost:8080/v1/jobs -d '{"type":"echo","payload":{"report":"daily"},"run_at":"2030-01-01T09:00:00Z"}'

{ "id": 1, "type": "echo", "status": "pending", "run_at": "2030-01-01T15:00:00+06:00", ... }
```

Validation error:

```
$ curl -i -X POST localhost:8080/v1/jobs -d '{"type":"Send Email"}'

HTTP/1.1 400 Bad Request

{
  "error": {
    "code": "validation_failed",
    "message": "must be 1-64 characters: lowercase letters, digits, '_', '.', '-', starting with a letter",
    "field": "type"
  }
}
```

## Get a job

`GET /v1/jobs/{id}`

```
$ curl localhost:8080/v1/jobs/1

{
  "id": 1,
  "type": "send_email",
  "payload": { "to": "tisa@example.com", "subject": "Welcome" },
  "priority": 5,
  "status": "succeeded",
  "attempts": 1,
  "max_attempts": 3,
  "run_at": "2026-11-02T15:02:06.970465+06:00",
  "locked_by": null,
  "locked_until": null,
  "last_error": null,
  "idempotency_key": "welcome-tisa",
  "created_at": "2026-11-02T15:02:06.971568+06:00",
  "updated_at": "2026-11-02T15:02:07.03345+06:00",
  "finished_at": "2026-11-02T15:02:07.03345+06:00"
}
```

A job waiting for a retry (`flaky` with `{"fail_attempts": 1}`, after its first failure):

```json
{
  "id": 3,
  "type": "flaky",
  "status": "pending",
  "attempts": 1,
  "max_attempts": 3,
  "run_at": "2026-11-02T15:02:25.683993+06:00",
  "last_error": "simulated failure on attempt 1 of 1 planned failures",
  "finished_at": null
}
```

A job that failed permanently (invalid input, so no retries were used):

```json
{
  "id": 2,
  "type": "send_email",
  "payload": { "to": "not-an-email" },
  "status": "dead",
  "attempts": 1,
  "max_attempts": 3,
  "last_error": "permanent: invalid recipient \"not-an-email\"",
  "finished_at": "2026-11-02T15:02:24.515124+06:00"
}
```

Unknown id → `404`:

```json
{ "error": { "code": "not_found", "message": "job not found" } }
```

## List jobs

`GET /v1/jobs?status=&type=&limit=&before=`

| Query | Meaning |
|---|---|
| `status` | Only this status |
| `type` | Only this type |
| `limit` | 1–100, default 20 |
| `before` | Cursor: only jobs with a smaller id. Use `next_cursor` from the previous page |

Newest first. `next_cursor` is `null` on the last page.

```
$ curl 'localhost:8080/v1/jobs?limit=2'

{
  "jobs": [
    { "id": 6, "type": "noop", "status": "succeeded", ... },
    { "id": 5, "type": "noop", "status": "succeeded", ... }
  ],
  "next_cursor": "5"
}

$ curl 'localhost:8080/v1/jobs?limit=2&before=5'
→ jobs 4 and 3, "next_cursor": "3"

$ curl 'localhost:8080/v1/jobs?status=dead'
→ job 2, "next_cursor": null
```

## Cancel a job

`POST /v1/jobs/{id}/cancel`: only `pending` jobs (including scheduled ones).
Running jobs cannot be cancelled.

```
$ curl -X POST localhost:8080/v1/jobs/1/cancel
{ "id": 1, "status": "cancelled", "finished_at": "2026-11-02T15:02:24.48639+06:00", ... }

$ curl -i -X POST localhost:8080/v1/jobs/1/cancel
HTTP/1.1 409 Conflict

{
  "error": {
    "code": "invalid_state",
    "message": "invalid status transition: job 1 is cancelled, cannot become cancelled"
  }
}
```

## Retry a job

`POST /v1/jobs/{id}/retry`: only `dead` or `cancelled` jobs. Resets
`attempts` to 0 and keeps `last_error` for reference.

```
$ curl -X POST localhost:8080/v1/jobs/2/retry
{ "id": 2, "status": "pending", "attempts": 0, "last_error": "permanent: invalid recipient \"not-an-email\"", ... }
```

## Stats

`GET /v1/stats`

```json
{
  "counts": { "cancelled": 0, "dead": 1, "pending": 1, "running": 0, "succeeded": 1 },
  "total": 3
}
```

## Workers

`GET /v1/workers`: workers with a live heartbeat in Redis.

```json
{
  "workers": [
    {
      "id": "vm-11726",
      "types": ["echo", "flaky", "noop", "send_email", "sleep"],
      "concurrency": 10,
      "active": 0,
      "started_at": "2026-11-02T09:02:05.469946984Z",
      "last_seen": "2026-11-02T09:02:05.470156127Z"
    }
  ]
}
```

## Health

| Endpoint | 200 | 503 |
|---|---|---|
| `GET /healthz` | `{"status":"ok"}`, always while the process runs | |
| `GET /readyz` | `{"database":"up","status":"ready"}` | `{"database":"down","status":"unavailable"}` |

## Metrics

`GET /metrics` (API) and `GET :9091/metrics` (worker), Prometheus text format.
Excerpt:

```
goflow_http_requests_total{method="POST",route="POST /v1/jobs",code="201"} 3
goflow_http_requests_total{method="POST",route="POST /v1/jobs",code="200"} 1
goflow_http_requests_total{method="POST",route="POST /v1/jobs",code="400"} 1
goflow_jobs{status="dead"} 1
goflow_jobs{status="pending"} 1
goflow_jobs{status="succeeded"} 1
goflow_jobs_processed_total{type="send_email",outcome="dead"} 1
goflow_jobs_processed_total{type="send_email",outcome="succeeded"} 1
goflow_worker_active_jobs 0
```
