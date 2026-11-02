//go:build integration

// Package e2e tests the whole system together: HTTP API -> PostgreSQL ->
// Redis wake-up -> worker -> handler -> result visible through the API.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/api"
	"github.com/webdomain0126/goflow/internal/coord"
	"github.com/webdomain0126/goflow/internal/handlers"
	"github.com/webdomain0126/goflow/internal/job"
	"github.com/webdomain0126/goflow/internal/metrics"
	"github.com/webdomain0126/goflow/internal/store/postgres"
	"github.com/webdomain0126/goflow/internal/worker"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type system struct {
	url   string
	store *postgres.Store
}

// startSystem runs the API (in-process HTTP server) and one worker,
// both wired to real PostgreSQL and Redis.
func startSystem(t *testing.T, pollInterval time.Duration) *system {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	store, err := postgres.Open(ctx, env("GOFLOW_TEST_DATABASE_URL", "postgres://goflow:goflow@localhost:5432/goflow?sslmode=disable"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	store.ExecForTest(ctx, "TRUNCATE jobs RESTART IDENTITY")

	rc, err := coord.Open(env("GOFLOW_TEST_REDIS_URL", "redis://localhost:6379/15"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Ping(ctx); err != nil {
		t.Fatalf("redis: %v", err)
	}

	srv := httptest.NewServer(api.New(store, rc, nil, metrics.NewRegistry()).Handler())

	w := worker.New(store, worker.Config{ID: "e2e", Concurrency: 8, PollInterval: pollInterval,
		HeartbeatInterval: 100 * time.Millisecond}, nil, worker.WithWakeups(rc.Wakeups(ctx)))
	handlers.Register(w, nil)
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	t.Cleanup(func() {
		cancel()
		<-done
		srv.Close()
		rc.Close()
		store.Close()
	})
	time.Sleep(100 * time.Millisecond) // let the Redis subscription start
	return &system{url: srv.URL, store: store}
}

func (s *system) post(t *testing.T, path, body string) (int, job.Job) {
	t.Helper()
	resp, err := http.Post(s.url+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var j job.Job
	json.NewDecoder(resp.Body).Decode(&j)
	return resp.StatusCode, j
}

func (s *system) waitStatus(t *testing.T, id int64, want job.Status, timeout time.Duration) job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := http.Get(fmt.Sprintf("%s/v1/jobs/%d", s.url, id))
		if err != nil {
			t.Fatal(err)
		}
		var j job.Job
		json.NewDecoder(resp.Body).Decode(&j)
		resp.Body.Close()
		if j.Status == want {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %d: status %s after %s, want %s", id, j.Status, timeout, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestJobLifecycleOverHTTP(t *testing.T) {
	sys := startSystem(t, time.Hour) // polling effectively off: only Redis wake-ups can trigger work

	start := time.Now()
	code, j := sys.post(t, "/v1/jobs", `{"type":"echo","payload":{"hello":"world"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create: status %d", code)
	}
	done := sys.waitStatus(t, j.ID, job.StatusSucceeded, 3*time.Second)
	t.Logf("job created -> succeeded in %s (via Redis wake-up)", time.Since(start).Round(time.Millisecond))
	if done.Attempts != 1 || done.FinishedAt == nil {
		t.Errorf("finished job = %+v", done)
	}
}

func TestRetriesOverHTTP(t *testing.T) {
	sys := startSystem(t, 20*time.Millisecond)
	_, j := sys.post(t, "/v1/jobs", `{"type":"flaky","payload":{"fail_attempts":2},"max_attempts":5}`)
	done := sys.waitStatus(t, j.ID, job.StatusSucceeded, 15*time.Second) // backoff: ~1s + ~2s
	if done.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", done.Attempts)
	}
}

func TestPermanentFailureThenManualRetry(t *testing.T) {
	sys := startSystem(t, 20*time.Millisecond)
	_, j := sys.post(t, "/v1/jobs", `{"type":"send_email","payload":{"to":"not-an-email"}}`)
	dead := sys.waitStatus(t, j.ID, job.StatusDead, 3*time.Second)
	if dead.Attempts != 1 || dead.LastError == nil || !strings.Contains(*dead.LastError, "invalid recipient") {
		t.Errorf("dead job = attempts %d, last_error %v", dead.Attempts, dead.LastError)
	}

	code, retried := sys.post(t, fmt.Sprintf("/v1/jobs/%d/retry", j.ID), "")
	if code != http.StatusOK || retried.Status != job.StatusPending {
		t.Fatalf("manual retry: status %d, job %+v", code, retried)
	}
	sys.waitStatus(t, j.ID, job.StatusDead, 3*time.Second) // still bad input, so dead again
}

func TestScheduledJobWaitsForRunAt(t *testing.T) {
	sys := startSystem(t, 20*time.Millisecond)
	runAt := time.Now().Add(700 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	_, j := sys.post(t, "/v1/jobs", `{"type":"echo","run_at":"`+runAt+`"}`)

	time.Sleep(300 * time.Millisecond)
	if got, _ := sys.store.Get(context.Background(), j.ID); got.Status != job.StatusPending {
		t.Fatalf("job ran before run_at: status %s", got.Status)
	}
	sys.waitStatus(t, j.ID, job.StatusSucceeded, 3*time.Second)
}
