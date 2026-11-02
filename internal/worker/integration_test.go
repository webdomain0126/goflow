//go:build integration

package worker_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/backoff"
	"github.com/webdomain0126/goflow/internal/job"
	"github.com/webdomain0126/goflow/internal/store/postgres"
	"github.com/webdomain0126/goflow/internal/worker"
)

func openStore(t testing.TB) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("GOFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://goflow:goflow@localhost:5432/goflow?sslmode=disable"
	}
	ctx := context.Background()
	s, err := postgres.Open(ctx, dsn, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.ExecForTest(ctx, "TRUNCATE jobs RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
	return s
}

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func testConfig(id string) worker.Config {
	return worker.Config{
		ID:                id,
		Concurrency:       8,
		Lease:             2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
		ReapInterval:      100 * time.Millisecond,
		ShutdownTimeout:   5 * time.Second,
		Backoff:           backoff.Exponential{Base: 5 * time.Millisecond, Max: 50 * time.Millisecond, Jitter: true},
	}
}

// TestThreeWorkersShareTheQueue runs three workers against one database
// with flaky handlers and checks that every job reaches a final state,
// and that no job ever ran on two workers at the same time.
func TestThreeWorkersShareTheQueue(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	const total = 300
	for i := 0; i < total; i++ {
		if _, _, err := s.Create(ctx, job.CreateParams{Type: "flaky", MaxAttempts: 4, Priority: i % 10}); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	inFlight := map[int64]string{}
	var overlaps []string

	handler := func(workerID string) worker.HandlerFunc {
		return func(ctx context.Context, j job.Job) error {
			mu.Lock()
			if other, busy := inFlight[j.ID]; busy {
				overlaps = append(overlaps, fmt.Sprintf("job %d on %s and %s", j.ID, other, workerID))
			}
			inFlight[j.ID] = workerID
			mu.Unlock()
			defer func() { mu.Lock(); delete(inFlight, j.ID); mu.Unlock() }()

			time.Sleep(time.Duration(rand.IntN(5)) * time.Millisecond)
			if rand.IntN(100) < 30 { // 30% of runs fail and get retried
				return errors.New("random failure")
			}
			return nil
		}
	}

	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("worker-%d", i)
		w := worker.New(s, testConfig(id), nil)
		w.Handle("flaky", handler(id))
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(runCtx) }()
	}

	waitUntil(t, 30*time.Second, "all jobs finished", func() bool {
		c, _ := s.CountByStatus(ctx)
		return c[job.StatusSucceeded]+c[job.StatusDead] == total
	})
	stop()
	wg.Wait()

	if len(overlaps) > 0 {
		t.Errorf("jobs ran on two workers at once: %v", overlaps)
	}
	c, _ := s.CountByStatus(ctx)
	t.Logf("final: %d succeeded, %d dead (dead = failed 4 times in a row)", c[job.StatusSucceeded], c[job.StatusDead])
	if c[job.StatusSucceeded] < total*9/10 {
		t.Errorf("only %d of %d succeeded; with 30%% failure and 4 attempts, expect ~99%%", c[job.StatusSucceeded], total)
	}
}

// TestCrashedWorkerJobsAreRecovered simulates a worker that claimed jobs
// and then died without finishing them.
func TestCrashedWorkerJobsAreRecovered(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		s.Create(ctx, job.CreateParams{Type: "echo"})
	}

	// The "crashed" worker claims everything with a 300ms lease, then never heartbeats.
	claimed, err := s.Claim(ctx, job.ClaimRequest{WorkerID: "crashed", Limit: 10, Lease: 300 * time.Millisecond})
	if err != nil || len(claimed) != 5 {
		t.Fatalf("crashed worker claimed %d jobs, err %v", len(claimed), err)
	}

	w := worker.New(s, testConfig("survivor"), nil)
	w.Handle("echo", func(context.Context, job.Job) error { return nil })
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { w.Run(runCtx); close(done) }()
	defer func() { stop(); <-done }()

	waitUntil(t, 10*time.Second, "survivor finished the crashed worker's jobs", func() bool {
		c, _ := s.CountByStatus(ctx)
		return c[job.StatusSucceeded] == 5
	})
	j, _ := s.Get(ctx, claimed[0].ID)
	if j.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (one crashed run + one real run)", j.Attempts)
	}
}

// TestGracefulShutdownWithPostgres: a job that is running when shutdown
// starts is allowed to finish and is recorded as succeeded.
func TestGracefulShutdownWithPostgres(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	created, _, _ := s.Create(ctx, job.CreateParams{Type: "slow"})

	started := make(chan struct{})
	w := worker.New(s, testConfig("graceful"), nil)
	w.Handle("slow", func(context.Context, job.Job) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		return nil
	})
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { w.Run(runCtx); close(done) }()

	<-started
	stop()
	<-done
	j, _ := s.Get(ctx, created.ID)
	if j.Status != job.StatusSucceeded {
		t.Errorf("status = %s, want succeeded", j.Status)
	}
}
