package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/backoff"
	"github.com/webdomain0126/goflow/internal/job"
)

// fastConfig keeps timings tiny so tests finish in milliseconds.
func fastConfig() Config {
	return Config{
		ID:                "test-worker",
		Concurrency:       4,
		Lease:             time.Second,
		HeartbeatInterval: 10 * time.Millisecond,
		PollInterval:      5 * time.Millisecond,
		ReapInterval:      time.Hour,
		JobTimeout:        5 * time.Second,
		ShutdownTimeout:   2 * time.Second,
		Backoff:           backoff.Exponential{Base: time.Millisecond, Max: 5 * time.Millisecond},
	}
}

// startWorker runs w in the background. The returned stop function
// cancels it and waits for Run to return.
func startWorker(t *testing.T, w *Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run returned error: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("worker did not stop within 10s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitFor polls cond until it is true or the timeout passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

type recordingObserver struct {
	mu       sync.Mutex
	outcomes map[Outcome]int
}

func (o *recordingObserver) JobFinished(_ string, out Outcome, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.outcomes == nil {
		o.outcomes = map[Outcome]int{}
	}
	o.outcomes[out]++
}
func (o *recordingObserver) ActiveJobs(int) {}
func (o *recordingObserver) count(out Outcome) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.outcomes[out]
}

func TestRunRequiresHandlers(t *testing.T) {
	w := New(newMemStore(), fastConfig(), nil)
	if err := w.Run(context.Background()); err == nil {
		t.Error("Run with no handlers should return an error")
	}
}

func TestProcessesAllJobs(t *testing.T) {
	s := newMemStore()
	for i := 0; i < 50; i++ {
		s.add("echo", 3, "")
	}
	var ran atomic.Int64
	w := New(s, fastConfig(), nil)
	w.Handle("echo", func(context.Context, job.Job) error { ran.Add(1); return nil })
	startWorker(t, w)

	waitFor(t, "50 jobs succeeded", func() bool { return s.countStatus(job.StatusSucceeded) == 50 })
	if ran.Load() != 50 {
		t.Errorf("handler ran %d times, want 50", ran.Load())
	}
	if s.maxDup > 1 {
		t.Errorf("a job was running %d times at once", s.maxDup)
	}
}

func TestRetriesThenSucceeds(t *testing.T) {
	s := newMemStore()
	id := s.add("flaky", 5, "")
	w := New(s, fastConfig(), nil)
	w.Handle("flaky", func(_ context.Context, j job.Job) error {
		if j.Attempts < 3 {
			return errors.New("temporary outage")
		}
		return nil
	})
	startWorker(t, w)

	waitFor(t, "job succeeded", func() bool { return s.get(id).Status == job.StatusSucceeded })
	if got := s.get(id).Attempts; got != 3 {
		t.Errorf("attempts = %d, want 3 (2 failures + 1 success)", got)
	}
}

func TestExhaustedAttemptsGoDead(t *testing.T) {
	s := newMemStore()
	id := s.add("broken", 3, "")
	w := New(s, fastConfig(), nil)
	w.Handle("broken", func(context.Context, job.Job) error { return errors.New("always fails") })
	startWorker(t, w)

	waitFor(t, "job dead", func() bool { return s.get(id).Status == job.StatusDead })
	j := s.get(id)
	if j.Attempts != 3 || j.LastError == nil || *j.LastError != "always fails" {
		t.Errorf("dead job = attempts %d, last_error %v", j.Attempts, j.LastError)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	s := newMemStore()
	id := s.add("validate", 10, "")
	w := New(s, fastConfig(), nil)
	w.Handle("validate", func(context.Context, job.Job) error {
		return job.Permanent(errors.New("payload missing 'email'"))
	})
	startWorker(t, w)

	waitFor(t, "job dead", func() bool { return s.get(id).Status == job.StatusDead })
	if got := s.get(id).Attempts; got != 1 {
		t.Errorf("attempts = %d, want 1 (no retries for permanent errors)", got)
	}
}

func TestPanicIsRecovered(t *testing.T) {
	s := newMemStore()
	bad := s.add("panics", 1, "")
	good := s.add("echo", 1, "")
	w := New(s, fastConfig(), nil)
	w.Handle("panics", func(context.Context, job.Job) error { panic("nil map write") })
	w.Handle("echo", func(context.Context, job.Job) error { return nil })
	startWorker(t, w)

	waitFor(t, "both jobs finished", func() bool {
		return s.get(bad).Status == job.StatusDead && s.get(good).Status == job.StatusSucceeded
	})
	if msg := *s.get(bad).LastError; !strings.Contains(msg, "panicked: nil map write") {
		t.Errorf("last_error = %q, want it to mention the panic", msg)
	}
}

func TestRespectsConcurrencyLimit(t *testing.T) {
	s := newMemStore()
	for i := 0; i < 20; i++ {
		s.add("slow", 1, "")
	}
	var running, maxRunning atomic.Int64
	cfg := fastConfig()
	cfg.Concurrency = 3
	w := New(s, cfg, nil)
	w.Handle("slow", func(context.Context, job.Job) error {
		n := running.Add(1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		running.Add(-1)
		return nil
	})
	startWorker(t, w)

	waitFor(t, "20 jobs done", func() bool { return s.countStatus(job.StatusSucceeded) == 20 })
	if m := maxRunning.Load(); m != 3 {
		t.Errorf("max concurrent jobs = %d, want exactly 3", m)
	}
}

func TestGracefulShutdownFinishesRunningJob(t *testing.T) {
	s := newMemStore()
	id := s.add("slow", 1, "")
	started := make(chan struct{})
	w := New(s, fastConfig(), nil)
	w.Handle("slow", func(context.Context, job.Job) error {
		close(started)
		time.Sleep(100 * time.Millisecond) // keeps working after shutdown begins
		return nil
	})
	stop := startWorker(t, w)

	<-started
	stop() // returns only after Run returns
	if st := s.get(id).Status; st != job.StatusSucceeded {
		t.Errorf("status after graceful shutdown = %s, want succeeded", st)
	}
}

func TestShutdownTimeoutReleasesJob(t *testing.T) {
	s := newMemStore()
	id := s.add("stuck", 3, "")
	started := make(chan struct{})
	cfg := fastConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond
	w := New(s, cfg, nil)
	w.Handle("stuck", func(ctx context.Context, _ job.Job) error {
		close(started)
		<-ctx.Done() // only stops when cancelled
		return ctx.Err()
	})
	stop := startWorker(t, w)

	<-started
	stop()
	j := s.get(id)
	if j.Status != job.StatusPending || j.Attempts != 0 {
		t.Errorf("after forced shutdown: status %s attempts %d, want pending with the attempt not counted", j.Status, j.Attempts)
	}
}

func TestLostLeaseCancelsJob(t *testing.T) {
	s := newMemStore()
	id := s.add("long", 3, "")
	obs := &recordingObserver{}
	started := make(chan struct{})
	w := New(s, fastConfig(), nil, WithObserver(obs))
	w.Handle("long", func(ctx context.Context, _ job.Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	startWorker(t, w)

	<-started
	s.mu.Lock()
	s.dropLeases[id] = true // simulate: the reaper gave the job to another worker
	s.mu.Unlock()

	waitFor(t, "lease_lost outcome", func() bool { return obs.count(OutcomeLeaseLost) == 1 })
}

func TestJobTimeoutCountsAsFailure(t *testing.T) {
	s := newMemStore()
	id := s.add("hangs", 1, "")
	cfg := fastConfig()
	cfg.JobTimeout = 20 * time.Millisecond
	w := New(s, cfg, nil)
	w.Handle("hangs", func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		return ctx.Err()
	})
	startWorker(t, w)

	waitFor(t, "job dead", func() bool { return s.get(id).Status == job.StatusDead })
	if msg := *s.get(id).LastError; !strings.Contains(msg, "deadline exceeded") {
		t.Errorf("last_error = %q, want a timeout error", msg)
	}
}

type denyOnceLimiter struct{ calls atomic.Int64 }

func (l *denyOnceLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	if l.calls.Add(1) == 1 {
		return false, time.Millisecond, nil
	}
	return true, 0, nil
}

func TestRateLimitedJobIsReleasedWithoutUsingAttempt(t *testing.T) {
	s := newMemStore()
	id := s.add("email", 1, "") // only 1 attempt: a counted release would kill it
	obs := &recordingObserver{}
	w := New(s, fastConfig(), nil, WithLimiter(&denyOnceLimiter{}), WithObserver(obs))
	w.Handle("email", func(context.Context, job.Job) error { return nil })
	startWorker(t, w)

	waitFor(t, "job succeeded", func() bool { return s.get(id).Status == job.StatusSucceeded })
	if obs.count(OutcomeReleased) != 1 || s.get(id).Attempts != 1 {
		t.Errorf("released %d times, attempts %d; want 1 release and 1 attempt", obs.count(OutcomeReleased), s.get(id).Attempts)
	}
}

type failingLimiter struct{}

func (failingLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return false, 0, errors.New("redis: connection refused")
}

func TestLimiterErrorFailsOpen(t *testing.T) {
	s := newMemStore()
	id := s.add("email", 1, "")
	w := New(s, fastConfig(), nil, WithLimiter(failingLimiter{}))
	w.Handle("email", func(context.Context, job.Job) error { return nil })
	startWorker(t, w)
	waitFor(t, "job succeeded despite limiter outage", func() bool { return s.get(id).Status == job.StatusSucceeded })
}

func TestWakeupClaimsImmediately(t *testing.T) {
	s := newMemStore()
	wake := make(chan struct{}, 1)
	cfg := fastConfig()
	cfg.PollInterval = time.Hour // without the wake-up, nothing would happen
	w := New(s, cfg, nil, WithWakeups(wake))
	w.Handle("echo", func(context.Context, job.Job) error { return nil })
	startWorker(t, w)

	time.Sleep(20 * time.Millisecond) // let the first (empty) claim happen
	id := s.add("echo", 1, "")
	wake <- struct{}{}
	waitFor(t, "job picked up via wake-up", func() bool { return s.get(id).Status == job.StatusSucceeded })
}

func TestOnlyClaimsRegisteredTypes(t *testing.T) {
	s := newMemStore()
	other := s.add("not_mine", 1, "")
	mine := s.add("echo", 1, "")
	w := New(s, fastConfig(), nil)
	w.Handle("echo", func(context.Context, job.Job) error { return nil })
	startWorker(t, w)

	waitFor(t, "own job done", func() bool { return s.get(mine).Status == job.StatusSucceeded })
	if st := s.get(other).Status; st != job.StatusPending {
		t.Errorf("job with no local handler: status %s, want it left pending for another worker", st)
	}
}
