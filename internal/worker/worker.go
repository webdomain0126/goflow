// Package worker claims jobs from the store and runs them concurrently.
//
// One Worker runs these goroutines:
//
//	fetcher   - claims jobs when there is free capacity, sends them on a channel
//	executors - N goroutines that receive jobs from the channel and run handlers
//	heartbeat - renews the lease of every running job, so nobody steals it
//	reaper    - returns jobs whose worker crashed (expired lease) to the queue
//
// Shutdown is graceful: on cancel, the fetcher stops claiming, running
// jobs get ShutdownTimeout to finish, and anything still running after
// that is cancelled and released back to the queue for another worker.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/webdomain0126/goflow/internal/backoff"
	"github.com/webdomain0126/goflow/internal/job"
)

// HandlerFunc does the actual work for one job type. It must stop when
// ctx is cancelled, and it must be idempotent: because delivery is
// at-least-once, the same job can occasionally run twice (for example if
// the worker crashes after the work is done but before it is recorded).
type HandlerFunc func(ctx context.Context, j job.Job) error

// Store is the part of the database the worker needs. Declaring it here,
// where it is used, lets tests pass an in-memory fake.
type Store interface {
	Claim(ctx context.Context, req job.ClaimRequest) ([]job.Job, error)
	ExtendLeases(ctx context.Context, workerID string, ids []int64, lease time.Duration) ([]int64, error)
	Complete(ctx context.Context, id int64, workerID string) error
	ScheduleRetry(ctx context.Context, id int64, workerID, errMsg string, delay time.Duration) error
	MarkDead(ctx context.Context, id int64, workerID, errMsg string) error
	Release(ctx context.Context, id int64, workerID string, delay time.Duration) error
	ReapExpired(ctx context.Context, limit int) (int64, error)
}

// Limiter decides whether a job of a given type may start now (rate limiting).
type Limiter interface {
	Allow(ctx context.Context, jobType string) (ok bool, retryAfter time.Duration, err error)
}

// Observer receives events for metrics. All methods must be cheap and non-blocking.
type Observer interface {
	JobFinished(jobType string, outcome Outcome, d time.Duration)
	ActiveJobs(n int)
}

// Outcome is how one run of a job ended.
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeRetried   Outcome = "retried"
	OutcomeDead      Outcome = "dead"
	OutcomeReleased  Outcome = "released"   // handed back without counting (rate limit or shutdown)
	OutcomeLeaseLost Outcome = "lease_lost" // another worker owns it now; our result was discarded
)

// Config tunes a Worker. Zero values get sensible defaults.
type Config struct {
	ID                string        // unique per process; default: hostname-pid
	Concurrency       int           // max jobs running at once; default 10
	Lease             time.Duration // default 30s
	HeartbeatInterval time.Duration // default Lease/3
	PollInterval      time.Duration // how often to look for work without a wake-up; default 1s
	ReapInterval      time.Duration // default 10s
	JobTimeout        time.Duration // max run time of one job; default 5m
	ShutdownTimeout   time.Duration // default 30s
	Backoff           backoff.Exponential
}

func (c *Config) setDefaults() {
	if c.ID == "" {
		host, _ := os.Hostname()
		c.ID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.Lease / 3
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 10 * time.Second
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = 5 * time.Minute
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.Backoff.Base <= 0 {
		c.Backoff = backoff.Default
	}
}

// Worker runs jobs. Create it with New, register handlers, then call Run.
type Worker struct {
	cfg      Config
	store    Store
	log      *slog.Logger
	handlers map[string]HandlerFunc
	limiter  Limiter
	observer Observer
	wake     <-chan struct{}

	// storeTimeout limits each call that records a job's result.
	storeTimeout time.Duration

	mu     sync.Mutex
	active map[int64]context.CancelCauseFunc // running jobs and how to cancel each
	busy   atomic.Int64                      // claimed jobs not yet finished
}

// Option configures optional parts of a Worker.
type Option func(*Worker)

// WithLimiter rate-limits job starts per type.
func WithLimiter(l Limiter) Option { return func(w *Worker) { w.limiter = l } }

// WithObserver reports metrics events.
func WithObserver(o Observer) Option { return func(w *Worker) { w.observer = o } }

// WithWakeups makes the worker check for jobs immediately whenever a
// value arrives on ch (e.g. from Redis pub/sub), instead of waiting for
// the next poll.
func WithWakeups(ch <-chan struct{}) Option { return func(w *Worker) { w.wake = ch } }

func New(store Store, cfg Config, log *slog.Logger, opts ...Option) *Worker {
	cfg.setDefaults()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	w := &Worker{
		cfg:          cfg,
		store:        store,
		log:          log.With("worker_id", cfg.ID),
		handlers:     map[string]HandlerFunc{},
		active:       map[int64]context.CancelCauseFunc{},
		storeTimeout: 10 * time.Second,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// ID returns the worker's unique ID.
func (w *Worker) ID() string { return w.cfg.ID }

// Concurrency returns the maximum number of jobs run at once.
func (w *Worker) Concurrency() int { return w.cfg.Concurrency }

// ActiveCount returns how many jobs are currently claimed by this worker.
func (w *Worker) ActiveCount() int { return int(w.busy.Load()) }

// Handle registers the handler for a job type. Call it before Run.
func (w *Worker) Handle(jobType string, h HandlerFunc) {
	w.handlers[jobType] = h
}

// Types returns the registered job types, sorted.
func (w *Worker) Types() []string {
	types := make([]string, 0, len(w.handlers))
	for t := range w.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// errShutdown and errLeaseLost are causes attached to a job's context
// when the worker cancels it, so the job's error can be classified.
var (
	errShutdown  = errors.New("worker shutting down")
	errLeaseLost = errors.New("lease lost")
)

// Run processes jobs until ctx is cancelled, then shuts down gracefully.
// It returns nil after a clean shutdown.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.handlers) == 0 {
		return errors.New("worker: no handlers registered")
	}
	w.log.Info("worker started", "concurrency", w.cfg.Concurrency, "types", w.Types())

	// runCtx is the parent of every job context. It is deliberately NOT
	// derived from ctx: when ctx is cancelled we want running jobs to
	// keep going for up to ShutdownTimeout, not to stop instantly.
	runCtx, stopJobs := context.WithCancelCause(context.Background())
	defer stopJobs(nil)

	jobs := make(chan job.Job)                          // unbuffered: hand-off only when an executor is free
	slotFreed := make(chan struct{}, w.cfg.Concurrency) // executors tell the fetcher "I'm free"

	var executors sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		executors.Add(1)
		go func() {
			defer executors.Done()
			for j := range jobs { // ends when the fetcher closes jobs
				w.execute(runCtx, j)
				select {
				case slotFreed <- struct{}{}:
				default: // fetcher already knows there is room
				}
			}
		}()
	}

	// Background loops stop as soon as ctx is cancelled, except the
	// heartbeat, which must keep renewing leases while jobs drain.
	var background sync.WaitGroup
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	background.Add(2)
	go func() { defer background.Done(); w.heartbeatLoop(heartbeatCtx) }()
	go func() { defer background.Done(); w.reapLoop(ctx) }()

	w.fetchLoop(ctx, jobs, slotFreed) // blocks until ctx is cancelled
	close(jobs)

	// Graceful drain: wait for running jobs, but not forever.
	w.log.Info("worker draining", "running", w.ActiveCount(), "timeout", w.cfg.ShutdownTimeout.String())
	drained := make(chan struct{})
	go func() { executors.Wait(); close(drained) }()

	select {
	case <-drained:
	case <-time.After(w.cfg.ShutdownTimeout):
		w.log.Warn("shutdown timeout reached, cancelling running jobs", "running", w.ActiveCount())
		stopJobs(errShutdown)
		<-drained // handlers must respect ctx; each is released back to the queue
	}

	stopHeartbeat()
	background.Wait()
	w.log.Info("worker stopped")
	return nil
}

// fetchLoop claims jobs whenever there is free capacity and something
// tells it to look: a wake-up, a freed slot, or the poll timer.
func (w *Worker) fetchLoop(ctx context.Context, jobs chan<- job.Job, slotFreed <-chan struct{}) {
	poll := time.NewTicker(w.cfg.PollInterval)
	defer poll.Stop()

	for {
		free := w.cfg.Concurrency - int(w.busy.Load())
		full := false
		if free > 0 {
			claimed, err := w.store.Claim(ctx, job.ClaimRequest{
				WorkerID: w.cfg.ID, Types: w.Types(), Limit: free, Lease: w.cfg.Lease,
			})
			if err != nil && ctx.Err() == nil {
				w.log.Error("claim failed", "err", err)
			}
			for _, j := range claimed {
				w.busy.Add(1)
				w.track(j.ID, nil) // lease is ours from now on; heartbeat must renew it
				jobs <- j          // an executor is free because we only claimed `free` jobs
			}
			full = len(claimed) == free
		}
		w.reportActive()

		if full {
			// We filled every slot; there may be more work waiting. Loop
			// immediately once a slot frees up instead of waiting to poll.
			select {
			case <-ctx.Done():
				return
			case <-slotFreed:
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		case <-slotFreed:
		case <-w.wake: // nil channel (no Redis) blocks forever, which is exactly what we want
		}
	}
}

// track records a running job; cancel may be nil until the job starts.
func (w *Worker) track(id int64, cancel context.CancelCauseFunc) {
	w.mu.Lock()
	w.active[id] = cancel
	w.mu.Unlock()
}

func (w *Worker) untrack(id int64) {
	w.mu.Lock()
	delete(w.active, id)
	w.mu.Unlock()
	w.busy.Add(-1)
	w.reportActive()
}

func (w *Worker) reportActive() {
	if w.observer != nil {
		w.observer.ActiveJobs(w.ActiveCount())
	}
}

// execute runs one job and records the result. It never panics.
func (w *Worker) execute(runCtx context.Context, j job.Job) {
	defer w.untrack(j.ID)
	log := w.log.With("job_id", j.ID, "type", j.Type, "attempt", j.Attempts)
	start := time.Now()

	// The job's context is cancelled if: it runs longer than JobTimeout,
	// the worker loses the lease, or shutdown runs out of time.
	jobCtx, cancel := context.WithCancelCause(runCtx)
	defer cancel(nil)
	jobCtx, cancelTimeout := context.WithTimeout(jobCtx, w.cfg.JobTimeout)
	defer cancelTimeout()
	w.track(j.ID, cancel)

	if w.limiter != nil {
		ok, retryAfter, err := w.limiter.Allow(jobCtx, j.Type)
		if err != nil {
			// Fail open: rate limiting protects downstream systems, but a
			// Redis outage should not stop all work.
			log.Warn("rate limiter unavailable, running job anyway", "err", err)
		} else if !ok {
			w.record(log, j, OutcomeReleased, start, w.save(func(ctx context.Context) error { return w.store.Release(ctx, j.ID, w.cfg.ID, retryAfter) }))
			return
		}
	}

	err := w.runHandler(jobCtx, j)

	switch {
	case err == nil:
		w.record(log, j, OutcomeSucceeded, start, w.save(func(ctx context.Context) error { return w.store.Complete(ctx, j.ID, w.cfg.ID) }))

	case errors.Is(context.Cause(jobCtx), errLeaseLost):
		// The reaper gave this job to someone else; our result does not count.
		w.record(log, j, OutcomeLeaseLost, start, nil)

	case errors.Is(context.Cause(jobCtx), errShutdown):
		// Not the job's fault: give it back without counting the attempt.
		w.record(log, j, OutcomeReleased, start, w.save(func(ctx context.Context) error { return w.store.Release(ctx, j.ID, w.cfg.ID, 0) }))

	case job.IsPermanent(err) || !j.AttemptsLeft():
		log.Warn("job failed permanently", "err", err)
		w.record(log, j, OutcomeDead, start, w.save(func(ctx context.Context) error { return w.store.MarkDead(ctx, j.ID, w.cfg.ID, truncate(err.Error())) }))

	default:
		delay := w.cfg.Backoff.Delay(j.Attempts)
		log.Info("job failed, will retry", "err", err, "retry_in", delay.String())
		w.record(log, j, OutcomeRetried, start, w.save(func(ctx context.Context) error {
			return w.store.ScheduleRetry(ctx, j.ID, w.cfg.ID, truncate(err.Error()), delay)
		}))
	}
}

// save records a job's result with its own short timeout.
//
// The timeout starts here, when the result is written, and not when the
// job started: a job may legitimately run for minutes. The context is also
// independent of the worker's shutdown, so a job that just finished is
// still recorded as succeeded while the worker is stopping.
func (w *Worker) save(write func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.storeTimeout)
	defer cancel()
	return write(ctx)
}

// runHandler calls the handler and turns a panic into an ordinary error,
// so one buggy handler cannot crash the whole worker process.
func (w *Worker) runHandler(ctx context.Context, j job.Job) (err error) {
	h, ok := w.handlers[j.Type]
	if !ok {
		return job.Permanent(fmt.Errorf("no handler registered for type %q", j.Type))
	}
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("handler panicked", "job_id", j.ID, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, j)
}

func (w *Worker) record(log *slog.Logger, j job.Job, outcome Outcome, start time.Time, storeErr error) {
	d := time.Since(start)
	if errors.Is(storeErr, job.ErrLeaseLost) {
		outcome = OutcomeLeaseLost
		storeErr = nil
	}
	if storeErr != nil {
		// The result could not be saved. The lease will expire and the
		// reaper will re-queue the job: at-least-once delivery.
		log.Error("could not record job result", "outcome", outcome, "err", storeErr)
	} else {
		log.Debug("job finished", "outcome", outcome, "duration", d.String())
	}
	if w.observer != nil {
		w.observer.JobFinished(j.Type, outcome, d)
	}
}

// heartbeatLoop renews the lease of every running job. Jobs whose lease
// could not be renewed now belong to someone else, so they are cancelled.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		w.mu.Lock()
		ids := make([]int64, 0, len(w.active))
		for id := range w.active {
			ids = append(ids, id)
		}
		w.mu.Unlock()
		if len(ids) == 0 {
			continue
		}

		extended, err := w.store.ExtendLeases(ctx, w.cfg.ID, ids, w.cfg.Lease)
		if err != nil {
			// Keep running: a short database blip should not kill jobs.
			// If it lasts longer than the lease, the jobs will be reaped.
			w.log.Warn("heartbeat failed", "err", err)
			continue
		}
		kept := make(map[int64]bool, len(extended))
		for _, id := range extended {
			kept[id] = true
		}
		w.mu.Lock()
		for _, id := range ids {
			if cancel, ok := w.active[id]; ok && !kept[id] && cancel != nil {
				w.log.Warn("lease lost, cancelling job", "job_id", id)
				cancel(errLeaseLost)
			}
		}
		w.mu.Unlock()
	}
}

// reapLoop periodically re-queues jobs whose worker stopped heartbeating.
// Every worker runs it; SKIP LOCKED makes that safe.
func (w *Worker) reapLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := w.store.ReapExpired(ctx, 100)
		if err != nil && ctx.Err() == nil {
			w.log.Warn("reap failed", "err", err)
		} else if n > 0 {
			w.log.Warn("re-queued jobs with expired leases", "count", n)
		}
	}
}

func truncate(s string) string {
	const max = 2000
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
