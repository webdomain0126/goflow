//go:build integration

// Integration tests run against a real PostgreSQL. They are skipped by a
// plain "go test" and run with:
//
//	docker compose up -d --wait
//	go test -tags=integration ./...
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/job"
)

const defaultTestDSN = "postgres://goflow:goflow@localhost:5432/goflow?sslmode=disable"

// newTestStore connects, migrates, and empties the jobs table, so every
// test starts from a clean, known state.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("GOFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, nil)
	if err != nil {
		t.Fatalf("open store (is PostgreSQL running? try: docker compose up -d --wait): %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := s.pool.Exec(ctx, "TRUNCATE jobs RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

func mustCreate(t *testing.T, s *Store, p job.CreateParams) job.Job {
	t.Helper()
	j, _, err := s.Create(context.Background(), p)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	return j
}

func TestMigrateIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("second migrate should be a no-op, got: %v", err)
	}
}

func TestCreateAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	created, isNew, err := s.Create(ctx, job.CreateParams{
		Type:     "send_email",
		Payload:  json.RawMessage(`{"to": "a@example.com"}`),
		Priority: 5,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !isNew {
		t.Error("created = false for a brand new job")
	}
	if created.ID == 0 || created.Status != job.StatusPending || created.Attempts != 0 {
		t.Errorf("unexpected new job: %+v", created)
	}

	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Type != "send_email" || got.Priority != 5 || got.MaxAttempts != job.DefaultMaxAttempts {
		t.Errorf("get returned %+v", got)
	}
	var payload map[string]string
	if err := json.Unmarshal(got.Payload, &payload); err != nil || payload["to"] != "a@example.com" {
		t.Errorf("payload = %s (err %v)", got.Payload, err)
	}
}

func TestGetNotFound(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Get(context.Background(), 999); !errors.Is(err, job.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestCreateRejectsInvalidParams(t *testing.T) {
	s := newTestStore(t)
	_, _, err := s.Create(context.Background(), job.CreateParams{Type: "Bad Type"})
	var ve *job.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("err = %v, want *job.ValidationError", err)
	}
}

func TestIdempotencyKeyUnderConcurrency(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// 20 goroutines create "the same" job at the same moment.
	const n = 20
	ids := make([]int64, n)
	created := make([]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j, isNew, err := s.Create(ctx, job.CreateParams{Type: "charge_card", IdempotencyKey: "order-42"})
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ids[i], created[i] = j.ID, isNew
		}(i)
	}
	wg.Wait()

	newCount := 0
	for i := 0; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("got different job IDs %d and %d for the same idempotency key", ids[0], ids[i])
		}
		if created[i] {
			newCount++
		}
	}
	if newCount != 1 {
		t.Errorf("%d calls reported created=true, want exactly 1", newCount)
	}
}

func TestClaimOrdersByPriorityThenRunAt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	past := time.Now().Add(-time.Minute)

	low := mustCreate(t, s, job.CreateParams{Type: "a", Priority: 1})
	high := mustCreate(t, s, job.CreateParams{Type: "a", Priority: 9})
	older := mustCreate(t, s, job.CreateParams{Type: "a", Priority: 1, RunAt: &past})
	_ = mustCreate(t, s, job.CreateParams{Type: "a", Priority: 1})

	// RunAt in the past is normalized to "now", so make "older" really older.
	if _, err := s.pool.Exec(ctx, "UPDATE jobs SET run_at = now() - interval '1 hour' WHERE id = $1", older.ID); err != nil {
		t.Fatal(err)
	}

	claimed, err := s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 3, Lease: time.Minute})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d jobs, want 3", len(claimed))
	}
	want := []int64{high.ID, older.ID, low.ID}
	for i, j := range claimed {
		if j.ID != want[i] {
			t.Errorf("claim order[%d] = job %d, want job %d", i, j.ID, want[i])
		}
		if j.Status != job.StatusRunning || j.Attempts != 1 || j.LockedBy == nil || *j.LockedBy != "w1" {
			t.Errorf("claimed job not marked running by w1: %+v", j)
		}
	}
}

func TestClaimFiltersByType(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, s, job.CreateParams{Type: "email", Priority: 9})
	sms := mustCreate(t, s, job.CreateParams{Type: "sms"})

	claimed, err := s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Types: []string{"sms"}, Limit: 10, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != sms.ID {
		t.Errorf("claimed %v, want only the sms job %d", ids(claimed), sms.ID)
	}
}

func TestClaimSkipsFutureJobs(t *testing.T) {
	s := newTestStore(t)
	later := time.Now().Add(time.Hour)
	mustCreate(t, s, job.CreateParams{Type: "later", RunAt: &later})

	claimed, err := s.Claim(context.Background(), job.ClaimRequest{WorkerID: "w1", Limit: 10, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Errorf("claimed %d jobs scheduled for the future, want 0", len(claimed))
	}
}

// TestConcurrentClaimNeverDoublesUp is the most important test in the
// store: many workers claim at the same time, and every job must be
// claimed exactly once.
func TestConcurrentClaimNeverDoublesUp(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const totalJobs = 2000
	if _, err := s.pool.Exec(ctx,
		"INSERT INTO jobs (type, max_attempts) SELECT 'bulk', 3 FROM generate_series(1, $1)", totalJobs); err != nil {
		t.Fatal(err)
	}

	const workers = 20
	var mu sync.Mutex
	claimedBy := make(map[int64]string, totalJobs)
	var wg sync.WaitGroup
	// start is a barrier: closing it releases every goroutine at once,
	// so the workers really do race for the same rows.
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			<-start
			for {
				jobs, err := s.Claim(ctx, job.ClaimRequest{WorkerID: worker, Limit: 10, Lease: time.Minute})
				if err != nil {
					t.Errorf("%s claim: %v", worker, err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if prev, dup := claimedBy[j.ID]; dup {
						t.Errorf("job %d claimed by both %s and %s", j.ID, prev, worker)
					}
					claimedBy[j.ID] = worker
				}
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%02d", w))
	}
	close(start)
	wg.Wait()

	if len(claimedBy) != totalJobs {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimedBy), totalJobs)
	}
}

func TestCompleteRequiresLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, s, job.CreateParams{Type: "a"})
	claimed, _ := s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 1, Lease: time.Minute})
	id := claimed[0].ID

	if err := s.Complete(ctx, id, "w2"); !errors.Is(err, job.ErrLeaseLost) {
		t.Errorf("complete by non-owner: err = %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, id, "w1"); err != nil {
		t.Fatalf("complete by owner: %v", err)
	}
	j, _ := s.Get(ctx, id)
	if j.Status != job.StatusSucceeded || j.FinishedAt == nil || j.LockedBy != nil {
		t.Errorf("after complete: %+v", j)
	}
	if err := s.Complete(ctx, id, "w1"); !errors.Is(err, job.ErrLeaseLost) {
		t.Errorf("completing twice: err = %v, want ErrLeaseLost", err)
	}
}

func TestScheduleRetryAndMarkDead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, s, job.CreateParams{Type: "a", MaxAttempts: 2})

	claimed, _ := s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 1, Lease: time.Minute})
	id := claimed[0].ID
	if err := s.ScheduleRetry(ctx, id, "w1", "boom", time.Hour); err != nil {
		t.Fatal(err)
	}
	j, _ := s.Get(ctx, id)
	if j.Status != job.StatusPending || j.LastError == nil || *j.LastError != "boom" {
		t.Errorf("after retry: %+v", j)
	}
	if time.Until(j.RunAt) < 59*time.Minute {
		t.Errorf("run_at = %s, want about 1 hour from now", j.RunAt)
	}

	// Not runnable yet because of the delay.
	if again, _ := s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 1, Lease: time.Minute}); len(again) != 0 {
		t.Fatal("job was claimable before its retry delay passed")
	}
	s.pool.Exec(ctx, "UPDATE jobs SET run_at = now() WHERE id = $1", id)
	claimed, _ = s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 1, Lease: time.Minute})
	if len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("second claim: %+v", claimed)
	}
	if err := s.MarkDead(ctx, id, "w1", "boom again"); err != nil {
		t.Fatal(err)
	}
	j, _ = s.Get(ctx, id)
	if j.Status != job.StatusDead || j.FinishedAt == nil {
		t.Errorf("after mark dead: %+v", j)
	}
}

func TestReleaseDoesNotCountAttempt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, s, job.CreateParams{Type: "a"})
	claimed, _ := s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 1, Lease: time.Minute})

	if err := s.Release(ctx, claimed[0].ID, "w1", 0); err != nil {
		t.Fatal(err)
	}
	j, _ := s.Get(ctx, claimed[0].ID)
	if j.Status != job.StatusPending || j.Attempts != 0 {
		t.Errorf("after release: status %s attempts %d, want pending 0", j.Status, j.Attempts)
	}
}

func TestExtendLeases(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, s, job.CreateParams{Type: "a"})
	mustCreate(t, s, job.CreateParams{Type: "a"})
	claimed, _ := s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 2, Lease: time.Second})

	extended, err := s.ExtendLeases(ctx, "w1", []int64{claimed[0].ID, claimed[1].ID, 12345}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(extended) != 2 {
		t.Errorf("extended %v, want both claimed jobs and not the unknown one", extended)
	}
	j, _ := s.Get(ctx, claimed[0].ID)
	if time.Until(*j.LockedUntil) < 59*time.Minute {
		t.Errorf("locked_until = %s, want about 1 hour from now", j.LockedUntil)
	}

	other, _ := s.ExtendLeases(ctx, "w2", []int64{claimed[0].ID}, time.Hour)
	if len(other) != 0 {
		t.Error("a worker extended a lease it does not own")
	}
}

func TestReapExpired(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	retryable := mustCreate(t, s, job.CreateParams{Type: "a", MaxAttempts: 3})
	lastTry := mustCreate(t, s, job.CreateParams{Type: "a", MaxAttempts: 1})
	healthy := mustCreate(t, s, job.CreateParams{Type: "a", MaxAttempts: 3})
	s.Claim(ctx, job.ClaimRequest{WorkerID: "crashed-worker", Limit: 3, Lease: time.Minute})

	// Simulate a crash: two leases ran out, one is still valid.
	s.pool.Exec(ctx, "UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = ANY($1)",
		[]int64{retryable.ID, lastTry.ID})

	n, err := s.ReapExpired(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("reaped %d jobs, want 2", n)
	}
	if j, _ := s.Get(ctx, retryable.ID); j.Status != job.StatusPending || j.LockedBy != nil {
		t.Errorf("job with attempts left: %+v, want pending and unlocked", j)
	}
	if j, _ := s.Get(ctx, lastTry.ID); j.Status != job.StatusDead {
		t.Errorf("job with no attempts left: status %s, want dead", j.Status)
	}
	if j, _ := s.Get(ctx, healthy.ID); j.Status != job.StatusRunning {
		t.Errorf("job with valid lease: status %s, want running", j.Status)
	}
	// The crashed worker must not be able to complete a reaped job.
	if err := s.Complete(ctx, retryable.ID, "crashed-worker"); !errors.Is(err, job.ErrLeaseLost) {
		t.Errorf("late complete after reap: err = %v, want ErrLeaseLost", err)
	}
}

func TestCancelAndRetry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.CreateParams{Type: "a"})

	cancelled, err := s.Cancel(ctx, j.ID)
	if err != nil || cancelled.Status != job.StatusCancelled {
		t.Fatalf("cancel: %+v, %v", cancelled, err)
	}
	if _, err := s.Cancel(ctx, j.ID); !errors.Is(err, job.ErrInvalidTransition) {
		t.Errorf("cancel twice: err = %v, want ErrInvalidTransition", err)
	}
	retried, err := s.Retry(ctx, j.ID)
	if err != nil || retried.Status != job.StatusPending || retried.Attempts != 0 {
		t.Fatalf("retry: %+v, %v", retried, err)
	}
	if _, err := s.Retry(ctx, j.ID); !errors.Is(err, job.ErrInvalidTransition) {
		t.Errorf("retry a pending job: err = %v, want ErrInvalidTransition", err)
	}
	if _, err := s.Cancel(ctx, 999); !errors.Is(err, job.ErrNotFound) {
		t.Errorf("cancel unknown job: err = %v, want ErrNotFound", err)
	}
}

func TestListPaginationAndFilters(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		mustCreate(t, s, job.CreateParams{Type: "email"})
	}
	for i := 0; i < 3; i++ {
		mustCreate(t, s, job.CreateParams{Type: "sms"})
	}

	page1, err := s.List(ctx, job.ListFilter{Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 4 || page1[0].ID != 8 || page1[3].ID != 5 {
		t.Fatalf("page 1 IDs wrong: %v", ids(page1))
	}
	page2, _ := s.List(ctx, job.ListFilter{Limit: 4, BeforeID: page1[3].ID})
	if len(page2) != 4 || page2[0].ID != 4 {
		t.Errorf("page 2 IDs wrong: %v", ids(page2))
	}

	sms, _ := s.List(ctx, job.ListFilter{Type: "sms"})
	if len(sms) != 3 {
		t.Errorf("type filter returned %d jobs, want 3", len(sms))
	}
	running, _ := s.List(ctx, job.ListFilter{Status: job.StatusRunning})
	if len(running) != 0 {
		t.Errorf("status filter returned %d running jobs, want 0", len(running))
	}
}

func TestCountByStatus(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreate(t, s, job.CreateParams{Type: "a"})
	mustCreate(t, s, job.CreateParams{Type: "a"})
	s.Claim(ctx, job.ClaimRequest{WorkerID: "w1", Limit: 1, Lease: time.Minute})

	counts, err := s.CountByStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[job.StatusPending] != 1 || counts[job.StatusRunning] != 1 || counts[job.StatusDead] != 0 {
		t.Errorf("counts = %v", counts)
	}
}

func ids(jobs []job.Job) []int64 {
	out := make([]int64, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}
