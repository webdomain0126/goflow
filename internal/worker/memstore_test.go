package worker

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/webdomain0126/goflow/internal/job"
)

// memStore is an in-memory Store for unit tests. It follows the same
// rules as the PostgreSQL store (leases, attempts, status changes), so
// the worker's logic can be tested quickly and deterministically.
type memStore struct {
	mu      sync.Mutex
	nextID  int64
	jobs    map[int64]*job.Job
	running map[int64]int // how many times each job is running right now (must never exceed 1)
	maxDup  int

	// dropLeases makes ExtendLeases "lose" these job IDs.
	dropLeases map[int64]bool
}

func newMemStore() *memStore {
	return &memStore{jobs: map[int64]*job.Job{}, running: map[int64]int{}, dropLeases: map[int64]bool{}}
}

func (s *memStore) add(typ string, maxAttempts int, payload string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	if payload == "" {
		payload = "{}"
	}
	s.jobs[s.nextID] = &job.Job{ID: s.nextID, Type: typ, Payload: []byte(payload), Status: job.StatusPending,
		MaxAttempts: maxAttempts, RunAt: time.Now()}
	return s.nextID
}

func (s *memStore) get(id int64) job.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.jobs[id]
}

func (s *memStore) countStatus(st job.Status) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, j := range s.jobs {
		if j.Status == st {
			n++
		}
	}
	return n
}

func (s *memStore) Claim(_ context.Context, req job.ClaimRequest) ([]job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []job.Job
	ids := make([]int64, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		j := s.jobs[id]
		if len(out) >= req.Limit {
			break
		}
		if j.Status != job.StatusPending || j.RunAt.After(now) {
			continue
		}
		if len(req.Types) > 0 && !slices.Contains(req.Types, j.Type) {
			continue
		}
		until := now.Add(req.Lease)
		worker := req.WorkerID
		j.Status, j.Attempts, j.LockedBy, j.LockedUntil = job.StatusRunning, j.Attempts+1, &worker, &until
		s.running[id]++
		if s.running[id] > s.maxDup {
			s.maxDup = s.running[id]
		}
		out = append(out, *j)
	}
	return out, nil
}

func (s *memStore) ExtendLeases(_ context.Context, workerID string, ids []int64, lease time.Duration) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int64
	for _, id := range ids {
		j := s.jobs[id]
		if j == nil || s.dropLeases[id] || j.Status != job.StatusRunning || *j.LockedBy != workerID {
			continue
		}
		until := time.Now().Add(lease)
		j.LockedUntil = &until
		out = append(out, id)
	}
	return out, nil
}

// finish applies a lease-guarded update, like the real store.
func (s *memStore) finish(id int64, workerID string, update func(j *job.Job)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil || j.Status != job.StatusRunning || j.LockedBy == nil || *j.LockedBy != workerID || s.dropLeases[id] {
		return job.ErrLeaseLost
	}
	s.running[id]--
	j.LockedBy, j.LockedUntil = nil, nil
	update(j)
	return nil
}

// Like a real database call, the finishing methods fail if ctx is
// already cancelled or past its deadline.
func (s *memStore) Complete(ctx context.Context, id int64, w string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.finish(id, w, func(j *job.Job) { j.Status = job.StatusSucceeded })
}

func (s *memStore) ScheduleRetry(ctx context.Context, id int64, w, msg string, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.finish(id, w, func(j *job.Job) {
		j.Status, j.RunAt, j.LastError = job.StatusPending, time.Now().Add(d), &msg
	})
}

func (s *memStore) MarkDead(ctx context.Context, id int64, w, msg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.finish(id, w, func(j *job.Job) { j.Status, j.LastError = job.StatusDead, &msg })
}

func (s *memStore) Release(ctx context.Context, id int64, w string, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.finish(id, w, func(j *job.Job) {
		j.Status, j.RunAt, j.Attempts = job.StatusPending, time.Now().Add(d), j.Attempts-1
	})
}

func (s *memStore) ReapExpired(context.Context, int) (int64, error) { return 0, nil }
