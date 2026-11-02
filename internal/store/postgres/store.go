// Package postgres stores jobs in PostgreSQL. The jobs table is both
// the permanent record and the queue that workers claim work from.
//
// Every time-based decision uses the database clock (now()), never the
// clock of the machine running Go. With many workers on many machines,
// one shared clock avoids bugs caused by clocks that disagree.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/webdomain0126/goflow/internal/job"
)

// Store is safe for concurrent use: pgxpool hands each query its own
// connection from a shared pool.
type Store struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// Open connects to PostgreSQL and checks the connection works.
func Open(ctx context.Context, dsn string, log *slog.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Store{pool: pool, log: log}, nil
}

// Close releases all connections.
func (s *Store) Close() { s.pool.Close() }

// Ping checks the database is reachable (used by the readiness probe).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// columns is the column list every query returns, in the order scanJob reads them.
const columns = `id, type, payload, priority, status, attempts, max_attempts, run_at,
	locked_by, locked_until, last_error, idempotency_key, created_at, updated_at, finished_at`

func scanJob(row pgx.Row) (job.Job, error) {
	var j job.Job
	var status string
	var payload []byte
	err := row.Scan(&j.ID, &j.Type, &payload, &j.Priority, &status, &j.Attempts, &j.MaxAttempts, &j.RunAt,
		&j.LockedBy, &j.LockedUntil, &j.LastError, &j.IdempotencyKey, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt)
	if err != nil {
		return job.Job{}, err
	}
	j.Status = job.Status(status)
	j.Payload = payload
	return j, nil
}

func collectJobs(rows pgx.Rows) ([]job.Job, error) {
	defer rows.Close()
	jobs := []job.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Create inserts a job. If IdempotencyKey is set and a job with that key
// already exists, Create returns the existing job and created=false
// instead of making a duplicate.
func (s *Store) Create(ctx context.Context, p job.CreateParams) (j job.Job, created bool, err error) {
	p, err = p.Normalize(time.Now())
	if err != nil {
		return job.Job{}, false, err
	}
	var key *string
	if p.IdempotencyKey != "" {
		key = &p.IdempotencyKey
	}

	// ON CONFLICT DO NOTHING makes the duplicate check atomic: two
	// requests with the same key at the same moment cannot both insert.
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (type, payload, priority, max_attempts, run_at, idempotency_key)
		VALUES ($1, $2::jsonb, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+columns,
		p.Type, string(p.Payload), p.Priority, p.MaxAttempts, *p.RunAt, key)
	j, err = scanJob(row)
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) || key == nil {
		return job.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	// No row came back, so the key already existed: return that job.
	j, err = scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE idempotency_key = $1`, *key))
	if err != nil {
		return job.Job{}, false, fmt.Errorf("load job by idempotency key: %w", err)
	}
	return j, false, nil
}

// Get returns one job, or job.ErrNotFound.
func (s *Store) Get(ctx context.Context, id int64) (job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("get job %d: %w", id, err)
	}
	return j, nil
}

// List returns jobs newest first. It uses keyset pagination
// ("WHERE id < cursor") instead of OFFSET, so page 1000 is as fast as
// page 1 and rows inserted meanwhile never shift the pages.
func (s *Store) List(ctx context.Context, f job.ListFilter) ([]job.Job, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	var status, typ *string
	if f.Status != "" {
		v := string(f.Status)
		status = &v
	}
	if f.Type != "" {
		typ = &f.Type
	}
	var before *int64
	if f.BeforeID > 0 {
		before = &f.BeforeID
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+columns+` FROM jobs
		WHERE ($1::text   IS NULL OR status = $1)
		  AND ($2::text   IS NULL OR type = $2)
		  AND ($3::bigint IS NULL OR id < $3)
		ORDER BY id DESC
		LIMIT $4`, status, typ, before, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return collectJobs(rows)
}

// Claim locks up to Limit runnable jobs for the worker and marks them running.
//
// FOR UPDATE SKIP LOCKED is what makes many concurrent workers safe and
// fast: each worker locks the rows it picks, and other workers skip
// those rows instead of waiting for them. No job is claimed twice, and
// no worker blocks another.
func (s *Store) Claim(ctx context.Context, req job.ClaimRequest) ([]job.Job, error) {
	if req.Limit <= 0 {
		return nil, nil
	}
	var types []string
	if len(req.Types) > 0 {
		types = req.Types
	}
	rows, err := s.pool.Query(ctx, `
		WITH next AS (
			SELECT id FROM jobs
			WHERE status = 'pending' AND run_at <= now()
			  AND ($4::text[] IS NULL OR type = ANY($4))
			ORDER BY priority DESC, run_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs SET
			status       = 'running',
			attempts     = jobs.attempts + 1,
			locked_by    = $2,
			locked_until = now() + $3::interval,
			updated_at   = now()
		FROM next
		WHERE jobs.id = next.id
		RETURNING `+qualified("jobs"),
		req.Limit, req.WorkerID, req.Lease, types)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	// RETURNING does not keep the ORDER BY of the CTE, so sort again.
	slices.SortFunc(jobs, func(a, b job.Job) int {
		if a.Priority != b.Priority {
			return b.Priority - a.Priority
		}
		if c := a.RunAt.Compare(b.RunAt); c != 0 {
			return c
		}
		return int(a.ID - b.ID)
	})
	return jobs, nil
}

// qualified prefixes each column with a table name, needed in UPDATE ... FROM.
func qualified(table string) string {
	return table + `.id, ` + table + `.type, ` + table + `.payload, ` + table + `.priority, ` +
		table + `.status, ` + table + `.attempts, ` + table + `.max_attempts, ` + table + `.run_at, ` +
		table + `.locked_by, ` + table + `.locked_until, ` + table + `.last_error, ` +
		table + `.idempotency_key, ` + table + `.created_at, ` + table + `.updated_at, ` + table + `.finished_at`
}

// ExtendLeases pushes the lease of each still-owned job forward and
// returns the IDs that were extended. An ID missing from the result
// means the worker no longer owns that job (the reaper took it back).
func (s *Store) ExtendLeases(ctx context.Context, workerID string, ids []int64, lease time.Duration) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs SET locked_until = now() + $3::interval, updated_at = now()
		WHERE id = ANY($1) AND locked_by = $2 AND status = 'running'
		RETURNING id`, ids, workerID, lease)
	if err != nil {
		return nil, fmt.Errorf("extend leases: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// Complete marks a running job as succeeded. It returns job.ErrLeaseLost
// if workerID no longer owns the job.
func (s *Store) Complete(ctx context.Context, id int64, workerID string) error {
	return s.finish(ctx, `
		UPDATE jobs SET status = 'succeeded', finished_at = now(), updated_at = now(),
			locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = $1 AND locked_by = $2 AND status = 'running'`, id, workerID)
}

// ScheduleRetry puts a failed job back to pending, to run again after delay.
func (s *Store) ScheduleRetry(ctx context.Context, id int64, workerID, errMsg string, delay time.Duration) error {
	return s.finish(ctx, `
		UPDATE jobs SET status = 'pending', run_at = now() + $4::interval, updated_at = now(),
			locked_by = NULL, locked_until = NULL, last_error = $3
		WHERE id = $1 AND locked_by = $2 AND status = 'running'`, id, workerID, errMsg, delay)
}

// MarkDead records a job that failed and will not be retried automatically.
func (s *Store) MarkDead(ctx context.Context, id int64, workerID, errMsg string) error {
	return s.finish(ctx, `
		UPDATE jobs SET status = 'dead', finished_at = now(), updated_at = now(),
			locked_by = NULL, locked_until = NULL, last_error = $3
		WHERE id = $1 AND locked_by = $2 AND status = 'running'`, id, workerID, errMsg)
}

// Release hands a claimed job back without counting the attempt, for
// example when a rate limit says "not yet". It runs again after delay.
func (s *Store) Release(ctx context.Context, id int64, workerID string, delay time.Duration) error {
	return s.finish(ctx, `
		UPDATE jobs SET status = 'pending', run_at = now() + $3::interval, updated_at = now(),
			locked_by = NULL, locked_until = NULL, attempts = GREATEST(attempts - 1, 0)
		WHERE id = $1 AND locked_by = $2 AND status = 'running'`, id, workerID, delay)
}

// finish runs a lease-guarded UPDATE. The "locked_by = worker AND
// status = 'running'" condition means a worker whose lease was taken
// away cannot overwrite the result of whoever owns the job now.
func (s *Store) finish(ctx context.Context, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("update job %v: %w", args[0], err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrLeaseLost
	}
	return nil
}

// ReapExpired takes back running jobs whose lease ran out, which means
// their worker crashed or lost its connection. Jobs with attempts left
// go back to pending; the rest become dead. It returns how many jobs
// were reaped.
func (s *Store) ReapExpired(ctx context.Context, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
			SELECT id FROM jobs
			WHERE status = 'running' AND locked_until < now()
			ORDER BY locked_until
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs SET
			status       = CASE WHEN jobs.attempts < jobs.max_attempts THEN 'pending' ELSE 'dead' END,
			finished_at  = CASE WHEN jobs.attempts < jobs.max_attempts THEN NULL ELSE now() END,
			run_at       = now(),
			locked_by    = NULL,
			locked_until = NULL,
			last_error   = 'lease expired: worker stopped responding',
			updated_at   = now()
		FROM expired
		WHERE jobs.id = expired.id`, limit)
	if err != nil {
		return 0, fmt.Errorf("reap expired leases: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Cancel stops a pending job from ever running.
func (s *Store) Cancel(ctx context.Context, id int64) (job.Job, error) {
	return s.transition(ctx, id, job.StatusCancelled, `
		UPDATE jobs SET status = 'cancelled', finished_at = now(), updated_at = now()
		WHERE id = $1 AND status = 'pending'
		RETURNING `+columns)
}

// Retry manually re-queues a dead or cancelled job with a fresh set of attempts.
func (s *Store) Retry(ctx context.Context, id int64) (job.Job, error) {
	return s.transition(ctx, id, job.StatusPending, `
		UPDATE jobs SET status = 'pending', attempts = 0, run_at = now(),
			finished_at = NULL, updated_at = now()
		WHERE id = $1 AND status IN ('dead', 'cancelled')
		RETURNING `+columns)
}

// transition runs a conditional UPDATE and, if no row changed, explains
// why: the job does not exist, or its status does not allow the change.
func (s *Store) transition(ctx context.Context, id int64, to job.Status, sql string) (job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, sql, id))
	if err == nil {
		return j, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, fmt.Errorf("update job %d: %w", id, err)
	}
	current, err := s.Get(ctx, id)
	if err != nil {
		return job.Job{}, err
	}
	return current, fmt.Errorf("%w: job %d is %s, cannot become %s", job.ErrInvalidTransition, id, current.Status, to)
}

// CountByStatus returns how many jobs are in each status (for metrics).
func (s *Store) CountByStatus(ctx context.Context) (map[job.Status]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}
	defer rows.Close()
	counts := make(map[job.Status]int64, len(job.AllStatuses))
	for _, st := range job.AllStatuses {
		counts[st] = 0
	}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		counts[job.Status(st)] = n
	}
	return counts, rows.Err()
}
