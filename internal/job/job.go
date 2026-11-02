// Package job defines what a job is and the rules it follows.
//
// This package is pure Go: no database, no network. Everything else in
// GoFlow (the store, the worker, the API) depends on these types, so
// keeping them simple and well tested pays off everywhere.
package job

import (
	"encoding/json"
	"time"
)

// Status is the lifecycle state of a job.
//
// Declaring a named type (instead of using plain strings everywhere)
// lets the compiler catch mistakes like passing a job type where a
// status was expected.
type Status string

const (
	StatusPending   Status = "pending"   // waiting for run_at, or for a free worker
	StatusRunning   Status = "running"   // claimed by a worker that holds a lease
	StatusSucceeded Status = "succeeded" // finished without error (terminal)
	StatusCancelled Status = "cancelled" // cancelled by a user before it ran (terminal)
	StatusDead      Status = "dead"      // failed on every attempt (terminal)
)

// AllStatuses lists every status, in lifecycle order.
var AllStatuses = []Status{StatusPending, StatusRunning, StatusSucceeded, StatusCancelled, StatusDead}

// Valid reports whether s is one of the known statuses.
func (s Status) Valid() bool {
	for _, known := range AllStatuses {
		if s == known {
			return true
		}
	}
	return false
}

// Terminal reports whether a job in this status will never run again
// on its own. Only a manual retry can bring it back.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusCancelled || s == StatusDead
}

// transitions is the state machine: for each status, the statuses it
// may move to. Anything not listed here is a bug or a race, and the
// store refuses it.
var transitions = map[Status][]Status{
	StatusPending:   {StatusRunning, StatusCancelled},
	StatusRunning:   {StatusSucceeded, StatusPending, StatusDead}, // pending = retry later, or lease expired
	StatusSucceeded: {},
	StatusCancelled: {StatusPending}, // manual retry
	StatusDead:      {StatusPending}, // manual retry
}

// CanTransition reports whether a job may move from one status to another.
func CanTransition(from, to Status) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// Job is one unit of work and its full history.
type Job struct {
	ID          int64           `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`

	// LockedBy and LockedUntil form the lease: which worker owns a
	// running job, and until when. Pointers are nil when unlocked,
	// which JSON shows as null.
	LockedBy    *string    `json:"locked_by"`
	LockedUntil *time.Time `json:"locked_until"`

	LastError      *string    `json:"last_error"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	FinishedAt     *time.Time `json:"finished_at"`
}

// AttemptsLeft reports whether a failed run should be retried.
// Attempts already counts the run that just failed.
func (j Job) AttemptsLeft() bool {
	return j.Attempts < j.MaxAttempts
}

// ClaimRequest describes the work a worker is ready to take.
type ClaimRequest struct {
	WorkerID string
	Types    []string      // job types the worker has handlers for; empty means any type
	Limit    int           // claim at most this many jobs
	Lease    time.Duration // how long the worker owns each job before it must renew
}

// ListFilter selects jobs to list. Zero values mean "no filter".
type ListFilter struct {
	Status   Status
	Type     string
	BeforeID int64 // cursor: only jobs with id < BeforeID
	Limit    int
}
