package job

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Limits and defaults for new jobs.
const (
	DefaultPriority    = 0
	MinPriority        = 0
	MaxPriority        = 100
	DefaultMaxAttempts = 3
	MaxMaxAttempts     = 25
	MaxPayloadBytes    = 256 << 10 // 256 KiB
	MaxIdempotencyKey  = 128
)

// typePattern: lowercase name such as "send_email" or "report.daily".
var typePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

// CreateParams is everything a caller may set when creating a job.
// Zero values mean "use the default".
type CreateParams struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// Normalize fills in defaults and validates the parameters.
// now is passed in (instead of calling time.Now inside) so tests can
// control the clock.
func (p CreateParams) Normalize(now time.Time) (CreateParams, error) {
	p.Type = strings.TrimSpace(p.Type)
	if !typePattern.MatchString(p.Type) {
		return p, &ValidationError{Field: "type", Message: "must be 1-64 characters: lowercase letters, digits, '_', '.', '-', starting with a letter"}
	}

	if len(p.Payload) == 0 {
		p.Payload = json.RawMessage(`{}`)
	}
	if len(p.Payload) > MaxPayloadBytes {
		return p, &ValidationError{Field: "payload", Message: fmt.Sprintf("must be at most %d bytes", MaxPayloadBytes)}
	}
	if !json.Valid(p.Payload) {
		return p, &ValidationError{Field: "payload", Message: "must be valid JSON"}
	}

	if p.Priority < MinPriority || p.Priority > MaxPriority {
		return p, &ValidationError{Field: "priority", Message: fmt.Sprintf("must be between %d and %d", MinPriority, MaxPriority)}
	}

	if p.MaxAttempts == 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}
	if p.MaxAttempts < 1 || p.MaxAttempts > MaxMaxAttempts {
		return p, &ValidationError{Field: "max_attempts", Message: fmt.Sprintf("must be between 1 and %d", MaxMaxAttempts)}
	}

	if p.RunAt == nil || p.RunAt.Before(now) {
		// A time in the past simply means "as soon as possible".
		p.RunAt = &now
	}

	p.IdempotencyKey = strings.TrimSpace(p.IdempotencyKey)
	if len(p.IdempotencyKey) > MaxIdempotencyKey {
		return p, &ValidationError{Field: "idempotency_key", Message: fmt.Sprintf("must be at most %d characters", MaxIdempotencyKey)}
	}
	return p, nil
}
