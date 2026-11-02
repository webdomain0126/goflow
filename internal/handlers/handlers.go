// Package handlers contains example job handlers. They show the
// patterns real handlers follow: decode the payload, respect ctx, return
// job.Permanent for errors a retry cannot fix, and be idempotent.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/mail"
	"time"

	"github.com/webdomain0126/goflow/internal/job"
	"github.com/webdomain0126/goflow/internal/worker"
)

// Register adds every example handler to w.
func Register(w *worker.Worker, log *slog.Logger) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	w.Handle("noop", Noop)
	w.Handle("echo", Echo(log))
	w.Handle("sleep", Sleep)
	w.Handle("flaky", Flaky)
	w.Handle("send_email", SendEmail(log))
}

// Noop does nothing. Used for throughput benchmarks.
func Noop(context.Context, job.Job) error { return nil }

// Echo logs the payload.
func Echo(log *slog.Logger) worker.HandlerFunc {
	return func(_ context.Context, j job.Job) error {
		log.Info("echo", "job_id", j.ID, "payload", json.RawMessage(j.Payload))
		return nil
	}
}

// decode reads the payload into T. A payload that does not match is a
// permanent error: retrying will never fix bad input.
func decode[T any](j job.Job) (T, error) {
	var v T
	if err := json.Unmarshal(j.Payload, &v); err != nil {
		return v, job.Permanent(fmt.Errorf("invalid payload: %w", err))
	}
	return v, nil
}

// Sleep waits for payload {"ms": N}, stopping early if ctx is cancelled.
func Sleep(ctx context.Context, j job.Job) error {
	p, err := decode[struct {
		Ms int `json:"ms"`
	}](j)
	if err != nil {
		return err
	}
	if p.Ms < 0 || p.Ms > 600_000 {
		return job.Permanent(errors.New("ms must be between 0 and 600000"))
	}
	select {
	case <-time.After(time.Duration(p.Ms) * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flaky fails its first N attempts (payload {"fail_attempts": N}), then
// succeeds. It demonstrates retries with exponential backoff.
func Flaky(_ context.Context, j job.Job) error {
	p, err := decode[struct {
		FailAttempts int `json:"fail_attempts"`
	}](j)
	if err != nil {
		return err
	}
	if j.Attempts <= p.FailAttempts {
		return fmt.Errorf("simulated failure on attempt %d of %d planned failures", j.Attempts, p.FailAttempts)
	}
	return nil
}

// SendEmail simulates sending an email to payload {"to": "...", "subject": "..."}.
// About 10% of sends fail with a temporary error to show retries.
func SendEmail(log *slog.Logger) worker.HandlerFunc {
	return func(ctx context.Context, j job.Job) error {
		p, err := decode[struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
		}](j)
		if err != nil {
			return err
		}
		if _, err := mail.ParseAddress(p.To); err != nil {
			return job.Permanent(fmt.Errorf("invalid recipient %q", p.To))
		}
		select { // pretend to talk to an email provider
		case <-time.After(time.Duration(20+rand.IntN(60)) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		if rand.IntN(10) == 0 {
			return errors.New("email provider timeout (simulated)")
		}
		log.Info("email sent", "job_id", j.ID, "to", p.To, "subject", p.Subject)
		return nil
	}
}
