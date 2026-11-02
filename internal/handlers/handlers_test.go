package handlers

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/job"
)

func mk(payload string, attempts int) job.Job {
	return job.Job{ID: 1, Payload: []byte(payload), Attempts: attempts}
}

func TestFlaky(t *testing.T) {
	ctx := context.Background()
	for attempt := 1; attempt <= 2; attempt++ {
		if err := Flaky(ctx, mk(`{"fail_attempts":2}`, attempt)); err == nil || job.IsPermanent(err) {
			t.Errorf("attempt %d: err = %v, want a retryable error", attempt, err)
		}
	}
	if err := Flaky(ctx, mk(`{"fail_attempts":2}`, 3)); err != nil {
		t.Errorf("attempt 3: err = %v, want success", err)
	}
}

func TestBadPayloadIsPermanent(t *testing.T) {
	ctx := context.Background()
	for name, err := range map[string]error{
		"flaky":      Flaky(ctx, mk(`{"fail_attempts":"two"}`, 1)),
		"sleep":      Sleep(ctx, mk(`{"ms":-5}`, 1)),
		"send_email": SendEmail(slog.New(slog.DiscardHandler))(ctx, mk(`{"to":"not-an-email"}`, 1)),
	} {
		if !job.IsPermanent(err) {
			t.Errorf("%s: err = %v, want a permanent error", name, err)
		}
	}
}

func TestSleepRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := Sleep(ctx, mk(`{"ms":60000}`, 1))
	if err == nil || time.Since(start) > time.Second {
		t.Errorf("Sleep ignored cancellation: err %v after %s", err, time.Since(start))
	}
}
