package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from, to Status
		want     bool
	}{
		{StatusPending, StatusRunning, true},
		{StatusPending, StatusCancelled, true},
		{StatusPending, StatusSucceeded, false}, // must run first
		{StatusRunning, StatusSucceeded, true},
		{StatusRunning, StatusPending, true}, // retry or expired lease
		{StatusRunning, StatusDead, true},
		{StatusRunning, StatusCancelled, false}, // cannot cancel mid-run
		{StatusSucceeded, StatusPending, false}, // success is final
		{StatusDead, StatusPending, true},       // manual retry
		{StatusCancelled, StatusPending, true},  // manual retry
		{StatusDead, StatusRunning, false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s->%s", tt.from, tt.to), func(t *testing.T) {
			if got := CanTransition(tt.from, tt.to); got != tt.want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestEveryStatusHasTransitionEntry(t *testing.T) {
	// Guards against adding a new status and forgetting the state machine.
	for _, s := range AllStatuses {
		if _, ok := transitions[s]; !ok {
			t.Errorf("status %q has no entry in transitions", s)
		}
	}
}

func TestStatusTerminal(t *testing.T) {
	terminal := map[Status]bool{StatusSucceeded: true, StatusCancelled: true, StatusDead: true}
	for _, s := range AllStatuses {
		if s.Terminal() != terminal[s] {
			t.Errorf("%s.Terminal() = %v, want %v", s, s.Terminal(), terminal[s])
		}
	}
	if Status("bogus").Valid() {
		t.Error(`Status("bogus").Valid() = true, want false`)
	}
}

func TestAttemptsLeft(t *testing.T) {
	j := Job{Attempts: 2, MaxAttempts: 3}
	if !j.AttemptsLeft() {
		t.Error("2 of 3 attempts used: want attempts left")
	}
	j.Attempts = 3
	if j.AttemptsLeft() {
		t.Error("3 of 3 attempts used: want no attempts left")
	}
}

func TestNormalizeDefaults(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	p, err := CreateParams{Type: "send_email"}.Normalize(now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(p.Payload) != "{}" {
		t.Errorf("Payload = %s, want {}", p.Payload)
	}
	if p.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", p.MaxAttempts, DefaultMaxAttempts)
	}
	if !p.RunAt.Equal(now) {
		t.Errorf("RunAt = %s, want now (%s)", p.RunAt, now)
	}
}

func TestNormalizeKeepsFutureRunAt(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)
	p, err := CreateParams{Type: "report", RunAt: &later}.Normalize(now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.RunAt.Equal(later) {
		t.Errorf("RunAt = %s, want %s", p.RunAt, later)
	}
}

func TestNormalizeInvalid(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name      string
		params    CreateParams
		wantField string
	}{
		{"empty type", CreateParams{}, "type"},
		{"uppercase type", CreateParams{Type: "SendEmail"}, "type"},
		{"type with space", CreateParams{Type: "send email"}, "type"},
		{"invalid json", CreateParams{Type: "a", Payload: json.RawMessage(`{bad`)}, "payload"},
		{"payload too large", CreateParams{Type: "a", Payload: json.RawMessage(`"` + strings.Repeat("x", MaxPayloadBytes) + `"`)}, "payload"},
		{"priority negative", CreateParams{Type: "a", Priority: -1}, "priority"},
		{"priority too high", CreateParams{Type: "a", Priority: 101}, "priority"},
		{"max attempts negative", CreateParams{Type: "a", MaxAttempts: -1}, "max_attempts"},
		{"max attempts too high", CreateParams{Type: "a", MaxAttempts: 26}, "max_attempts"},
		{"idempotency key too long", CreateParams{Type: "a", IdempotencyKey: strings.Repeat("k", 129)}, "idempotency_key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.params.Normalize(now)
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error = %v, want a *ValidationError", err)
			}
			if ve.Field != tt.wantField {
				t.Errorf("Field = %q, want %q", ve.Field, tt.wantField)
			}
		})
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("missing email address")
	err := fmt.Errorf("handler: %w", Permanent(base))

	if !IsPermanent(err) {
		t.Error("IsPermanent(wrapped permanent error) = false, want true")
	}
	if !errors.Is(err, base) {
		t.Error("errors.Is should still find the original error through Permanent")
	}
	if IsPermanent(base) {
		t.Error("IsPermanent(plain error) = true, want false")
	}
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) should be nil")
	}
}
