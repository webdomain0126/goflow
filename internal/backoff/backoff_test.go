package backoff

import (
	"testing"
	"time"
)

func TestDelayWithoutJitter(t *testing.T) {
	e := Exponential{Base: time.Second, Max: 30 * time.Second}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, // treated as 1
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second}, // 32s capped to 30s
		{1000, 30 * time.Second},
	}
	for _, tt := range tests {
		if got := e.Delay(tt.attempt); got != tt.want {
			t.Errorf("Delay(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestDelayNeverOverflows(t *testing.T) {
	e := Exponential{Base: time.Hour, Max: 365 * 24 * time.Hour}
	for attempt := 1; attempt <= 200; attempt++ {
		d := e.Delay(attempt)
		if d <= 0 || d > e.Max {
			t.Fatalf("Delay(%d) = %s, want within (0, %s]", attempt, d, e.Max)
		}
	}
}

func TestJitterRange(t *testing.T) {
	low := Exponential{Base: 10 * time.Second, Max: time.Hour, Jitter: true, Rand: func() float64 { return 0 }}
	high := Exponential{Base: 10 * time.Second, Max: time.Hour, Jitter: true, Rand: func() float64 { return 0.999999 }}

	// attempt 3 -> 40s without jitter, so jitter must stay within [20s, 40s).
	if got := low.Delay(3); got != 20*time.Second {
		t.Errorf("lowest jitter Delay(3) = %s, want 20s", got)
	}
	if got := high.Delay(3); got < 39*time.Second || got >= 40*time.Second {
		t.Errorf("highest jitter Delay(3) = %s, want just under 40s", got)
	}
}

func TestDefaultJitterUsesRealRandomness(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 20; i++ {
		seen[Default.Delay(5)] = true
	}
	if len(seen) < 2 {
		t.Error("20 jittered delays were all identical; jitter is not random")
	}
}
