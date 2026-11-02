package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

// fakeEnv turns a map into a lookupFunc, so tests never touch the
// real environment and can safely run in parallel.
func fakeEnv(vars map[string]string) lookupFunc {
	return func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(fakeEnv(nil))
	if err != nil {
		t.Fatalf("load with empty environment: unexpected error: %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.WorkerConcurrency != 10 {
		t.Errorf("WorkerConcurrency = %d, want 10", cfg.WorkerConcurrency)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 30s", cfg.ShutdownTimeout)
	}
	if cfg.SlogLevel() != slog.LevelInfo {
		t.Errorf("SlogLevel = %s, want INFO", cfg.SlogLevel())
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(fakeEnv(map[string]string{
		"GOFLOW_HTTP_ADDR":          ":9090",
		"GOFLOW_WORKER_CONCURRENCY": "25",
		"GOFLOW_SHUTDOWN_TIMEOUT":   "1m",
		"GOFLOW_LOG_LEVEL":          "debug",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":9090")
	}
	if cfg.WorkerConcurrency != 25 {
		t.Errorf("WorkerConcurrency = %d, want 25", cfg.WorkerConcurrency)
	}
	if cfg.ShutdownTimeout != time.Minute {
		t.Errorf("ShutdownTimeout = %s, want 1m", cfg.ShutdownTimeout)
	}
	if cfg.SlogLevel() != slog.LevelDebug {
		t.Errorf("SlogLevel = %s, want DEBUG", cfg.SlogLevel())
	}
}

// TestLoadInvalid is a table-driven test: each row is one bad input
// and a piece of text the error message must contain.
func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"concurrency not a number", map[string]string{"GOFLOW_WORKER_CONCURRENCY": "ten"}, "must be a whole number"},
		{"concurrency zero", map[string]string{"GOFLOW_WORKER_CONCURRENCY": "0"}, "between 1 and 1000"},
		{"concurrency too high", map[string]string{"GOFLOW_WORKER_CONCURRENCY": "5000"}, "between 1 and 1000"},
		{"timeout missing unit", map[string]string{"GOFLOW_SHUTDOWN_TIMEOUT": "30"}, "must be a duration"},
		{"timeout negative", map[string]string{"GOFLOW_SHUTDOWN_TIMEOUT": "-5s"}, "must be positive"},
		{"unknown log level", map[string]string{"GOFLOW_LOG_LEVEL": "loud"}, "GOFLOW_LOG_LEVEL"},
		{"poll interval too small", map[string]string{"GOFLOW_POLL_INTERVAL": "1ms"}, "at least 10ms"},
		{"rate limit missing number", map[string]string{"GOFLOW_RATE_LIMITS": "email"}, "GOFLOW_RATE_LIMITS"},
		{"rate limit zero", map[string]string{"GOFLOW_RATE_LIMITS": "email=0"}, "GOFLOW_RATE_LIMITS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(fakeEnv(tt.env))
			if err == nil {
				t.Fatalf("expected an error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(fakeEnv(map[string]string{
		"GOFLOW_WORKER_CONCURRENCY": "ten",  // parse error
		"GOFLOW_SHUTDOWN_TIMEOUT":   "soon", // parse error
		"GOFLOW_LOG_LEVEL":          "loud", // validation error
	}))
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	for _, want := range []string{"GOFLOW_WORKER_CONCURRENCY", "GOFLOW_SHUTDOWN_TIMEOUT", "GOFLOW_LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %s", err, want)
		}
	}
	// Each mistake must be reported exactly once.
	if n := strings.Count(err.Error(), "GOFLOW_WORKER_CONCURRENCY"); n != 1 {
		t.Errorf("GOFLOW_WORKER_CONCURRENCY reported %d times, want 1:\n%s", n, err)
	}
}

func TestEmptyValueUsesDefault(t *testing.T) {
	cfg, err := load(fakeEnv(map[string]string{"GOFLOW_HTTP_ADDR": ""}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want default %q", cfg.HTTPAddr, ":8080")
	}
}

func TestRateLimitsAndRedis(t *testing.T) {
	cfg, err := load(fakeEnv(map[string]string{
		"GOFLOW_RATE_LIMITS": " send_email=50, sms=10 ",
		"GOFLOW_REDIS_URL":   "disabled",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RateLimits["send_email"] != 50 || cfg.RateLimits["sms"] != 10 || len(cfg.RateLimits) != 2 {
		t.Errorf("RateLimits = %v", cfg.RateLimits)
	}
	if cfg.RedisEnabled() {
		t.Error("RedisEnabled() = true for GOFLOW_REDIS_URL=disabled")
	}

	def, _ := load(fakeEnv(nil))
	if !def.RedisEnabled() || len(def.RateLimits) != 0 {
		t.Errorf("defaults: RedisEnabled %v, RateLimits %v", def.RedisEnabled(), def.RateLimits)
	}
}
