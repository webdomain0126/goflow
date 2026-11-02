// Package config loads GoFlow settings from environment variables.
//
// Every setting has a sensible default for local development, so the
// programs start with zero configuration. In production, each value is
// overridden with an environment variable (the "12-factor app" style).
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every setting the API and worker need.
type Config struct {
	HTTPAddr          string         // address the API listens on, e.g. ":8080"
	MetricsAddr       string         // address the worker serves /metrics and /healthz on
	DatabaseURL       string         // PostgreSQL connection string
	RedisURL          string         // Redis connection string, or "disabled"
	WorkerConcurrency int            // number of jobs one worker process runs at once
	PollInterval      time.Duration  // how often an idle worker checks for jobs
	JobTimeout        time.Duration  // maximum run time of a single job
	ShutdownTimeout   time.Duration  // how long to wait for running jobs on shutdown
	RateLimits        map[string]int // max job starts per second, per job type
	LogLevel          string         // debug, info, warn or error
}

// RedisEnabled reports whether Redis should be used at all.
func (c Config) RedisEnabled() bool { return c.RedisURL != "disabled" }

// Load reads the configuration from the real process environment.
func Load() (Config, error) {
	return load(os.LookupEnv)
}

// lookupFunc has the same shape as os.LookupEnv. Accepting it as a
// parameter lets tests pass a fake environment instead of changing
// real environment variables.
type lookupFunc func(key string) (string, bool)

func load(lookup lookupFunc) (Config, error) {
	cfg := Config{
		HTTPAddr:    getString(lookup, "GOFLOW_HTTP_ADDR", ":8080"),
		MetricsAddr: getString(lookup, "GOFLOW_METRICS_ADDR", ":9091"),
		DatabaseURL: getString(lookup, "GOFLOW_DATABASE_URL", "postgres://goflow:goflow@localhost:5432/goflow?sslmode=disable"),
		RedisURL:    getString(lookup, "GOFLOW_REDIS_URL", "redis://localhost:6379/0"),
		LogLevel:    getString(lookup, "GOFLOW_LOG_LEVEL", "info"),
	}

	// Collect every error instead of stopping at the first one, so a
	// user with three typos sees all three at once.
	var errs []error
	var err error

	cfg.WorkerConcurrency, err = getInt(lookup, "GOFLOW_WORKER_CONCURRENCY", 10)
	errs = append(errs, err)

	cfg.ShutdownTimeout, err = getDuration(lookup, "GOFLOW_SHUTDOWN_TIMEOUT", 30*time.Second)
	errs = append(errs, err)

	cfg.PollInterval, err = getDuration(lookup, "GOFLOW_POLL_INTERVAL", time.Second)
	errs = append(errs, err)

	cfg.JobTimeout, err = getDuration(lookup, "GOFLOW_JOB_TIMEOUT", 5*time.Minute)
	errs = append(errs, err)

	cfg.RateLimits, err = parseRateLimits(getString(lookup, "GOFLOW_RATE_LIMITS", ""))
	errs = append(errs, err)

	// A value that failed to parse was replaced by its default, so
	// validate never reports the same mistake a second time.
	errs = append(errs, cfg.validate())

	// errors.Join skips nil errors and returns nil if all were nil.
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validate checks that the values make sense together.
func (c Config) validate() error {
	var errs []error
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("GOFLOW_HTTP_ADDR must not be empty"))
	}
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("GOFLOW_DATABASE_URL must not be empty"))
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 1000 {
		errs = append(errs, fmt.Errorf("GOFLOW_WORKER_CONCURRENCY must be between 1 and 1000, got %d", c.WorkerConcurrency))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("GOFLOW_SHUTDOWN_TIMEOUT must be positive, got %s", c.ShutdownTimeout))
	}
	if c.PollInterval < 10*time.Millisecond {
		errs = append(errs, fmt.Errorf("GOFLOW_POLL_INTERVAL must be at least 10ms, got %s", c.PollInterval))
	}
	if c.JobTimeout <= 0 {
		errs = append(errs, fmt.Errorf("GOFLOW_JOB_TIMEOUT must be positive, got %s", c.JobTimeout))
	}
	if _, err := parseLogLevel(c.LogLevel); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// SlogLevel converts LogLevel into a log/slog level.
// It is safe to call on a Config returned by Load, which is already validated.
func (c Config) SlogLevel() slog.Level {
	level, _ := parseLogLevel(c.LogLevel)
	return level
}

func parseLogLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("GOFLOW_LOG_LEVEL must be one of debug, info, warn, error; got %q", s)
	}
}

// parseRateLimits reads "send_email=50,sms=10" (job starts per second, per type).
func parseRateLimits(s string) (map[string]int, error) {
	limits := map[string]int{}
	if strings.TrimSpace(s) == "" {
		return limits, nil
	}
	for _, part := range strings.Split(s, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if !ok || strings.TrimSpace(name) == "" || err != nil || n < 1 {
			return map[string]int{}, fmt.Errorf("GOFLOW_RATE_LIMITS must look like \"send_email=50,sms=10\" with positive numbers, got %q", s)
		}
		limits[strings.TrimSpace(name)] = n
	}
	return limits, nil
}

// getString returns the variable's value, or def if it is unset or empty.
func getString(lookup lookupFunc, key, def string) string {
	if v, ok := lookup(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(lookup lookupFunc, key string, def int) (int, error) {
	v, ok := lookup(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s must be a whole number, got %q", key, v)
	}
	return n, nil
}

func getDuration(lookup lookupFunc, key string, def time.Duration) (time.Duration, error) {
	v, ok := lookup(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("%s must be a duration like 30s or 1m, got %q", key, v)
	}
	return d, nil
}
