// Package app holds startup code shared by the goflow-api and
// goflow-worker programs: logging setup and connecting to dependencies.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/webdomain0126/goflow/internal/config"
	"github.com/webdomain0126/goflow/internal/coord"
	"github.com/webdomain0126/goflow/internal/store/postgres"
)

// NewLogger returns a JSON logger at the configured level.
func NewLogger(cfg config.Config, service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.SlogLevel()})
	return slog.New(h).With("service", service)
}

// OpenStore connects to PostgreSQL, retrying for up to 30 seconds (the
// database may still be starting, e.g. right after "docker compose up"),
// then applies migrations.
func OpenStore(ctx context.Context, cfg config.Config, log *slog.Logger) (*postgres.Store, error) {
	var lastErr error
	for attempt := 1; attempt <= 15; attempt++ {
		s, err := postgres.Open(ctx, cfg.DatabaseURL, log)
		if err == nil {
			if err := s.Migrate(ctx); err != nil {
				s.Close()
				return nil, fmt.Errorf("migrate: %w", err)
			}
			return s, nil
		}
		lastErr = err
		log.Warn("database not ready, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("connect to database: %w", lastErr)
}

// OpenRedis returns a Redis client, or nil if Redis is disabled. A Redis
// that is down at startup is not fatal: GoFlow keeps working without it
// and the client reconnects when Redis comes back.
func OpenRedis(ctx context.Context, cfg config.Config, log *slog.Logger) (*coord.Client, error) {
	if !cfg.RedisEnabled() {
		log.Info("redis disabled: workers will poll, no rate limits or worker registry")
		return nil, nil
	}
	c, err := coord.Open(cfg.RedisURL, log)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := c.Ping(pingCtx); err != nil {
		log.Warn("redis unreachable at startup; continuing without it until it comes back", "err", err)
	}
	return c, nil
}
