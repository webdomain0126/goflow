// Command goflow-worker claims jobs from PostgreSQL and runs them.
//
// For now (milestone M0) it only loads configuration and logs that it
// started. The worker pool is added in M3.
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/webdomain0126/goflow/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "goflow-worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.SlogLevel()}))
	logger.Info("goflow-worker starting",
		"concurrency", cfg.WorkerConcurrency,
		"shutdown_timeout", cfg.ShutdownTimeout.String(),
	)

	logger.Info("goflow-worker stopped")
	return nil
}
