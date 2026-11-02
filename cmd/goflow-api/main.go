// Command goflow-api serves the GoFlow REST API.
//
// For now (milestone M0) it only loads configuration and logs that it
// started. The HTTP server is added in M5.
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/webdomain0126/goflow/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "goflow-api:", err)
		os.Exit(1)
	}
}

// run holds the real program. Keeping main tiny means every error is
// returned to one place, and os.Exit is called in exactly one spot.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.SlogLevel()}))
	logger.Info("goflow-api starting", "http_addr", cfg.HTTPAddr)

	logger.Info("goflow-api stopped")
	return nil
}
