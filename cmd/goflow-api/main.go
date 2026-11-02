// Command goflow-api serves the GoFlow REST API.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/webdomain0126/goflow/internal/api"
	"github.com/webdomain0126/goflow/internal/app"
	"github.com/webdomain0126/goflow/internal/config"
	"github.com/webdomain0126/goflow/internal/metrics"
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
	log := app.NewLogger(cfg, "goflow-api")

	// ctx is cancelled on Ctrl+C (SIGINT) or SIGTERM (what Docker and
	// Kubernetes send to stop a container).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := app.OpenStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer store.Close()

	redis, err := app.OpenRedis(ctx, cfg, log)
	if err != nil {
		return err
	}
	var coordinator api.Coordinator
	if redis != nil {
		defer redis.Close()
		coordinator = redis // assign only when non-nil: a nil *coord.Client inside an interface is not a nil interface
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(store, coordinator, log, metrics.NewRegistry()).Handler(),
		ReadHeaderTimeout: 5 * time.Second, // protects against slow-header (Slowloris) attacks
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("goflow-api listening", "addr", cfg.HTTPAddr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err) // e.g. port already in use
	case <-ctx.Done():
	}

	// Graceful shutdown: stop accepting connections and let in-flight
	// requests finish, but give up after ShutdownTimeout.
	log.Info("goflow-api shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("goflow-api stopped")
	return nil
}
