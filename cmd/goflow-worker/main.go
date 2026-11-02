// Command goflow-worker claims jobs from PostgreSQL and runs them.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/webdomain0126/goflow/internal/app"
	"github.com/webdomain0126/goflow/internal/config"
	"github.com/webdomain0126/goflow/internal/coord"
	"github.com/webdomain0126/goflow/internal/handlers"
	"github.com/webdomain0126/goflow/internal/metrics"
	"github.com/webdomain0126/goflow/internal/worker"
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
	log := app.NewLogger(cfg, "goflow-worker")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := app.OpenStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer store.Close()

	reg := metrics.NewRegistry()
	opts := []worker.Option{worker.WithObserver(worker.NewMetricsObserver(reg))}

	redis, err := app.OpenRedis(ctx, cfg, log)
	if err != nil {
		return err
	}
	if redis != nil {
		defer redis.Close()
		opts = append(opts,
			worker.WithWakeups(redis.Wakeups(ctx)),
			worker.WithLimiter(redis.RateLimiter(cfg.RateLimits)),
		)
	}

	w := worker.New(store, worker.Config{
		Concurrency:     cfg.WorkerConcurrency,
		PollInterval:    cfg.PollInterval,
		JobTimeout:      cfg.JobTimeout,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, log, opts...)
	handlers.Register(w, log)

	var background sync.WaitGroup
	if redis != nil {
		started := time.Now().UTC()
		background.Add(1)
		go func() {
			defer background.Done()
			redis.RunHeartbeats(ctx, 5*time.Second, func() coord.WorkerInfo {
				return coord.WorkerInfo{ID: w.ID(), Types: w.Types(), Concurrency: w.Concurrency(),
					Active: w.ActiveCount(), StartedAt: started}
			})
		}()
	}

	// A small HTTP server for Prometheus and container health checks.
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", reg.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}` + "\n"))
	})
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("worker metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	}()

	runErr := w.Run(ctx) // blocks until Ctrl+C / SIGTERM, then drains running jobs

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metricsSrv.Shutdown(shutdownCtx)
	background.Wait()
	return runErr
}
