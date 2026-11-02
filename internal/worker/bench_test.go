//go:build integration

package worker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/job"
	"github.com/webdomain0126/goflow/internal/worker"
)

// BenchmarkThroughput measures end-to-end jobs per second: real
// PostgreSQL, real worker pool, a handler that does nothing. It shows the
// overhead GoFlow itself adds per job.
func BenchmarkThroughput(b *testing.B) {
	for _, tc := range []struct{ workers, concurrency int }{
		{1, 1}, {1, 10}, {1, 50}, {3, 20},
	} {
		b.Run(fmt.Sprintf("workers=%d/concurrency=%d", tc.workers, tc.concurrency), func(b *testing.B) {
			s := openStore(b)
			ctx := context.Background()
			const jobs = 3000
			if err := s.ExecForTest(ctx, "INSERT INTO jobs (type) SELECT 'noop' FROM generate_series(1, $1)", jobs); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			start := time.Now()
			runCtx, stop := context.WithCancel(ctx)
			done := make(chan struct{}, tc.workers)
			for i := 0; i < tc.workers; i++ {
				cfg := testConfig(fmt.Sprintf("bench-%d", i))
				cfg.Concurrency = tc.concurrency
				w := worker.New(s, cfg, nil)
				w.Handle("noop", func(context.Context, job.Job) error { return nil })
				go func() { w.Run(runCtx); done <- struct{}{} }()
			}
			for {
				c, err := s.CountByStatus(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if c[job.StatusSucceeded] == jobs {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			elapsed := time.Since(start)
			stop()
			for i := 0; i < tc.workers; i++ {
				<-done
			}
			b.ReportMetric(float64(jobs)/elapsed.Seconds(), "jobs/s")
		})
	}
}
