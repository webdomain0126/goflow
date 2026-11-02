//go:build integration

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/job"
)

func BenchmarkCreate(b *testing.B) {
	s := newBenchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := s.Create(ctx, job.CreateParams{Type: "noop"}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClaimComplete measures one full cycle per job: claim it, then
// mark it succeeded. Larger batches spread the claim query over more jobs.
func BenchmarkClaimComplete(b *testing.B) {
	for _, batch := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			s := newBenchStore(b)
			ctx := context.Background()
			if err := s.ExecForTest(ctx, "INSERT INTO jobs (type) SELECT 'noop' FROM generate_series(1, $1)", b.N); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			done := 0
			for done < b.N {
				jobs, err := s.Claim(ctx, job.ClaimRequest{WorkerID: "bench", Limit: batch, Lease: time.Minute})
				if err != nil {
					b.Fatal(err)
				}
				for _, j := range jobs {
					if err := s.Complete(ctx, j.ID, "bench"); err != nil {
						b.Fatal(err)
					}
				}
				done += len(jobs)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
		})
	}
}

func newBenchStore(b *testing.B) *Store {
	b.Helper()
	ctx := context.Background()
	s, err := Open(ctx, defaultTestDSN, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	if err := s.ExecForTest(ctx, "TRUNCATE jobs RESTART IDENTITY"); err != nil {
		b.Fatal(err)
	}
	return s
}
