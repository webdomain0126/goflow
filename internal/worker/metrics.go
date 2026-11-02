package worker

import (
	"sync/atomic"
	"time"

	"github.com/webdomain0126/goflow/internal/metrics"
)

// MetricsObserver records worker events as Prometheus metrics.
type MetricsObserver struct {
	finished *metrics.CounterVec
	duration *metrics.HistogramVec
	active   atomic.Int64
}

// NewMetricsObserver registers the worker metrics in reg.
func NewMetricsObserver(reg *metrics.Registry) *MetricsObserver {
	o := &MetricsObserver{
		finished: reg.NewCounterVec("goflow_jobs_processed_total", "Job runs finished by this worker, by type and outcome.", "type", "outcome"),
		duration: reg.NewHistogramVec("goflow_job_duration_seconds", "Job run time, by type.", metrics.DefaultBuckets, "type"),
	}
	reg.NewGaugeFunc("goflow_worker_active_jobs", "Jobs currently running in this worker.", func() map[string]float64 {
		return map[string]float64{"": float64(o.active.Load())}
	})
	return o
}

func (o *MetricsObserver) JobFinished(jobType string, outcome Outcome, d time.Duration) {
	o.finished.Inc(jobType, string(outcome))
	o.duration.Observe(d.Seconds(), jobType)
}

func (o *MetricsObserver) ActiveJobs(n int) { o.active.Store(int64(n)) }
