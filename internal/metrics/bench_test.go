package metrics

import "testing"

// Parallel benchmarks show how the mutex behaves when many goroutines
// (e.g. workers) record metrics at the same time.
func BenchmarkCounterIncParallel(b *testing.B) {
	c := NewRegistry().NewCounterVec("c_total", "c", "type", "outcome")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc("send_email", "succeeded")
		}
	})
}

func BenchmarkHistogramObserveParallel(b *testing.B) {
	h := NewRegistry().NewHistogramVec("h_seconds", "h", DefaultBuckets, "type")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Observe(0.042, "send_email")
		}
	})
}
