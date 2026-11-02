package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestCounterOutput(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("jobs_total", "Jobs processed.", "type", "outcome")
	c.Inc("email", "succeeded")
	c.Inc("email", "succeeded")
	c.Add(3, "sms", "failed")

	var b strings.Builder
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		"# TYPE jobs_total counter",
		`jobs_total{type="email",outcome="succeeded"} 2`,
		`jobs_total{type="sms",outcome="failed"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogramVec("duration_seconds", "Duration.", []float64{0.1, 1}, "type")
	h.Observe(0.05, "a") // <= 0.1
	h.Observe(0.5, "a")  // <= 1
	h.Observe(5, "a")    // only +Inf

	var b strings.Builder
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		`duration_seconds_bucket{type="a",le="0.1"} 1`,
		`duration_seconds_bucket{type="a",le="1"} 2`,
		`duration_seconds_bucket{type="a",le="+Inf"} 3`,
		`duration_seconds_sum{type="a"} 5.55`,
		`duration_seconds_count{type="a"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestGaugeFuncAndEscaping(t *testing.T) {
	r := NewRegistry()
	r.NewGaugeFunc("jobs", "Jobs by status.", func() map[string]float64 {
		return map[string]float64{"pending": 4, `we"ird`: 1}
	}, "status")

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	out := rec.Body.String()
	if !strings.Contains(out, `jobs{status="pending"} 4`) || !strings.Contains(out, `jobs{status="we\"ird"} 1`) {
		t.Errorf("unexpected output:\n%s", out)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("hits_total", "Hits.", "route")
	h := r.NewHistogramVec("lat_seconds", "Latency.", DefaultBuckets, "route")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Inc("/x")
				h.Observe(0.01, "/x")
			}
		}()
	}
	wg.Wait()
	if c.Value("/x") != 5000 || h.Count("/x") != 5000 {
		t.Errorf("counter = %v, histogram count = %d, want 5000 each", c.Value("/x"), h.Count("/x"))
	}
}

func TestDuplicateNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("registering the same name twice should panic")
		}
	}()
	r := NewRegistry()
	r.NewCounterVec("x", "x")
	r.NewCounterVec("x", "x")
}
