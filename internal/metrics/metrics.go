// Package metrics is a small, dependency-free implementation of the
// Prometheus text format (counters, gauges and histograms with labels).
//
// A real company would usually import prometheus/client_golang. GoFlow
// writes its own ~200 lines instead, to keep dependencies minimal and
// to show what a metrics library actually does underneath.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds every metric and writes them out for Prometheus.
type Registry struct {
	mu      sync.Mutex
	metrics []writer
	names   map[string]bool
}

type writer interface {
	write(w io.Writer)
}

func NewRegistry() *Registry {
	return &Registry{names: map[string]bool{}}
}

func (r *Registry) register(name string, m writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names[name] {
		panic("metrics: duplicate metric name " + name) // a programming error, caught at startup
	}
	r.names[name] = true
	r.metrics = append(r.metrics, m)
}

// Handler serves the metrics in Prometheus text format (for GET /metrics).
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

// Write writes every metric to w.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	ms := append([]writer(nil), r.metrics...)
	r.mu.Unlock()
	for _, m := range ms {
		m.write(w)
	}
}

// labelKey joins label values into one map key. \xff cannot appear in UTF-8 text.
func labelKey(values []string) string { return strings.Join(values, "\xff") }

func formatLabels(names, values []string, extra ...string) string {
	if len(names) == 0 && len(extra) == 0 {
		return ""
	}
	parts := make([]string, 0, len(names)+1)
	for i, n := range names {
		parts = append(parts, n+`="`+escape(values[i])+`"`)
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, extra[i]+`="`+escape(extra[i+1])+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

var escaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escape(s string) string { return escaper.Replace(s) }

func formatFloat(f float64) string {
	if math.IsInf(f, +1) {
		return "+Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// ---- Counter ---------------------------------------------------------

// CounterVec is a set of counters that only go up, one per label combination.
type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	values     map[string]float64
	labelVals  map[string][]string
}

func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labels, values: map[string]float64{}, labelVals: map[string][]string{}}
	r.register(name, c)
	return c
}

// Inc adds 1 to the counter for the given label values.
func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Add adds v (which must not be negative) to the counter.
func (c *CounterVec) Add(v float64, labelValues ...string) {
	if len(labelValues) != len(c.labels) || v < 0 {
		panic(fmt.Sprintf("metrics: %s: bad Add(%v, %v)", c.name, v, labelValues))
	}
	k := labelKey(labelValues)
	c.mu.Lock()
	c.values[k] += v
	if _, ok := c.labelVals[k]; !ok {
		c.labelVals[k] = append([]string(nil), labelValues...)
	}
	c.mu.Unlock()
}

// Value returns the current count (useful in tests).
func (c *CounterVec) Value(labelValues ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[labelKey(labelValues)]
}

func (c *CounterVec) write(w io.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
	for _, k := range sortedKeys(c.values) {
		fmt.Fprintf(w, "%s%s %s\n", c.name, formatLabels(c.labels, c.labelVals[k]), formatFloat(c.values[k]))
	}
}

// ---- Gauge -----------------------------------------------------------

// GaugeFunc reports a value computed at scrape time, such as "jobs
// currently running". The function returns one value per label set.
type GaugeFunc struct {
	name, help string
	labels     []string
	fn         func() map[string]float64 // key: the single label value ("" if no labels)
}

// NewGaugeFunc registers a gauge with at most one label.
func (r *Registry) NewGaugeFunc(name, help string, fn func() map[string]float64, label ...string) {
	if len(label) > 1 {
		panic("metrics: NewGaugeFunc supports at most one label")
	}
	r.register(name, &GaugeFunc{name: name, help: help, labels: label, fn: fn})
}

func (g *GaugeFunc) write(w io.Writer) {
	values := g.fn()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
	for _, k := range sortedKeys(values) {
		labels := ""
		if len(g.labels) == 1 {
			labels = formatLabels(g.labels, []string{k})
		}
		fmt.Fprintf(w, "%s%s %s\n", g.name, labels, formatFloat(values[k]))
	}
}

// ---- Histogram -------------------------------------------------------

// DefaultBuckets suit job and request durations, in seconds.
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// HistogramVec counts observations (like durations) into buckets, which
// lets Prometheus compute percentiles such as p95 latency.
type HistogramVec struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*histogram
}

type histogram struct {
	labelValues []string
	counts      []uint64 // counts[i] = observations <= buckets[i] (not cumulative yet)
	count       uint64
	sum         float64
}

func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if !sort.Float64sAreSorted(buckets) {
		panic("metrics: buckets must be sorted")
	}
	h := &HistogramVec{name: name, help: help, labels: labels, buckets: buckets, series: map[string]*histogram{}}
	r.register(name, h)
	return h
}

// Observe records one value, e.g. a duration in seconds.
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	if len(labelValues) != len(h.labels) {
		panic(fmt.Sprintf("metrics: %s: got %d label values, want %d", h.name, len(labelValues), len(h.labels)))
	}
	k := labelKey(labelValues)
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[k]
	if !ok {
		s = &histogram{labelValues: append([]string(nil), labelValues...), counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	// First bucket whose upper bound is >= v. Values above every bucket
	// only count toward +Inf (the total count).
	if i := sort.SearchFloat64s(h.buckets, v); i < len(h.buckets) {
		s.counts[i]++
	}
	s.count++
	s.sum += v
}

// Count returns how many values were observed (useful in tests).
func (h *HistogramVec) Count(labelValues ...string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.series[labelKey(labelValues)]; ok {
		return s.count
	}
	return 0
}

func (h *HistogramVec) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	for _, k := range sortedKeys(h.series) {
		s := h.series[k]
		var cumulative uint64
		for i, upper := range h.buckets {
			cumulative += s.counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, formatLabels(h.labels, s.labelValues, "le", formatFloat(upper)), cumulative)
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, formatLabels(h.labels, s.labelValues, "le", "+Inf"), s.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, formatLabels(h.labels, s.labelValues), formatFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, formatLabels(h.labels, s.labelValues), s.count)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
