// Package api is GoFlow's REST API, built on the standard library's
// net/http (Go 1.22+ routing supports "GET /v1/jobs/{id}" patterns, so
// no router library is needed).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/webdomain0126/goflow/internal/coord"
	"github.com/webdomain0126/goflow/internal/job"
	"github.com/webdomain0126/goflow/internal/metrics"
)

// Store is the part of the database the API uses.
type Store interface {
	Create(ctx context.Context, p job.CreateParams) (job.Job, bool, error)
	Get(ctx context.Context, id int64) (job.Job, error)
	List(ctx context.Context, f job.ListFilter) ([]job.Job, error)
	Cancel(ctx context.Context, id int64) (job.Job, error)
	Retry(ctx context.Context, id int64) (job.Job, error)
	CountByStatus(ctx context.Context) (map[job.Status]int64, error)
	Ping(ctx context.Context) error
}

// Coordinator is the optional Redis side: wake-ups and the worker list.
type Coordinator interface {
	PublishNewJob(ctx context.Context, jobType string) error
	ListWorkers(ctx context.Context) ([]coord.WorkerInfo, error)
}

const maxBodyBytes = 1 << 20 // 1 MiB

// Server holds the API's dependencies.
type Server struct {
	store   Store
	coord   Coordinator // may be nil
	log     *slog.Logger
	metrics *metrics.Registry
	now     func() time.Time

	httpRequests *metrics.CounterVec
	httpDuration *metrics.HistogramVec
}

// New builds the API. coord may be nil (Redis disabled).
func New(store Store, c Coordinator, log *slog.Logger, reg *metrics.Registry) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if reg == nil {
		reg = metrics.NewRegistry()
	}
	s := &Server{store: store, coord: c, log: log, metrics: reg, now: time.Now}
	s.httpRequests = reg.NewCounterVec("goflow_http_requests_total", "HTTP requests by route and status code.", "method", "route", "code")
	s.httpDuration = reg.NewHistogramVec("goflow_http_request_duration_seconds", "HTTP request latency.", metrics.DefaultBuckets, "route")
	reg.NewGaugeFunc("goflow_jobs", "Jobs in the database by status.", s.jobCounts, "status")
	return s
}

// Handler returns the full HTTP handler with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", s.createJob)
	mux.HandleFunc("GET /v1/jobs", s.listJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("POST /v1/jobs/{id}/retry", s.retryJob)
	mux.HandleFunc("GET /v1/stats", s.stats)
	mux.HandleFunc("GET /v1/workers", s.workers)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", s.metrics.Handler())

	// Order matters: the first middleware listed is the outermost.
	return chain(mux, s.requestID, s.logAndMeasure, s.recoverPanics)
}

// ---- handlers --------------------------------------------------------

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var p job.CreateParams
	if !decodeJSON(w, r, &p) {
		return
	}
	// The key may come from the standard header or the body, not two different values.
	if h := strings.TrimSpace(r.Header.Get("Idempotency-Key")); h != "" {
		if p.IdempotencyKey != "" && p.IdempotencyKey != h {
			writeError(w, http.StatusBadRequest, "bad_request", "Idempotency-Key header and body idempotency_key differ", "idempotency_key")
			return
		}
		p.IdempotencyKey = h
	}

	j, created, err := s.store.Create(r.Context(), p)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	if !created {
		// Same key seen before: return the original job, do not create a second one.
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, j)
		return
	}
	if !j.RunAt.After(s.now()) {
		s.wakeWorkers(r.Context(), j.Type)
	}
	w.Header().Set("Location", fmt.Sprintf("/v1/jobs/%d", j.ID))
	writeJSON(w, http.StatusCreated, j)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	j, err := s.store.Get(r.Context(), id)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

type listResponse struct {
	Jobs       []job.Job `json:"jobs"`
	NextCursor *string   `json:"next_cursor"` // pass as ?before= to get the next page; null on the last page
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := job.ListFilter{Type: q.Get("type"), Limit: 20}

	if st := q.Get("status"); st != "" {
		f.Status = job.Status(st)
		if !f.Status.Valid() {
			writeError(w, http.StatusBadRequest, "validation_failed", "unknown status "+strconv.Quote(st), "status")
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "validation_failed", "limit must be a number from 1 to 100", "limit")
			return
		}
		f.Limit = n
	}
	if v := q.Get("before"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "validation_failed", "before must be a positive job id", "before")
			return
		}
		f.BeforeID = n
	}

	jobs, err := s.store.List(r.Context(), f)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	resp := listResponse{Jobs: jobs}
	if len(jobs) == f.Limit {
		next := strconv.FormatInt(jobs[len(jobs)-1].ID, 10)
		resp.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	j, err := s.store.Cancel(r.Context(), id)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	j, err := s.store.Retry(r.Context(), id)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	s.wakeWorkers(r.Context(), j.Type)
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.store.CountByStatus(r.Context())
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts, "total": total})
}

func (s *Server) workers(w http.ResponseWriter, r *http.Request) {
	if s.coord == nil {
		writeError(w, http.StatusServiceUnavailable, "redis_disabled", "worker registry needs Redis", "")
		return
	}
	list, err := s.coord.ListWorkers(r.Context())
	if err != nil {
		s.log.Warn("list workers failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "redis_unavailable", "worker registry is temporarily unavailable", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": list})
}

// healthz (liveness): the process is up. It never checks dependencies,
// so a database outage does not make Kubernetes restart healthy pods.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz (readiness): can this instance serve traffic right now?
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.log.Warn("readiness check failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "database": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "database": "up"})
}

// ---- helpers ---------------------------------------------------------

// wakeWorkers is best effort: if Redis is down, workers still find the
// job on their next poll, so a failure is only logged.
func (s *Server) wakeWorkers(ctx context.Context, jobType string) {
	if s.coord == nil {
		return
	}
	if err := s.coord.PublishNewJob(ctx, jobType); err != nil {
		s.log.Warn("publish wake-up failed; workers will find the job by polling", "err", err)
	}
}

func (s *Server) jobCounts() map[string]float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := s.store.CountByStatus(ctx)
	out := map[string]float64{}
	if err != nil {
		s.log.Warn("count jobs for metrics failed", "err", err)
		return out
	}
	for st, n := range counts {
		out[string(st)] = float64(n)
	}
	return out
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "bad_request", "job id must be a positive integer", "id")
		return 0, false
	}
	return id, true
}

// decodeJSON reads a JSON body strictly: unknown fields, trailing data
// and oversized bodies are all rejected with a clear 4xx error.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &tooBig):
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("request body must be at most %d bytes", maxBodyBytes), "")
		case errors.As(err, &typeErr):
			writeError(w, http.StatusBadRequest, "validation_failed", fmt.Sprintf("%s must be of type %s", typeErr.Field, typeErr.Type), typeErr.Field)
		default:
			writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error(), "")
		}
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "bad_request", "request body must contain a single JSON object", "")
		return false
	}
	return true
}

// handleError maps domain errors to HTTP responses. Unknown errors become
// a generic 500: the details go to the log, never to the client.
func (s *Server) handleError(w http.ResponseWriter, r *http.Request, err error) {
	var ve *job.ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusBadRequest, "validation_failed", ve.Message, ve.Field)
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "job not found", "")
	case errors.Is(err, job.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "invalid_state", err.Error(), "")
	case errors.Is(err, context.Canceled):
		// The client went away; nobody will read a response.
	default:
		s.log.Error("request failed", "err", err, "request_id", requestIDFrom(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal", "internal server error", "")
	}
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, msg, field string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg, Field: field}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v) // the status line is already sent; nothing useful to do on error
}
