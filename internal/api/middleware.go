package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"time"
)

type middleware func(http.Handler) http.Handler

// chain wraps h so that mws[0] runs first (outermost).
func chain(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type ctxKey int

const requestIDKey ctxKey = 0

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// requestID gives every request an ID, reusing the caller's X-Request-ID
// when it is safe, so one request can be traced across services and logs.
func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if !validRequestID.MatchString(id) {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

// statusRecorder remembers the status code a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// logAndMeasure writes one log line and updates metrics per request.
func (s *Server) logAndMeasure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		d := time.Since(start)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}

		// r.Pattern is the matched route ("GET /v1/jobs/{id}"), set by
		// ServeMux. Using it instead of the raw path keeps metric labels
		// few: /v1/jobs/1 and /v1/jobs/2 count as the same route.
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		s.httpRequests.Inc(r.Method, route, strconv.Itoa(rec.status))
		s.httpDuration.Observe(d.Seconds(), route)

		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" {
			return // scraped constantly; would drown out useful logs
		}
		s.log.Info("http request",
			"method", r.Method, "path", r.URL.Path, "route", route,
			"status", rec.status, "duration_ms", d.Milliseconds(),
			"request_id", requestIDFrom(r.Context()))
	})
}

// recoverPanics turns a panicking handler into a 500 instead of a
// dropped connection, and logs the stack trace.
func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v) // the standard way to abort a response; let net/http handle it
				}
				s.log.Error("handler panicked", "panic", v, "stack", string(debug.Stack()),
					"request_id", requestIDFrom(r.Context()))
				writeError(w, http.StatusInternalServerError, "internal", "internal server error", "")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
