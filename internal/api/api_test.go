package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/webdomain0126/goflow/internal/coord"
	"github.com/webdomain0126/goflow/internal/job"
	"github.com/webdomain0126/goflow/internal/metrics"
)

// fakeStore is an in-memory Store for handler tests.
type fakeStore struct {
	mu      sync.Mutex
	jobs    []job.Job // index = id-1
	keys    map[string]int64
	pingErr error
	failAll error // if set, every call returns it
}

func newFakeStore() *fakeStore { return &fakeStore{keys: map[string]int64{}} }

func (f *fakeStore) Create(_ context.Context, p job.CreateParams) (job.Job, bool, error) {
	if f.failAll != nil {
		return job.Job{}, false, f.failAll
	}
	p, err := p.Normalize(time.Now())
	if err != nil {
		return job.Job{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.keys[p.IdempotencyKey]; ok && p.IdempotencyKey != "" {
		return f.jobs[id-1], false, nil
	}
	j := job.Job{ID: int64(len(f.jobs) + 1), Type: p.Type, Payload: p.Payload, Priority: p.Priority,
		Status: job.StatusPending, MaxAttempts: p.MaxAttempts, RunAt: *p.RunAt}
	f.jobs = append(f.jobs, j)
	if p.IdempotencyKey != "" {
		f.keys[p.IdempotencyKey] = j.ID
	}
	return j, true, nil
}

func (f *fakeStore) Get(_ context.Context, id int64) (job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id < 1 || id > int64(len(f.jobs)) {
		return job.Job{}, job.ErrNotFound
	}
	return f.jobs[id-1], nil
}

func (f *fakeStore) List(_ context.Context, flt job.ListFilter) ([]job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []job.Job{}
	for i := len(f.jobs) - 1; i >= 0 && len(out) < flt.Limit; i-- {
		j := f.jobs[i]
		if (flt.BeforeID == 0 || j.ID < flt.BeforeID) && (flt.Status == "" || j.Status == flt.Status) && (flt.Type == "" || j.Type == flt.Type) {
			out = append(out, j)
		}
	}
	return out, nil
}

func (f *fakeStore) setStatus(id int64, from []job.Status, to job.Status) (job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id < 1 || id > int64(len(f.jobs)) {
		return job.Job{}, job.ErrNotFound
	}
	j := &f.jobs[id-1]
	for _, s := range from {
		if j.Status == s {
			j.Status = to
			return *j, nil
		}
	}
	return *j, fmt.Errorf("%w: job %d is %s", job.ErrInvalidTransition, id, j.Status)
}

func (f *fakeStore) Cancel(_ context.Context, id int64) (job.Job, error) {
	return f.setStatus(id, []job.Status{job.StatusPending}, job.StatusCancelled)
}

func (f *fakeStore) Retry(_ context.Context, id int64) (job.Job, error) {
	return f.setStatus(id, []job.Status{job.StatusDead, job.StatusCancelled}, job.StatusPending)
}

func (f *fakeStore) CountByStatus(context.Context) (map[job.Status]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := map[job.Status]int64{}
	for _, j := range f.jobs {
		c[j.Status]++
	}
	return c, nil
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

type fakeCoord struct {
	mu        sync.Mutex
	published []string
}

func (c *fakeCoord) PublishNewJob(_ context.Context, t string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.published = append(c.published, t)
	return nil
}

func (c *fakeCoord) ListWorkers(context.Context) ([]coord.WorkerInfo, error) {
	return []coord.WorkerInfo{{ID: "w-1", Concurrency: 4}}, nil
}

// ---- helpers ---------------------------------------------------------

type testAPI struct {
	srv   *httptest.Server
	store *fakeStore
	coord *fakeCoord
	reg   *metrics.Registry
}

func newTestAPI(t *testing.T) *testAPI {
	t.Helper()
	ta := &testAPI{store: newFakeStore(), coord: &fakeCoord{}, reg: metrics.NewRegistry()}
	ta.srv = httptest.NewServer(New(ta.store, ta.coord, nil, ta.reg).Handler())
	t.Cleanup(ta.srv.Close)
	return ta
}

func (ta *testAPI) do(t *testing.T, method, path, body string, headers ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, ta.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

func decode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	return v
}

func expectStatus(t *testing.T, resp *http.Response, body []byte, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, want, body)
	}
}

// ---- tests -----------------------------------------------------------

func TestCreateJob(t *testing.T) {
	ta := newTestAPI(t)
	resp, body := ta.do(t, "POST", "/v1/jobs", `{"type":"send_email","payload":{"to":"a@b.c"},"priority":5}`)
	expectStatus(t, resp, body, http.StatusCreated)

	j := decode[job.Job](t, body)
	if j.ID != 1 || j.Type != "send_email" || j.Status != job.StatusPending || j.Priority != 5 {
		t.Errorf("created job = %+v", j)
	}
	if loc := resp.Header.Get("Location"); loc != "/v1/jobs/1" {
		t.Errorf("Location = %q", loc)
	}
	if len(ta.coord.published) != 1 || ta.coord.published[0] != "send_email" {
		t.Errorf("published wake-ups = %v, want [send_email]", ta.coord.published)
	}
}

func TestCreateScheduledJobDoesNotWakeWorkers(t *testing.T) {
	ta := newTestAPI(t)
	runAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	resp, body := ta.do(t, "POST", "/v1/jobs", `{"type":"report","run_at":"`+runAt+`"}`)
	expectStatus(t, resp, body, http.StatusCreated)
	if len(ta.coord.published) != 0 {
		t.Errorf("a job scheduled for later woke workers: %v", ta.coord.published)
	}
}

func TestCreateJobValidation(t *testing.T) {
	ta := newTestAPI(t)
	tests := []struct {
		name, body string
		wantStatus int
		wantCode   string
		wantField  string
	}{
		{"bad type", `{"type":"Bad Type"}`, 400, "validation_failed", "type"},
		{"priority out of range", `{"type":"a","priority":500}`, 400, "validation_failed", "priority"},
		{"wrong field type", `{"type":"a","priority":"high"}`, 400, "validation_failed", "priority"},
		{"unknown field", `{"type":"a","colour":"red"}`, 400, "bad_request", ""},
		{"not json", `hello`, 400, "bad_request", ""},
		{"two objects", `{"type":"a"}{"type":"b"}`, 400, "bad_request", ""},
		{"too large", `{"type":"a","payload":"` + strings.Repeat("x", maxBodyBytes) + `"}`, 413, "body_too_large", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := ta.do(t, "POST", "/v1/jobs", tt.body)
			expectStatus(t, resp, body, tt.wantStatus)
			e := decode[errorBody](t, body)
			if e.Error.Code != tt.wantCode || e.Error.Field != tt.wantField {
				t.Errorf("error = %+v, want code %q field %q", e.Error, tt.wantCode, tt.wantField)
			}
		})
	}
}

func TestIdempotencyKeyHeader(t *testing.T) {
	ta := newTestAPI(t)
	r1, b1 := ta.do(t, "POST", "/v1/jobs", `{"type":"charge"}`, "Idempotency-Key", "order-7")
	expectStatus(t, r1, b1, http.StatusCreated)
	r2, b2 := ta.do(t, "POST", "/v1/jobs", `{"type":"charge"}`, "Idempotency-Key", "order-7")
	expectStatus(t, r2, b2, http.StatusOK)

	if decode[job.Job](t, b1).ID != decode[job.Job](t, b2).ID {
		t.Error("same idempotency key returned different jobs")
	}
	if r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Error("replayed response is missing the Idempotent-Replayed header")
	}

	r3, b3 := ta.do(t, "POST", "/v1/jobs", `{"type":"charge","idempotency_key":"other"}`, "Idempotency-Key", "order-7")
	expectStatus(t, r3, b3, http.StatusBadRequest)
}

func TestGetJob(t *testing.T) {
	ta := newTestAPI(t)
	ta.do(t, "POST", "/v1/jobs", `{"type":"a"}`)

	resp, body := ta.do(t, "GET", "/v1/jobs/1", "")
	expectStatus(t, resp, body, http.StatusOK)

	resp, body = ta.do(t, "GET", "/v1/jobs/42", "")
	expectStatus(t, resp, body, http.StatusNotFound)
	if decode[errorBody](t, body).Error.Code != "not_found" {
		t.Errorf("body = %s", body)
	}

	resp, body = ta.do(t, "GET", "/v1/jobs/abc", "")
	expectStatus(t, resp, body, http.StatusBadRequest)
}

func TestListJobsPagination(t *testing.T) {
	ta := newTestAPI(t)
	for i := 0; i < 5; i++ {
		ta.do(t, "POST", "/v1/jobs", `{"type":"a"}`)
	}

	resp, body := ta.do(t, "GET", "/v1/jobs?limit=2", "")
	expectStatus(t, resp, body, http.StatusOK)
	page := decode[listResponse](t, body)
	if len(page.Jobs) != 2 || page.Jobs[0].ID != 5 || page.NextCursor == nil || *page.NextCursor != "4" {
		t.Fatalf("page 1 = %+v", page)
	}

	var seen []int64
	cursor := ""
	for {
		_, body := ta.do(t, "GET", "/v1/jobs?limit=2"+cursor, "")
		p := decode[listResponse](t, body)
		for _, j := range p.Jobs {
			seen = append(seen, j.ID)
		}
		if p.NextCursor == nil {
			break
		}
		cursor = "&before=" + *p.NextCursor
	}
	if fmt.Sprint(seen) != "[5 4 3 2 1]" {
		t.Errorf("walking all pages gave %v, want [5 4 3 2 1]", seen)
	}
}

func TestListJobsBadQuery(t *testing.T) {
	ta := newTestAPI(t)
	for _, q := range []string{"status=done", "limit=0", "limit=101", "limit=x", "before=-1"} {
		resp, body := ta.do(t, "GET", "/v1/jobs?"+q, "")
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("?%s: status %d, want 400 (%s)", q, resp.StatusCode, body)
		}
	}
}

func TestCancelAndRetry(t *testing.T) {
	ta := newTestAPI(t)
	ta.do(t, "POST", "/v1/jobs", `{"type":"a"}`)

	resp, body := ta.do(t, "POST", "/v1/jobs/1/cancel", "")
	expectStatus(t, resp, body, http.StatusOK)
	if decode[job.Job](t, body).Status != job.StatusCancelled {
		t.Errorf("after cancel: %s", body)
	}

	resp, body = ta.do(t, "POST", "/v1/jobs/1/cancel", "")
	expectStatus(t, resp, body, http.StatusConflict)

	resp, body = ta.do(t, "POST", "/v1/jobs/1/retry", "")
	expectStatus(t, resp, body, http.StatusOK)
	if decode[job.Job](t, body).Status != job.StatusPending {
		t.Errorf("after retry: %s", body)
	}

	resp, body = ta.do(t, "POST", "/v1/jobs/99/retry", "")
	expectStatus(t, resp, body, http.StatusNotFound)
}

func TestStatsAndWorkers(t *testing.T) {
	ta := newTestAPI(t)
	ta.do(t, "POST", "/v1/jobs", `{"type":"a"}`)
	ta.do(t, "POST", "/v1/jobs", `{"type":"a"}`)

	resp, body := ta.do(t, "GET", "/v1/stats", "")
	expectStatus(t, resp, body, http.StatusOK)
	stats := decode[struct {
		Counts map[string]int64 `json:"counts"`
		Total  int64            `json:"total"`
	}](t, body)
	if stats.Total != 2 || stats.Counts["pending"] != 2 {
		t.Errorf("stats = %+v", stats)
	}

	resp, body = ta.do(t, "GET", "/v1/workers", "")
	expectStatus(t, resp, body, http.StatusOK)
	if !strings.Contains(string(body), `"w-1"`) {
		t.Errorf("workers = %s", body)
	}
}

func TestWorkersWithoutRedis(t *testing.T) {
	srv := httptest.NewServer(New(newFakeStore(), nil, nil, nil).Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/workers")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestHealthAndReadiness(t *testing.T) {
	ta := newTestAPI(t)
	resp, body := ta.do(t, "GET", "/healthz", "")
	expectStatus(t, resp, body, http.StatusOK)
	resp, body = ta.do(t, "GET", "/readyz", "")
	expectStatus(t, resp, body, http.StatusOK)

	ta.store.pingErr = errors.New("connection refused")
	resp, body = ta.do(t, "GET", "/readyz", "")
	expectStatus(t, resp, body, http.StatusServiceUnavailable)
	resp, body = ta.do(t, "GET", "/healthz", "")
	expectStatus(t, resp, body, http.StatusOK) // liveness ignores the database
}

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	ta := newTestAPI(t)
	ta.store.failAll = errors.New("pq: password authentication failed for user goflow")
	resp, body := ta.do(t, "POST", "/v1/jobs", `{"type":"a"}`)
	expectStatus(t, resp, body, http.StatusInternalServerError)
	if strings.Contains(string(body), "password") {
		t.Errorf("internal error details leaked to the client: %s", body)
	}
}

func TestRequestIDAndPanicRecovery(t *testing.T) {
	s := New(newFakeStore(), nil, nil, nil)
	h := chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }),
		s.requestID, s.logAndMeasure, s.recoverPanics)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Request-ID", "abc-123")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
		t.Errorf("X-Request-ID = %q, want the caller's ID echoed back", got)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Request-ID", "<script>")
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got == "<script>" || len(got) != 16 {
		t.Errorf("unsafe request ID was not replaced: %q", got)
	}
}

func TestMetricsUseRoutePatterns(t *testing.T) {
	ta := newTestAPI(t)
	ta.do(t, "POST", "/v1/jobs", `{"type":"a"}`)
	ta.do(t, "GET", "/v1/jobs/1", "")
	ta.do(t, "GET", "/v1/jobs/2", "")

	_, body := ta.do(t, "GET", "/metrics", "")
	out := string(body)
	for _, want := range []string{
		`goflow_http_requests_total{method="GET",route="GET /v1/jobs/{id}",code="200"} 1`,
		`goflow_http_requests_total{method="GET",route="GET /v1/jobs/{id}",code="404"} 1`,
		`goflow_jobs{status="pending"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n%s", want, out)
		}
	}
}
