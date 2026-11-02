package api

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// BenchmarkCreateJobHandler measures the HTTP layer alone (routing,
// middleware, JSON decode/encode, validation) with an in-memory store.
func BenchmarkCreateJobHandler(b *testing.B) {
	h := New(newFakeStore(), nil, nil, nil).Handler()
	body := `{"type":"send_email","payload":{"to":"a@example.com"},"priority":5}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/jobs", strings.NewReader(body)))
		if rec.Code != 201 {
			b.Fatalf("status %d", rec.Code)
		}
	}
}
