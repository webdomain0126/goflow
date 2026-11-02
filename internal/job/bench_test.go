package job

import (
	"encoding/json"
	"testing"
	"time"
)

func BenchmarkNormalize(b *testing.B) {
	p := CreateParams{Type: "send_email", Payload: json.RawMessage(`{"to":"a@example.com","subject":"Hi","body":"Hello there"}`), Priority: 5}
	now := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := p.Normalize(now); err != nil {
			b.Fatal(err)
		}
	}
}
