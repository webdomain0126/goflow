package backoff

import "testing"

func BenchmarkDelay(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Default.Delay(i%20 + 1)
	}
}
