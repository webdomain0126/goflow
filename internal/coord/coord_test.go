//go:build integration

package coord

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openTestClient(t *testing.T) *Client {
	t.Helper()
	url := os.Getenv("GOFLOW_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/15" // database 15, away from dev data
	}
	c, err := Open(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("redis not reachable (try: docker compose up -d --wait): %v", err)
	}
	c.rdb.FlushDB(ctx)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestPublishWakesSubscribers(t *testing.T) {
	c := openTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wake := c.Wakeups(ctx)
	time.Sleep(100 * time.Millisecond) // let SUBSCRIBE reach the server

	if err := c.PublishNewJob(ctx, "email"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("no wake-up received within 2s")
	}
}

func TestWakeupsCoalesce(t *testing.T) {
	c := openTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := c.Wakeups(ctx)
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		c.PublishNewJob(ctx, "email")
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(wake); n != 1 {
		t.Errorf("buffered wake-ups = %d, want 1 (extra signals are dropped)", n)
	}
}

func TestRateLimiterAcrossConcurrentCallers(t *testing.T) {
	c := openTestClient(t)
	l := c.RateLimiter(map[string]int{"sms": 5})
	ctx := context.Background()

	// 20 concurrent calls in the same second: exactly 5 may pass.
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, retryAfter, err := l.Allow(ctx, "sms")
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				allowed.Add(1)
			} else if retryAfter <= 0 || retryAfter > time.Second {
				t.Errorf("retryAfter = %s, want within (0, 1s]", retryAfter)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 5 {
		t.Errorf("allowed %d calls, want exactly 5", allowed.Load())
	}

	// Unlimited types always pass.
	if ok, _, _ := l.Allow(ctx, "email"); !ok {
		t.Error("type without a limit was denied")
	}

	// After the window resets, calls pass again.
	time.Sleep(1100 * time.Millisecond)
	if ok, _, _ := l.Allow(ctx, "sms"); !ok {
		t.Error("call denied after the window reset")
	}
}

func TestRegistryHeartbeatsAndExpiry(t *testing.T) {
	c := openTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	started := time.Now().UTC()
	done := make(chan struct{})
	go func() {
		c.RunHeartbeats(ctx, 50*time.Millisecond, func() WorkerInfo {
			return WorkerInfo{ID: "w-1", Types: []string{"echo"}, Concurrency: 4, StartedAt: started}
		})
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)

	workers, err := c.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || workers[0].ID != "w-1" || workers[0].Concurrency != 4 {
		t.Fatalf("workers = %+v", workers)
	}

	cancel() // graceful stop deletes the key
	<-done
	workers, _ = c.ListWorkers(context.Background())
	if len(workers) != 0 {
		t.Errorf("after shutdown, workers = %+v, want none", workers)
	}
}

func TestCrashedWorkerExpires(t *testing.T) {
	c := openTestClient(t)
	// Simulate a heartbeat from a worker that then died: TTL runs out.
	c.rdb.Set(context.Background(), workerKeyPrefix+"ghost", `{"id":"ghost"}`, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	workers, _ := c.ListWorkers(context.Background())
	if len(workers) != 0 {
		t.Errorf("expired heartbeat still listed: %+v", workers)
	}
}

func TestRedisDownReturnsErrorsQuickly(t *testing.T) {
	c, err := Open("redis://localhost:1/0", nil) // nothing listens on port 1
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()

	start := time.Now()
	if _, _, err := c.RateLimiter(map[string]int{"sms": 1}).Allow(ctx, "sms"); err == nil {
		t.Error("Allow with Redis down: want an error (the worker then fails open)")
	}
	if err := c.PublishNewJob(ctx, "sms"); err == nil {
		t.Error("Publish with Redis down: want an error")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("errors took %s; Redis outages must not stall the system", d)
	}
}
