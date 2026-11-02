// Package coord uses Redis to coordinate GoFlow processes:
//
//   - Notifier: the API publishes "new job" events; workers wake up
//     instantly instead of waiting for their next poll.
//   - RateLimiter: caps how many jobs of a type start per second,
//     across all workers.
//   - Registry: each worker writes a heartbeat key with a short expiry,
//     so "which workers are alive" is simply "which keys exist".
//
// Redis is an optimization here, never the source of truth. If Redis is
// down, workers fall back to polling, rate limits fail open, and the
// worker list is empty, but no job is lost or run twice.
package coord

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	newJobChannel   = "goflow:jobs:new"
	workerKeyPrefix = "goflow:workers:"
	rateKeyPrefix   = "goflow:ratelimit:"
)

// Client wraps a Redis connection.
type Client struct {
	rdb *redis.Client
	log *slog.Logger
}

// Open creates a client. It does not fail if Redis is down: go-redis
// connects lazily and reconnects on its own, so features recover by
// themselves when Redis comes back. Use Ping to check right now.
func Open(url string, log *slog.Logger) (*Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	// Short timeouts: Redis calls sit on hot paths, and a slow Redis
	// should degrade the system, not stall it.
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = time.Second
	opts.WriteTimeout = time.Second
	opts.MaxRetries = 1
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Client{rdb: redis.NewClient(opts), log: log}, nil
}

func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }
func (c *Client) Close() error                   { return c.rdb.Close() }

// ---- Notifier --------------------------------------------------------

// PublishNewJob tells subscribed workers that a job of jobType is ready.
func (c *Client) PublishNewJob(ctx context.Context, jobType string) error {
	return c.rdb.Publish(ctx, newJobChannel, jobType).Err()
}

// Wakeups subscribes to new-job events. The returned channel has a
// buffer of one and drops extra signals: a worker only needs to know
// "there is something to do", not how many times it was told.
// The subscription ends when ctx is cancelled.
func (c *Client) Wakeups(ctx context.Context) <-chan struct{} {
	out := make(chan struct{}, 1)
	sub := c.rdb.Subscribe(ctx, newJobChannel)
	go func() {
		defer sub.Close()
		msgs := sub.Channel() // go-redis reconnects and resubscribes on its own
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-msgs:
				if !ok {
					return
				}
				select {
				case out <- struct{}{}:
				default: // a wake-up is already pending
				}
			}
		}
	}()
	return out
}

// ---- RateLimiter -----------------------------------------------------

// RateLimiter allows at most Limits[type] job starts per second for each
// type, counted across every worker. Types without a limit are unlimited.
type RateLimiter struct {
	c      *Client
	limits map[string]int
}

func (c *Client) RateLimiter(limits map[string]int) *RateLimiter {
	return &RateLimiter{c: c, limits: limits}
}

// The script runs atomically inside Redis, so two workers can never both
// see "49" and both start job number 50.
// It returns {count in the current window, milliseconds until it resets}.
var rateScript = redis.NewScript(`
local count = redis.call("INCR", KEYS[1])
if count == 1 then
	redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return {count, redis.call("PTTL", KEYS[1])}
`)

// Allow reports whether a job of this type may start now. If not,
// retryAfter says when the current one-second window ends.
func (l *RateLimiter) Allow(ctx context.Context, jobType string) (bool, time.Duration, error) {
	limit, ok := l.limits[jobType]
	if !ok || limit <= 0 {
		return true, 0, nil
	}
	res, err := rateScript.Run(ctx, l.c.rdb, []string{rateKeyPrefix + jobType}, 1000).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("rate limit %s: %w", jobType, err)
	}
	count, ttlMillis := res[0], res[1]
	if count <= int64(limit) {
		return true, 0, nil
	}
	retryAfter := time.Duration(ttlMillis) * time.Millisecond
	if retryAfter < 10*time.Millisecond {
		retryAfter = 10 * time.Millisecond
	}
	return false, retryAfter, nil
}

// ---- Registry --------------------------------------------------------

// WorkerInfo is what a worker reports about itself in its heartbeat.
type WorkerInfo struct {
	ID          string    `json:"id"`
	Types       []string  `json:"types"`
	Concurrency int       `json:"concurrency"`
	Active      int       `json:"active"`
	StartedAt   time.Time `json:"started_at"`
	LastSeen    time.Time `json:"last_seen"`
}

// RunHeartbeats publishes info() every interval until ctx is cancelled,
// then deletes the key so the worker disappears from the list at once.
// The key expires after 3 intervals, so a crashed worker disappears too.
func (c *Client) RunHeartbeats(ctx context.Context, interval time.Duration, info func() WorkerInfo) {
	ttl := 3 * interval
	beat := func() {
		wi := info()
		wi.LastSeen = time.Now().UTC()
		data, _ := json.Marshal(wi) // cannot fail for this struct
		if err := c.rdb.Set(ctx, workerKeyPrefix+wi.ID, data, ttl).Err(); err != nil && ctx.Err() == nil {
			c.log.Warn("worker heartbeat to redis failed", "err", err)
		}
	}
	beat()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			delCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			c.rdb.Del(delCtx, workerKeyPrefix+info().ID)
			cancel()
			return
		case <-t.C:
			beat()
		}
	}
}

// ListWorkers returns every worker with a live heartbeat, sorted by ID.
func (c *Client) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	var keys []string
	iter := c.rdb.Scan(ctx, 0, workerKeyPrefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	workers := []WorkerInfo{}
	if len(keys) == 0 {
		return workers, nil
	}
	values, err := c.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			continue // key expired between SCAN and MGET
		}
		var wi WorkerInfo
		if err := json.Unmarshal([]byte(s), &wi); err == nil {
			workers = append(workers, wi)
		}
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers, nil
}
