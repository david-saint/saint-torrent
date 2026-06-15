package downloader

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiter provides token-bucket rate limiting for bandwidth control.
// Tokens accumulate at the configured bytes-per-second rate, capped at a
// burst size equal to the rate (i.e. 1 second worth of tokens). A limit
// of 0 disables rate limiting entirely: Wait always returns immediately.
//
// limit is an atomic so the unlimited fast path in Wait never touches the
// mutex. This matters because a single global limiter is shared by every peer
// of every session: with the old design each in-flight block took the mutex
// even when no limit was configured, serializing the whole swarm on one lock.
// The token-bucket state (tokens/maxTokens/lastRefill) is still mutex-guarded;
// SetLimit holds the mutex while it updates both limit and maxTokens so the
// slow path always reads a consistent pair.
type RateLimiter struct {
	mu         sync.Mutex
	limit      atomic.Int64 // bytes per second; 0 = unlimited
	tokens     float64      // current available tokens
	maxTokens  float64      // burst cap (== limit)
	lastRefill time.Time    // last time tokens were refilled
}

// NewRateLimiter creates a rate limiter with the given bytes-per-second limit.
// A limit of 0 means unlimited (Wait always returns immediately).
func NewRateLimiter(bytesPerSec int64) *RateLimiter {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	r := &RateLimiter{
		tokens:     float64(bytesPerSec),
		maxTokens:  float64(bytesPerSec),
		lastRefill: time.Now(),
	}
	r.limit.Store(bytesPerSec)
	return r
}

// SetLimit changes the rate limit dynamically. 0 = unlimited.
// Existing accumulated tokens are capped to the new burst size.
func (r *RateLimiter) SetLimit(bytesPerSec int64) {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.limit.Store(bytesPerSec)
	r.maxTokens = float64(bytesPerSec)

	// Refill before capping so we don't lose accrued tokens unnecessarily.
	r.refillLocked()

	if r.tokens > r.maxTokens {
		r.tokens = r.maxTokens
	}
}

// Limit returns the current limit in bytes per second. 0 = unlimited.
func (r *RateLimiter) Limit() int64 {
	return r.limit.Load()
}

// Wait blocks until n bytes of bandwidth are available, or ctx is cancelled.
// Returns nil on success, ctx.Err() on cancellation.
//
// When the limit is 0 (unlimited), Wait returns immediately without error.
// For positive limits the method sleeps in a loop, refilling the token bucket
// each iteration, until enough tokens have accumulated. The sleep granularity
// is capped at 10 ms so the limiter remains responsive to context cancellation
// and dynamic limit changes.
func (r *RateLimiter) Wait(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}

	// Lock-free fast path: unlimited mode lets everything through without ever
	// touching the mutex, so the shared global limiter is not a contention point
	// when no limit is configured. Context is still honored to preserve the
	// original semantics (a cancelled ctx returns its error even when unlimited).
	if r.limit.Load() == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	for {
		// Check context first.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		r.mu.Lock()

		// Re-read under the lock so limit and maxTokens are a consistent pair
		// (SetLimit updates both while holding the mutex). A late switch to
		// unlimited still short-circuits here.
		limit := r.limit.Load()
		if limit == 0 {
			r.mu.Unlock()
			return nil
		}

		r.refillLocked()

		needed := float64(n)
		if r.tokens >= needed {
			r.tokens -= needed
			r.mu.Unlock()
			return nil
		}

		// Calculate how long we need to wait for enough tokens.
		// Cap at 100ms so we re-check frequently for dynamic limit changes.
		deficit := needed - r.tokens
		waitDur := time.Duration(deficit / float64(limit) * float64(time.Second))
		if waitDur < 10*time.Millisecond {
			waitDur = 10 * time.Millisecond
		}
		if waitDur > 100*time.Millisecond {
			waitDur = 100 * time.Millisecond
		}

		r.mu.Unlock()

		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Loop back to refill and re-check.
		}
	}
}

// refillLocked adds tokens based on elapsed time since the last refill.
// Must be called with r.mu held.
func (r *RateLimiter) refillLocked() {
	now := time.Now()
	elapsed := now.Sub(r.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	r.lastRefill = now
	r.tokens += elapsed * float64(r.limit.Load())
	if r.tokens > r.maxTokens {
		r.tokens = r.maxTokens
	}
}
