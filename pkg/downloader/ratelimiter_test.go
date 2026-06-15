package downloader

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRateLimiterUnlimited(t *testing.T) {
	rl := NewRateLimiter(0)

	start := time.Now()
	err := rl.Wait(context.Background(), 1024*1024) // 1 MB
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("unlimited mode took too long: %v", elapsed)
	}
}

func TestRateLimiterThrottles(t *testing.T) {
	// 10,000 bytes/sec limit. Request 10,000 bytes (the full burst),
	// then request another 5,000 which must wait ~500ms.
	const rate = 10_000
	rl := NewRateLimiter(rate)

	ctx := context.Background()

	// Drain the initial burst.
	if err := rl.Wait(ctx, rate); err != nil {
		t.Fatalf("initial burst wait failed: %v", err)
	}

	// Now the bucket is empty; requesting 5,000 more should take ~500ms.
	start := time.Now()
	if err := rl.Wait(ctx, rate/2); err != nil {
		t.Fatalf("throttled wait failed: %v", err)
	}
	elapsed := time.Since(start)

	// Allow generous tolerance for CI/slow machines: 300ms - 900ms.
	if elapsed < 300*time.Millisecond {
		t.Fatalf("expected throttling (~500ms), but only waited %v", elapsed)
	}
	if elapsed > 900*time.Millisecond {
		t.Fatalf("throttling took too long: %v (expected ~500ms)", elapsed)
	}
}

func TestRateLimiterSetLimit(t *testing.T) {
	rl := NewRateLimiter(100)
	if got := rl.Limit(); got != 100 {
		t.Fatalf("expected limit 100, got %d", got)
	}

	rl.SetLimit(200)
	if got := rl.Limit(); got != 200 {
		t.Fatalf("expected limit 200 after SetLimit, got %d", got)
	}

	// Setting to 0 should make it unlimited.
	rl.SetLimit(0)
	if got := rl.Limit(); got != 0 {
		t.Fatalf("expected limit 0 (unlimited), got %d", got)
	}

	start := time.Now()
	err := rl.Wait(context.Background(), 999_999)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected nil error after SetLimit(0), got %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("expected immediate return after SetLimit(0), took %v", elapsed)
	}
}

func TestRateLimiterNegativeLimitsBecomeUnlimited(t *testing.T) {
	rl := NewRateLimiter(-100)
	if got := rl.Limit(); got != 0 {
		t.Fatalf("expected negative initial limit to clamp to 0, got %d", got)
	}

	rl.SetLimit(100)
	rl.SetLimit(-1)
	if got := rl.Limit(); got != 0 {
		t.Fatalf("expected negative SetLimit to clamp to 0, got %d", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := rl.Wait(ctx, 1_000_000); err != nil {
		t.Fatalf("expected clamped unlimited limiter to return immediately, got %v", err)
	}
}

func TestRateLimiterSetLimitDynamic(t *testing.T) {
	// Start with a very low limit, change to unlimited mid-wait.
	const slowRate = 100 // 100 bytes/sec
	rl := NewRateLimiter(slowRate)

	ctx := context.Background()

	// Drain the burst.
	if err := rl.Wait(ctx, slowRate); err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	// Start a goroutine that will switch to unlimited after 200ms.
	go func() {
		time.Sleep(200 * time.Millisecond)
		rl.SetLimit(0)
	}()

	start := time.Now()
	err := rl.Wait(ctx, 50_000)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	// Should complete shortly after the 200ms SetLimit(0) call,
	// not the ~500 seconds it would take at 100 bytes/sec.
	if elapsed > 1*time.Second {
		t.Fatalf("dynamic SetLimit(0) didn't take effect fast enough: %v", elapsed)
	}
}

func TestRateLimiterContextCancellation(t *testing.T) {
	rl := NewRateLimiter(100) // Very slow

	// Drain burst.
	if err := rl.Wait(context.Background(), 100); err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := rl.Wait(ctx, 999_999) // Would take ~10,000 seconds normally
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("cancellation took too long: %v", elapsed)
	}
}

func TestRateLimiterContextAlreadyCancelled(t *testing.T) {
	rl := NewRateLimiter(100)

	// Drain burst.
	if err := rl.Wait(context.Background(), 100); err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	err := rl.Wait(ctx, 50)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRateLimiterConcurrentAccess(t *testing.T) {
	rl := NewRateLimiter(100_000)
	ctx := context.Background()

	var wg sync.WaitGroup
	const goroutines = 20

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = rl.Wait(ctx, 100)
			}
		}()
	}

	// Concurrently call SetLimit and Limit.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			rl.SetLimit(int64(50_000 + j*100))
			_ = rl.Limit()
		}
	}()

	wg.Wait()
	// Test passes if no race detector complaints.
}

// TestRateLimiterUnlimitedHonorsCancelledContext verifies that the lock-free
// unlimited fast path still reports a cancelled context (preserving the old
// semantics where Wait checked the context before returning).
func TestRateLimiterUnlimitedHonorsCancelledContext(t *testing.T) {
	rl := NewRateLimiter(0) // unlimited

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	if err := rl.Wait(ctx, 1024); err != context.Canceled {
		t.Fatalf("expected context.Canceled on the unlimited fast path, got %v", err)
	}

	// With a live context the unlimited fast path returns nil immediately.
	if err := rl.Wait(context.Background(), 1<<20); err != nil {
		t.Fatalf("expected nil on live-context unlimited wait, got %v", err)
	}
}

// TestRateLimiterUnlimitedConcurrentLockFree hammers the unlimited path from
// many goroutines while the limit is toggled. The point is that unlimited Wait
// no longer serializes on the limiter mutex; correctness is checked by -race
// and by the calls all returning promptly.
func TestRateLimiterUnlimitedConcurrentLockFree(t *testing.T) {
	rl := NewRateLimiter(0) // unlimited
	ctx := context.Background()

	var wg sync.WaitGroup
	const goroutines = 32
	const iters = 1000

	start := time.Now()
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				if err := rl.Wait(ctx, 16384); err != nil {
					t.Errorf("unexpected error on unlimited wait: %v", err)
					return
				}
			}
		}()
	}

	// Concurrently flip the limit to exercise the atomic store vs. the
	// lock-free load in Wait.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			rl.SetLimit(0)
			_ = rl.Limit()
		}
	}()

	wg.Wait()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("unlimited concurrent waits took too long (%v); fast path may be locking", elapsed)
	}
}

func TestRateLimiterZeroBytes(t *testing.T) {
	rl := NewRateLimiter(100)

	err := rl.Wait(context.Background(), 0)
	if err != nil {
		t.Fatalf("expected nil for 0-byte wait, got %v", err)
	}

	err = rl.Wait(context.Background(), -5)
	if err != nil {
		t.Fatalf("expected nil for negative-byte wait, got %v", err)
	}
}

func TestRateLimiterNewLimiterState(t *testing.T) {
	rl := NewRateLimiter(1024)

	if got := rl.Limit(); got != 1024 {
		t.Fatalf("expected initial limit 1024, got %d", got)
	}

	// Should be able to immediately consume up to the burst size.
	start := time.Now()
	err := rl.Wait(context.Background(), 1024)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("initial burst should be instant, took %v", elapsed)
	}
}
