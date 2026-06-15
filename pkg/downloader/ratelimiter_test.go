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
	// rate bytes/sec limit. Request the full burst, then request another half-burst
	// which must wait ~500ms. The rate is kept above BlockSize so burst == rate (a
	// limit below one block floors the burst at BlockSize; see TestRateLimiterSubBlockLimitDoesNotHang).
	const rate = 2 * BlockSize // 32,768 B/s, above one 16 KB block
	rl := NewRateLimiter(rate)

	ctx := context.Background()

	// Drain the initial burst.
	if err := rl.Wait(ctx, rate); err != nil {
		t.Fatalf("initial burst wait failed: %v", err)
	}

	// Now the bucket is empty; requesting half the rate should take ~500ms.
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

// TestRateLimiterSubBlockLimitDoesNotHang pins the fix for a limit set below one
// block. Previously the burst cap equalled the limit, so a full BlockSize request
// could never accumulate enough tokens (maxTokens < needed) and Wait spun forever
// until the context was cancelled — wedging the peer goroutine that called it. The
// burst is now floored at BlockSize, so a single block always eventually fits while
// the configured rate still throttles.
func TestRateLimiterSubBlockLimitDoesNotHang(t *testing.T) {
	const rate = BlockSize / 4 // 4 KB/s, well below one 16 KB block
	rl := NewRateLimiter(rate)

	// A fresh limiter starts with a full burst, so the first full-block request must
	// return promptly rather than hanging. The context timeout is the regression
	// guard: the old code would block here until it expired.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	if err := rl.Wait(ctx, BlockSize); err != nil {
		t.Fatalf("full-block request under a sub-block limit hung/failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("first full-block request should be immediate (full burst), took %v", elapsed)
	}

	// The initial burst is now drained. A further request must still be throttled by
	// the configured rate (proving the floored burst didn't turn the limiter into a
	// no-op) yet complete in bounded time (proving the bucket can refill past the
	// request size). rate bytes take ~1s at rate B/s.
	start = time.Now()
	if err := rl.Wait(ctx, rate); err != nil {
		t.Fatalf("throttled refill under a sub-block limit failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 300*time.Millisecond {
		t.Fatalf("expected the sub-block limit to still throttle (~1s), but waited only %v", elapsed)
	}
	if elapsed > 2500*time.Millisecond {
		t.Fatalf("sub-block-limit refill took too long: %v", elapsed)
	}
}

func TestRateLimiterTryReserveAndRefund(t *testing.T) {
	rl := NewRateLimiter(BlockSize)

	ok, charged, retryAfter := rl.tryReserve(BlockSize)
	if !ok || !charged || retryAfter != 0 {
		t.Fatalf("initial reservation = (%v, %v, %v), want (true, true, 0)", ok, charged, retryAfter)
	}

	ok, charged, retryAfter = rl.tryReserve(BlockSize)
	if ok || charged {
		t.Fatalf("empty bucket reservation = (%v, %v), want (false, false)", ok, charged)
	}
	if retryAfter < 10*time.Millisecond || retryAfter > 100*time.Millisecond {
		t.Fatalf("retry delay %v outside bounded scheduler range", retryAfter)
	}

	rl.refund(BlockSize)
	ok, charged, retryAfter = rl.tryReserve(BlockSize)
	if !ok || !charged || retryAfter != 0 {
		t.Fatalf("refunded reservation = (%v, %v, %v), want (true, true, 0)", ok, charged, retryAfter)
	}

	unlimited := NewRateLimiter(0)
	ok, charged, retryAfter = unlimited.tryReserve(BlockSize)
	if !ok || charged || retryAfter != 0 {
		t.Fatalf("unlimited reservation = (%v, %v, %v), want (true, false, 0)", ok, charged, retryAfter)
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
