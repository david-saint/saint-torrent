package downloader

import (
	"testing"
	"time"
)

func testPipelineConfig() peerPipelineConfig {
	return peerPipelineConfig{
		MinWindowBlocks:           4,
		InitialWindowBlocks:       16,
		MaxWindowBlocks:           1024,
		StartupProbeCeilingBlocks: 256,
		TargetHorizonMin:          time.Second,
		TargetHorizonMax:          5 * time.Second,
		PeerMaxOutstandingBytes:   16 * 1024 * 1024,
		MaxConcurrentPieces:       2048,
	}
}

func TestDynamicWindowStartupProbeAndRaisedCap(t *testing.T) {
	now := time.Unix(100, 0)
	p := newPeerPipelineController(testPipelineConfig())

	if got := p.WindowBlocks(now); got != 16 {
		t.Fatalf("initial window = %d, want 16", got)
	}

	now = now.Add(250 * time.Millisecond)
	p.OnWindowLimited(now)
	if got := p.WindowBlocks(now); got != 256 {
		t.Fatalf("startup probe window = %d, want 256", got)
	}

	for i := 0; i < 96; i++ {
		req := &blockRequest{length: BlockSize, requested: true, requestedAt: now}
		p.OnRequestSent(req, now)
		now = now.Add(1 * time.Millisecond)
		p.OnBlockAccepted(req, BlockSize, now)
		p.WindowBlocks(now)
	}

	if got := p.WindowBlocks(now); got <= 256 {
		t.Fatalf("window did not grow past fixed cap: got %d", got)
	}
	if got := p.WindowBlocks(now); got > 1024 {
		t.Fatalf("window exceeded configured cap: got %d", got)
	}
}

func TestDynamicWindowSlowPeerShrinksTowardMinimum(t *testing.T) {
	now := time.Unix(200, 0)
	p := newPeerPipelineController(testPipelineConfig())

	for i := 0; i < 12; i++ {
		req := &blockRequest{length: BlockSize, requested: true, requestedAt: now}
		p.OnRequestSent(req, now)
		now = now.Add(2 * time.Second)
		p.OnBlockAccepted(req, BlockSize, now)
		p.WindowBlocks(now)
	}

	if got := p.WindowBlocks(now); got > 8 {
		t.Fatalf("slow peer window = %d, want <= 8", got)
	}
	if got := p.WindowBlocks(now); got < 4 {
		t.Fatalf("slow peer window = %d, want min clamp 4", got)
	}
}

func TestDynamicWindowStartupDoesNotExitOnAcceptedBlockCount(t *testing.T) {
	now := time.Unix(250, 0)
	p := newPeerPipelineController(testPipelineConfig())

	for i := 0; i < 16; i++ {
		req := &blockRequest{length: BlockSize, requested: true, requestedAt: now.Add(-50 * time.Millisecond)}
		p.OnRequestSent(req, req.requestedAt)
		now = now.Add(2 * time.Millisecond)
		p.OnBlockAccepted(req, BlockSize, now)
	}

	if !p.inStartupProbe {
		t.Fatal("startup probe exited solely because 16 blocks were accepted")
	}
}

func TestDynamicWindowHealthyWindowLimitedFloor(t *testing.T) {
	now := time.Unix(260, 0)
	p := newPeerPipelineController(testPipelineConfig())
	p.inStartupProbe = false
	p.windowBlocks = 32
	p.targetWindowBlocks = 32
	p.metrics.rateFast.update(float64(8*BlockSize), time.Second)
	p.metrics.rateSlow.update(float64(8*BlockSize), time.Second)

	p.OnWindowLimited(now)

	if got := p.WindowBlocks(now); got < dynamicPipelineHealthyFloorBlocks {
		t.Fatalf("healthy window-limited peer window = %d, want at least %d", got, dynamicPipelineHealthyFloorBlocks)
	}

	now = now.Add(200 * time.Millisecond)
	if got := p.WindowBlocks(now); got < dynamicPipelineHealthyFloorBlocks {
		t.Fatalf("healthy floor was not preserved during active demand: got %d", got)
	}
}

func TestDynamicWindowTimeoutDisablesHealthyFloor(t *testing.T) {
	now := time.Unix(270, 0)
	p := newPeerPipelineController(testPipelineConfig())
	p.inStartupProbe = false
	p.windowBlocks = 128
	p.targetWindowBlocks = 128

	p.OnRequestTimeout(nil, now)
	now = now.Add(500 * time.Millisecond)
	p.OnWindowLimited(now)

	if got := p.WindowBlocks(now); got >= dynamicPipelineHealthyFloorBlocks {
		t.Fatalf("timeout-limited peer jumped to healthy floor: got %d, want below %d", got, dynamicPipelineHealthyFloorBlocks)
	}

	now = now.Add(time.Second)
	p.OnWindowLimited(now)
	if got := p.WindowBlocks(now); got < dynamicPipelineHealthyFloorBlocks {
		t.Fatalf("peer did not recover healthy floor after timeout cooldown: got %d", got)
	}
}

func TestDynamicWindowTimeoutBackoffHalvesOncePerCooldown(t *testing.T) {
	now := time.Unix(280, 0)
	p := newPeerPipelineController(testPipelineConfig())
	p.inStartupProbe = false
	p.windowBlocks = 256
	p.targetWindowBlocks = 256

	p.OnRequestTimeout(nil, now)
	if got := p.WindowBlocks(now); got != 128 {
		t.Fatalf("window after first timeout = %d, want 128", got)
	}

	p.OnRequestTimeout(nil, now.Add(100*time.Millisecond))
	if got := p.WindowBlocks(now.Add(100 * time.Millisecond)); got != 128 {
		t.Fatalf("window after second timeout in cooldown = %d, want 128", got)
	}
}

func TestDynamicWindowTimeoutHalvesWindowAndReleasesOutstanding(t *testing.T) {
	now := time.Unix(300, 0)
	p := newPeerPipelineController(testPipelineConfig())
	now = now.Add(250 * time.Millisecond)
	p.OnWindowLimited(now)
	req := &blockRequest{length: BlockSize, requested: true, requestedAt: now}
	p.OnRequestSent(req, now)

	p.OnRequestTimeout(req, now.Add(time.Second))

	if got := p.WindowBlocks(now.Add(time.Second)); got != 128 {
		t.Fatalf("window after timeout = %d, want 128", got)
	}
	if got := p.OutstandingBlocks(); got != 0 {
		t.Fatalf("outstanding after timeout = %d, want 0", got)
	}
	if req.controllerSeq != 0 {
		t.Fatal("timeout did not clear request controller sequence")
	}
}

func TestDynamicWindowLimiterFlagsFreezeGrowth(t *testing.T) {
	now := time.Unix(400, 0)
	p := newPeerPipelineController(testPipelineConfig())
	p.OnAppLimited(now, 500*time.Millisecond)
	p.OnBudgetLimited(now)
	p.OnPieceCapLimited(now)
	p.OnWriterLimited(now)

	before := p.WindowBlocks(now)
	p.OnWindowLimited(now.Add(100 * time.Millisecond))
	after := p.WindowBlocks(now.Add(100 * time.Millisecond))
	if after != before {
		t.Fatalf("limited window changed from %d to %d", before, after)
	}

	snap := p.Snapshot(now.Add(100 * time.Millisecond))
	if !snap.AppLimited || !snap.BudgetLimited || !snap.PieceCapLimited || !snap.WriterLimited {
		t.Fatalf("snapshot flags = app:%v budget:%v piece:%v writer:%v, want all true",
			snap.AppLimited, snap.BudgetLimited, snap.PieceCapLimited, snap.WriterLimited)
	}
}

func TestDynamicWindowConcurrentPieceCapScalesWithWindow(t *testing.T) {
	now := time.Unix(500, 0)
	p := newPeerPipelineController(testPipelineConfig())
	for i := 0; i < 4; i++ {
		now = now.Add(250 * time.Millisecond)
		p.OnWindowLimited(now)
	}

	if got := p.ConcurrentPieceCap(1, 0); got < 257 {
		t.Fatalf("one-block piece cap = %d, want at least 257", got)
	}
	if got := p.ConcurrentPieceCap(16, 0); got < minConcurrentPiecesPerPeer {
		t.Fatalf("normal piece cap = %d, want at least %d", got, minConcurrentPiecesPerPeer)
	}
}

func TestDynamicWindowByteBudgetReserveReleaseAndHighWater(t *testing.T) {
	b := newPipelineByteBudget(3 * BlockSize)
	if !b.tryReserve(BlockSize) || !b.tryReserve(2*BlockSize) {
		t.Fatal("expected reservations within limit to succeed")
	}
	if b.tryReserve(1) {
		t.Fatal("reservation beyond limit succeeded")
	}
	limit, used, high := b.snapshot()
	if limit != 3*BlockSize || used != 3*BlockSize || high != 3*BlockSize {
		t.Fatalf("snapshot = limit %d used %d high %d", limit, used, high)
	}
	b.release(BlockSize)
	if !b.tryReserve(BlockSize) {
		t.Fatal("reservation after release failed")
	}
	_, used, high = b.snapshot()
	if used != 3*BlockSize || high != 3*BlockSize {
		t.Fatalf("snapshot after release/re-reserve = used %d high %d", used, high)
	}
	b.release(10 * BlockSize)
	_, used, _ = b.snapshot()
	if used != 0 {
		t.Fatalf("over-release used = %d, want 0", used)
	}
}

func TestDynamicWindowSessionBudgetCoversLegacyOutboundDepth(t *testing.T) {
	want := int64(maxOutboundPeers * dynamicPipelineHealthyFloorBlocks * BlockSize)
	if dynamicPipelineSessionBudgetBytes < want {
		t.Fatalf("session pipeline budget = %d, want at least %d", dynamicPipelineSessionBudgetBytes, want)
	}
}
