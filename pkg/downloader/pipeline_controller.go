package downloader

import (
	"math"
	"sort"
	"sync/atomic"
	"time"
)

const (
	dynamicPipelineMinWindowBlocks     = 4
	dynamicPipelineInitialWindowBlocks = 64
	dynamicPipelineMaxWindowBlocks     = 1024
	dynamicPipelineStartupCeiling      = 256
	dynamicPipelineHealthyFloorBlocks  = dynamicPipelineStartupCeiling
	dynamicPipelinePeerMaxBytes        = int64(dynamicPipelineMaxWindowBlocks * BlockSize)
	dynamicPipelineSessionBudgetBytes  = int64(maxOutboundPeers * dynamicPipelineHealthyFloorBlocks * BlockSize)
	minConcurrentPiecesPerPeer         = 16
	maxConcurrentPiecesPerPeer         = 2048
)

type pipelineByteBudget struct {
	limit     atomic.Int64
	used      atomic.Int64
	highWater atomic.Int64
}

func newPipelineByteBudget(limit int64) *pipelineByteBudget {
	b := &pipelineByteBudget{}
	b.limit.Store(limit)
	return b
}

func (b *pipelineByteBudget) tryReserve(n int64) bool {
	if n <= 0 {
		return true
	}
	limit := b.limit.Load()
	if limit <= 0 {
		return true
	}
	for {
		used := b.used.Load()
		next := used + n
		if next < used || next > limit {
			return false
		}
		if b.used.CompareAndSwap(used, next) {
			b.recordHighWater(next)
			return true
		}
	}
}

func (b *pipelineByteBudget) release(n int64) {
	if n <= 0 || b.limit.Load() <= 0 {
		return
	}
	for {
		used := b.used.Load()
		next := used - n
		if next < 0 {
			next = 0
		}
		if b.used.CompareAndSwap(used, next) {
			return
		}
	}
}

func (b *pipelineByteBudget) snapshot() (limit, used, highWater int64) {
	return b.limit.Load(), b.used.Load(), b.highWater.Load()
}

func (b *pipelineByteBudget) recordHighWater(v int64) {
	for {
		cur := b.highWater.Load()
		if v <= cur {
			return
		}
		if b.highWater.CompareAndSwap(cur, v) {
			return
		}
	}
}

type peerPipelineConfig struct {
	MinWindowBlocks           int
	InitialWindowBlocks       int
	MaxWindowBlocks           int
	StartupProbeCeilingBlocks int
	TargetHorizonMin          time.Duration
	TargetHorizonMax          time.Duration
	PeerMaxOutstandingBytes   int64
	MaxConcurrentPieces       int
}

func defaultPeerPipelineConfig() peerPipelineConfig {
	return peerPipelineConfig{
		MinWindowBlocks:           dynamicPipelineMinWindowBlocks,
		InitialWindowBlocks:       dynamicPipelineInitialWindowBlocks,
		MaxWindowBlocks:           dynamicPipelineMaxWindowBlocks,
		StartupProbeCeilingBlocks: dynamicPipelineStartupCeiling,
		TargetHorizonMin:          time.Second,
		TargetHorizonMax:          5 * time.Second,
		PeerMaxOutstandingBytes:   dynamicPipelinePeerMaxBytes,
		MaxConcurrentPieces:       maxConcurrentPiecesPerPeer,
	}
}

type peerPipelineMetrics struct {
	acceptedBytes     int64
	requestedBytes    int64
	wastedBytes       int64
	outstandingBytes  int64
	outstandingBlocks int

	rateFast     ewmaFloat
	rateSlow     ewmaFloat
	interArrival ewmaDuration
	srtt         ewmaDuration

	latencySamples [32]time.Duration
	latencyIndex   int
	latencyCount   int
	rttMin         time.Duration

	requestsSent          uint64
	blocksAccepted        uint64
	blocksRetried         uint64
	blocksTimedOut        uint64
	blocksCancelled       uint64
	duplicateBlocks       uint64
	unsolicitedBlocks     uint64
	appLimitedEvents      uint64
	budgetLimitedEvents   uint64
	pieceCapLimitedEvents uint64
	writerLimitedEvents   uint64
}

type ewmaFloat struct {
	value       float64
	initialized bool
	halfLife    time.Duration
}

func newEWMAFloat(halfLife time.Duration) ewmaFloat {
	return ewmaFloat{halfLife: halfLife}
}

func (e *ewmaFloat) update(sample float64, dt time.Duration) {
	if sample < 0 || math.IsNaN(sample) || math.IsInf(sample, 0) {
		return
	}
	if !e.initialized {
		e.value = sample
		e.initialized = true
		return
	}
	if dt <= 0 {
		return
	}
	halfLife := e.halfLife
	if halfLife <= 0 {
		halfLife = time.Second
	}
	alpha := 1 - math.Exp(-math.Ln2*float64(dt)/float64(halfLife))
	if alpha < 0 {
		alpha = 0
	} else if alpha > 1 {
		alpha = 1
	}
	e.value += alpha * (sample - e.value)
}

type ewmaDuration struct {
	value       time.Duration
	initialized bool
	halfLife    time.Duration
}

func newEWMADuration(halfLife time.Duration) ewmaDuration {
	return ewmaDuration{halfLife: halfLife}
}

func (e *ewmaDuration) update(sample, dt time.Duration) {
	if sample <= 0 {
		return
	}
	if !e.initialized {
		e.value = sample
		e.initialized = true
		return
	}
	if dt <= 0 {
		return
	}
	halfLife := e.halfLife
	if halfLife <= 0 {
		halfLife = time.Second
	}
	alpha := 1 - math.Exp(-math.Ln2*float64(dt)/float64(halfLife))
	if alpha < 0 {
		alpha = 0
	} else if alpha > 1 {
		alpha = 1
	}
	next := float64(e.value) + alpha*(float64(sample)-float64(e.value))
	e.value = time.Duration(next)
}

type peerPipelineController struct {
	cfg peerPipelineConfig

	windowBlocks       int
	targetWindowBlocks int
	inStartupProbe     bool

	metrics peerPipelineMetrics

	lastAcceptedAt     time.Time
	lastRateSampleAt   time.Time
	pendingRateBytes   int64
	lastWindowUpdate   time.Time
	lastProbeAt        time.Time
	windowLimitedUntil time.Time
	chokedAt           time.Time
	seq                uint64

	probeCooldownUntil   time.Time
	appLimitedUntil      time.Time
	budgetLimitedUntil   time.Time
	writerLimitedUntil   time.Time
	pieceCapLimitedUntil time.Time
}

func newPeerPipelineController(cfg peerPipelineConfig) *peerPipelineController {
	cfg = normalizePeerPipelineConfig(cfg)
	return &peerPipelineController{
		cfg:                cfg,
		windowBlocks:       cfg.InitialWindowBlocks,
		targetWindowBlocks: cfg.InitialWindowBlocks,
		inStartupProbe:     true,
		metrics: peerPipelineMetrics{
			rateFast:     newEWMAFloat(2 * time.Second),
			rateSlow:     newEWMAFloat(20 * time.Second),
			interArrival: newEWMADuration(2 * time.Second),
			srtt:         newEWMADuration(5 * time.Second),
		},
	}
}

func normalizePeerPipelineConfig(cfg peerPipelineConfig) peerPipelineConfig {
	if cfg.MinWindowBlocks <= 0 {
		cfg.MinWindowBlocks = dynamicPipelineMinWindowBlocks
	}
	if cfg.InitialWindowBlocks <= 0 {
		cfg.InitialWindowBlocks = dynamicPipelineInitialWindowBlocks
	}
	if cfg.InitialWindowBlocks < cfg.MinWindowBlocks {
		cfg.InitialWindowBlocks = cfg.MinWindowBlocks
	}
	if cfg.MaxWindowBlocks <= 0 {
		cfg.MaxWindowBlocks = dynamicPipelineMaxWindowBlocks
	}
	if cfg.MaxWindowBlocks < cfg.InitialWindowBlocks {
		cfg.MaxWindowBlocks = cfg.InitialWindowBlocks
	}
	if cfg.StartupProbeCeilingBlocks <= 0 {
		cfg.StartupProbeCeilingBlocks = dynamicPipelineStartupCeiling
	}
	if cfg.StartupProbeCeilingBlocks < cfg.InitialWindowBlocks {
		cfg.StartupProbeCeilingBlocks = cfg.InitialWindowBlocks
	}
	if cfg.StartupProbeCeilingBlocks > cfg.MaxWindowBlocks {
		cfg.StartupProbeCeilingBlocks = cfg.MaxWindowBlocks
	}
	if cfg.TargetHorizonMin <= 0 {
		cfg.TargetHorizonMin = time.Second
	}
	if cfg.TargetHorizonMax <= 0 || cfg.TargetHorizonMax < cfg.TargetHorizonMin {
		cfg.TargetHorizonMax = 5 * time.Second
	}
	if cfg.PeerMaxOutstandingBytes <= 0 {
		cfg.PeerMaxOutstandingBytes = dynamicPipelinePeerMaxBytes
	}
	maxByBytes := int(cfg.PeerMaxOutstandingBytes / BlockSize)
	if maxByBytes <= 0 {
		maxByBytes = 1
	}
	if cfg.MaxWindowBlocks > maxByBytes {
		cfg.MaxWindowBlocks = maxByBytes
	}
	if cfg.MaxConcurrentPieces <= 0 {
		cfg.MaxConcurrentPieces = maxConcurrentPiecesPerPeer
	}
	if cfg.MaxConcurrentPieces < minConcurrentPiecesPerPeer {
		cfg.MaxConcurrentPieces = minConcurrentPiecesPerPeer
	}
	return cfg
}

func (p *peerPipelineController) WindowBlocks(now time.Time) int {
	p.updateWindow(now)
	return p.windowBlocks
}

func (p *peerPipelineController) OutstandingBlocks() int {
	return p.metrics.outstandingBlocks
}

func (p *peerPipelineController) CanReserve(length int64) bool {
	if length <= 0 {
		return true
	}
	return p.metrics.outstandingBytes+length <= p.cfg.PeerMaxOutstandingBytes
}

func (p *peerPipelineController) ConcurrentPieceCap(avgBlocksPerPiece, requestablePiecesRemaining int) int {
	if avgBlocksPerPiece <= 0 {
		avgBlocksPerPiece = 1
	}
	desired := divCeil(p.windowBlocks, avgBlocksPerPiece) + 1
	if desired < minConcurrentPiecesPerPeer {
		desired = minConcurrentPiecesPerPeer
	}
	if requestablePiecesRemaining > 0 && desired > requestablePiecesRemaining {
		desired = requestablePiecesRemaining
		if desired < minConcurrentPiecesPerPeer && requestablePiecesRemaining >= minConcurrentPiecesPerPeer {
			desired = minConcurrentPiecesPerPeer
		}
	}
	if desired > p.cfg.MaxConcurrentPieces {
		desired = p.cfg.MaxConcurrentPieces
	}
	return desired
}

func (p *peerPipelineController) EffectiveWindowBlocks(avgBlocksPerPiece int) int {
	if avgBlocksPerPiece <= 0 {
		avgBlocksPerPiece = 1
	}
	pieceCap := p.ConcurrentPieceCap(avgBlocksPerPiece, 0)
	effective := pieceCap * avgBlocksPerPiece
	if effective < p.windowBlocks {
		return effective
	}
	return p.windowBlocks
}

func (p *peerPipelineController) OnWindowLimited(now time.Time) {
	if p.limited(now) || now.Before(p.probeCooldownUntil) {
		return
	}
	p.windowLimitedUntil = now.Add(p.windowLimitedHold())
	minGap := p.probeGap()
	if !p.lastProbeAt.IsZero() && now.Sub(p.lastProbeAt) < minGap {
		return
	}
	p.lastProbeAt = now
	ceiling := p.cfg.StartupProbeCeilingBlocks
	if !p.inStartupProbe {
		ceiling = p.cfg.MaxWindowBlocks
	}
	growBy := p.windowBlocks
	if !p.inStartupProbe {
		growBy = max(1, p.windowBlocks/16)
	}
	target := p.windowBlocks + growBy
	if p.inStartupProbe && target < p.cfg.StartupProbeCeilingBlocks {
		target = p.cfg.StartupProbeCeilingBlocks
	}
	if floor := p.healthyWindowFloor(now); floor > target {
		target = floor
	}
	if target > ceiling {
		target = ceiling
	}
	if target > p.windowBlocks {
		p.windowBlocks = target
		p.targetWindowBlocks = target
	}
	p.maybeFinishStartupProbe(now)
}

func (p *peerPipelineController) OnRequestSent(req *blockRequest, now time.Time) {
	if req == nil || req.length <= 0 {
		return
	}
	p.seq++
	req.controllerSeq = p.seq
	p.metrics.requestsSent++
	p.metrics.requestedBytes += req.length
	p.metrics.outstandingBlocks++
	p.metrics.outstandingBytes += req.length
}

func (p *peerPipelineController) OnBlockAccepted(req *blockRequest, bytes int64, now time.Time) {
	if req == nil || bytes <= 0 {
		return
	}
	p.releaseOutstanding(req)
	p.metrics.acceptedBytes += bytes
	p.metrics.blocksAccepted++

	if !p.lastAcceptedAt.IsZero() {
		p.metrics.interArrival.update(now.Sub(p.lastAcceptedAt), now.Sub(p.lastAcceptedAt))
	}
	p.lastAcceptedAt = now

	p.recordRate(bytes, now)
	if req.retries == 0 && !req.requestedAt.IsZero() {
		p.recordLatency(now.Sub(req.requestedAt), now)
	}
	p.updateWindow(now)
}

func (p *peerPipelineController) OnRequestTimeout(req *blockRequest, now time.Time) {
	if req != nil && req.retries > 0 {
		p.metrics.blocksRetried++
	}
	p.releaseOutstanding(req)
	p.metrics.blocksTimedOut++
	cooldown := maxDuration(time.Second, 2*p.smoothedRTT())
	if !now.Before(p.probeCooldownUntil) {
		p.windowBlocks = max(p.cfg.MinWindowBlocks, p.windowBlocks/2)
		p.targetWindowBlocks = p.windowBlocks
	}
	if until := now.Add(cooldown); until.After(p.probeCooldownUntil) {
		p.probeCooldownUntil = until
	}
	p.windowLimitedUntil = time.Time{}
}

func (p *peerPipelineController) OnCancel(req *blockRequest, now time.Time) {
	p.releaseOutstanding(req)
	p.metrics.blocksCancelled++
}

func (p *peerPipelineController) OnDuplicate(bytes int64, now time.Time) {
	if bytes > 0 {
		p.metrics.wastedBytes += bytes
	}
	p.metrics.duplicateBlocks++
}

func (p *peerPipelineController) OnUnsolicited(bytes int64, now time.Time) {
	if bytes > 0 {
		p.metrics.wastedBytes += bytes
	}
	p.metrics.unsolicitedBlocks++
}

func (p *peerPipelineController) OnAppLimited(now time.Time, retryAfter time.Duration) {
	p.metrics.appLimitedEvents++
	if retryAfter <= 0 {
		retryAfter = 250 * time.Millisecond
	}
	p.appLimitedUntil = now.Add(maxDuration(retryAfter, 250*time.Millisecond))
}

func (p *peerPipelineController) OnBudgetLimited(now time.Time) {
	p.metrics.budgetLimitedEvents++
	p.budgetLimitedUntil = now.Add(500 * time.Millisecond)
}

func (p *peerPipelineController) OnPieceCapLimited(now time.Time) {
	p.metrics.pieceCapLimitedEvents++
	p.pieceCapLimitedUntil = now.Add(500 * time.Millisecond)
}

func (p *peerPipelineController) OnWriterLimited(now time.Time) {
	p.metrics.writerLimitedEvents++
	p.writerLimitedUntil = now.Add(time.Second)
}

func (p *peerPipelineController) OnChoke(now time.Time) {
	p.chokedAt = now
}

func (p *peerPipelineController) OnUnchoke(now time.Time) {
	if !p.chokedAt.IsZero() && now.Sub(p.chokedAt) > 30*time.Second {
		p.windowBlocks = p.cfg.InitialWindowBlocks
		p.targetWindowBlocks = p.cfg.InitialWindowBlocks
		p.inStartupProbe = true
	}
	p.chokedAt = time.Time{}
}

func (p *peerPipelineController) Snapshot(now time.Time) peerPipelineSnapshot {
	rate := p.controlRate()
	queueSeconds := 0.0
	if rate > 1 {
		queueSeconds = float64(p.metrics.outstandingBytes) / rate
	}
	timeoutRate := 0.0
	if p.metrics.requestsSent > 0 {
		timeoutRate = float64(p.metrics.blocksTimedOut) / float64(p.metrics.requestsSent)
	}
	return peerPipelineSnapshot{
		WindowBlocks:       p.windowBlocks,
		TargetWindowBlocks: p.targetWindowBlocks,
		OutstandingBlocks:  p.metrics.outstandingBlocks,
		OutstandingBytes:   p.metrics.outstandingBytes,
		QueueSeconds:       queueSeconds,
		RTT:                p.smoothedRTT(),
		Rate:               rate,
		TimeoutRate:        timeoutRate,
		AppLimited:         now.Before(p.appLimitedUntil),
		BudgetLimited:      now.Before(p.budgetLimitedUntil),
		PieceCapLimited:    now.Before(p.pieceCapLimitedUntil),
		WriterLimited:      now.Before(p.writerLimitedUntil),
	}
}

type peerPipelineSnapshot struct {
	WindowBlocks       int
	TargetWindowBlocks int
	OutstandingBlocks  int
	OutstandingBytes   int64
	QueueSeconds       float64
	RTT                time.Duration
	Rate               float64
	TimeoutRate        float64
	AppLimited         bool
	BudgetLimited      bool
	PieceCapLimited    bool
	WriterLimited      bool
}

func (p *peerPipelineController) updateWindow(now time.Time) {
	if p.windowBlocks <= 0 {
		p.windowBlocks = p.cfg.InitialWindowBlocks
	}
	if p.limited(now) {
		return
	}
	rate := p.controlRate()
	if rate <= 1 {
		if p.windowBlocks < p.cfg.MinWindowBlocks {
			p.windowBlocks = p.cfg.MinWindowBlocks
		}
		p.targetWindowBlocks = p.windowBlocks
		p.maybeFinishStartupProbe(now)
		return
	}
	target := p.modelWindowBlocks(rate)
	if p.inStartupProbe {
		if target <= p.windowBlocks && !p.healthyDemandLimited(now) {
			p.inStartupProbe = false
		} else if target < p.windowBlocks {
			target = p.windowBlocks
		}
	}
	if floor := p.healthyWindowFloor(now); floor > target {
		target = floor
	}
	p.targetWindowBlocks = target

	if target > p.windowBlocks {
		if !p.lastWindowUpdate.IsZero() && now.Sub(p.lastWindowUpdate) < 50*time.Millisecond {
			return
		}
		step := max(2, p.windowBlocks/8)
		p.windowBlocks = min(p.windowBlocks+step, target)
		p.lastWindowUpdate = now
		return
	}
	if target < p.windowBlocks {
		if !p.lastWindowUpdate.IsZero() && now.Sub(p.lastWindowUpdate) < 100*time.Millisecond {
			return
		}
		next := p.windowBlocks - max(1, p.windowBlocks/10)
		if next < target {
			next = target
		}
		p.windowBlocks = clamp(next, p.cfg.MinWindowBlocks, p.cfg.MaxWindowBlocks)
		p.lastWindowUpdate = now
	}
	p.maybeFinishStartupProbe(now)
}

func (p *peerPipelineController) limited(now time.Time) bool {
	return now.Before(p.appLimitedUntil) ||
		now.Before(p.budgetLimitedUntil) ||
		now.Before(p.writerLimitedUntil) ||
		now.Before(p.pieceCapLimitedUntil)
}

func (p *peerPipelineController) controlRate() float64 {
	switch {
	case p.metrics.rateFast.initialized && p.metrics.rateSlow.initialized:
		return math.Max(p.metrics.rateFast.value, p.metrics.rateSlow.value)
	case p.metrics.rateFast.initialized:
		return p.metrics.rateFast.value
	case p.metrics.rateSlow.initialized:
		return p.metrics.rateSlow.value
	default:
		return 0
	}
}

func (p *peerPipelineController) modelWindowBlocks(rate float64) int {
	horizon := p.targetHorizon()
	modelBlocks := divCeil(int(math.Ceil(rate*horizon.Seconds())), BlockSize)
	return clamp(modelBlocks, p.cfg.MinWindowBlocks, p.cfg.MaxWindowBlocks)
}

func (p *peerPipelineController) recordRate(bytes int64, now time.Time) {
	if bytes <= 0 {
		return
	}
	if p.lastRateSampleAt.IsZero() {
		p.lastRateSampleAt = now
		p.pendingRateBytes = bytes
		return
	}
	p.pendingRateBytes += bytes
	dt := now.Sub(p.lastRateSampleAt)
	if dt < 25*time.Millisecond {
		return
	}
	sample := float64(p.pendingRateBytes) / dt.Seconds()
	p.metrics.rateFast.update(sample, dt)
	p.metrics.rateSlow.update(sample, dt)
	if p.healthyDemandLimited(now) && p.metrics.rateFast.initialized &&
		(!p.metrics.rateSlow.initialized || p.metrics.rateSlow.value < p.metrics.rateFast.value) {
		p.metrics.rateSlow.value = p.metrics.rateFast.value
		p.metrics.rateSlow.initialized = true
	}
	p.pendingRateBytes = 0
	p.lastRateSampleAt = now
}

func (p *peerPipelineController) recordLatency(latency time.Duration, now time.Time) {
	if latency <= 0 || now.Before(p.writerLimitedUntil) {
		return
	}
	dt := latency
	if !p.lastAcceptedAt.IsZero() {
		dt = maxDuration(time.Millisecond, now.Sub(p.lastAcceptedAt))
	}
	p.metrics.srtt.update(latency, dt)
	if p.metrics.rttMin == 0 || latency < p.metrics.rttMin {
		p.metrics.rttMin = latency
	}
	p.metrics.latencySamples[p.metrics.latencyIndex%len(p.metrics.latencySamples)] = latency
	p.metrics.latencyIndex++
	if p.metrics.latencyCount < len(p.metrics.latencySamples) {
		p.metrics.latencyCount++
	}
}

func (p *peerPipelineController) targetHorizon() time.Duration {
	srtt := p.smoothedRTT()
	base := p.cfg.TargetHorizonMin
	if srtt > 0 {
		base = maxDuration(base, 6*srtt)
	}
	cushion := time.Duration(0)
	if p95 := p.latencyP95(); p95 > srtt && srtt > 0 {
		cushion = p95 - srtt
		if cushion > 500*time.Millisecond {
			cushion = 500 * time.Millisecond
		}
	}
	return clampDuration(base+cushion, p.cfg.TargetHorizonMin, p.cfg.TargetHorizonMax)
}

func (p *peerPipelineController) smoothedRTT() time.Duration {
	if p.metrics.srtt.initialized {
		return p.metrics.srtt.value
	}
	if p.metrics.rttMin > 0 {
		return p.metrics.rttMin
	}
	return 0
}

func (p *peerPipelineController) latencyP95() time.Duration {
	if p.metrics.latencyCount == 0 {
		return 0
	}
	samples := make([]time.Duration, p.metrics.latencyCount)
	copy(samples, p.metrics.latencySamples[:p.metrics.latencyCount])
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	idx := int(math.Ceil(float64(len(samples))*0.95)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return samples[idx]
}

func (p *peerPipelineController) probeGap() time.Duration {
	if rtt := p.smoothedRTT(); rtt > 0 {
		return clampDuration(rtt, 100*time.Millisecond, time.Second)
	}
	return 200 * time.Millisecond
}

func (p *peerPipelineController) windowLimitedHold() time.Duration {
	return maxDuration(time.Second, 2*p.probeGap())
}

func (p *peerPipelineController) healthyDemandLimited(now time.Time) bool {
	return now.Before(p.windowLimitedUntil) &&
		!now.Before(p.probeCooldownUntil) &&
		!p.limited(now)
}

func (p *peerPipelineController) healthyWindowFloor(now time.Time) int {
	if !p.healthyDemandLimited(now) {
		return 0
	}
	return clamp(dynamicPipelineHealthyFloorBlocks, p.cfg.MinWindowBlocks, p.cfg.MaxWindowBlocks)
}

func (p *peerPipelineController) maybeFinishStartupProbe(now time.Time) {
	if !p.inStartupProbe {
		return
	}
	if p.windowBlocks >= p.cfg.StartupProbeCeilingBlocks {
		p.inStartupProbe = false
		return
	}
	if rate := p.controlRate(); rate > 1 && p.modelWindowBlocks(rate) <= p.windowBlocks && !p.healthyDemandLimited(now) {
		p.inStartupProbe = false
	}
}

func (p *peerPipelineController) releaseOutstanding(req *blockRequest) {
	if req == nil || req.controllerSeq == 0 {
		return
	}
	if p.metrics.outstandingBlocks > 0 {
		p.metrics.outstandingBlocks--
	}
	p.metrics.outstandingBytes -= req.length
	if p.metrics.outstandingBytes < 0 {
		p.metrics.outstandingBytes = 0
	}
	req.controllerSeq = 0
}

func divCeil(n, d int) int {
	if d <= 0 {
		return 0
	}
	if n <= 0 {
		return 0
	}
	return (n + d - 1) / d
}

func clamp(v, low, high int) int {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}

func clampDuration(v, low, high time.Duration) time.Duration {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
