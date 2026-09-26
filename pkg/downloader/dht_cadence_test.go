package downloader

import (
	"fmt"
	"testing"
	"time"

	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/peer"
)

// TestDHTLookupIntervalPerStateA2 pins the lookup cadence to what the session
// needs: every torrent used to look up every 30 s, seeding or not. A magnet
// fetching metadata still does; a download short of peers looks up every
// minute; a seed or a well connected download every 15 minutes; a paused or
// metadata-stalled session skips lookups.
func TestDHTLookupIntervalPerStateA2(t *testing.T) {
	check := func(t *testing.T, sess *Session, wantInterval time.Duration, wantSkip bool) {
		t.Helper()
		sess.mu.RLock()
		interval, skip := sess.dhtLookupIntervalLocked()
		sess.mu.RUnlock()
		if interval != wantInterval || skip != wantSkip {
			t.Fatalf("interval %v skip %v, want %v skip %v", interval, skip, wantInterval, wantSkip)
		}
	}

	t.Run("metadata", func(t *testing.T) {
		sess, _ := newMagnetTestSession(t, 1)
		check(t, sess, dhtMetadataLookupInterval, false)
		sess.mu.Lock()
		sess.metadataCompleted = true
		sess.statusErr = fmt.Errorf("storage unavailable")
		sess.mu.Unlock()
		check(t, sess, dhtMetadataLookupInterval, true)
	})
	t.Run("downloading", func(t *testing.T) {
		sess := newWireTestSession(t, 4, 16*1024)
		check(t, sess, dhtDownloadLookupInterval, false)

		sess.mu.Lock()
		for i := 0; i < dhtWellConnectedPeers; i++ {
			sess.activePeers[fmt.Sprintf("198.51.100.%d:6881", i)] = &peer.Client{}
		}
		sess.mu.Unlock()
		check(t, sess, dhtIdleLookupInterval, false)
		sess.mu.Lock()
		clear(sess.activePeers) // Close would close their (absent) connections
		sess.mu.Unlock()

		sess.Pause()
		check(t, sess, dhtMetadataLookupInterval, true)
	})
	t.Run("seeding", func(t *testing.T) {
		sess, _ := newSeedingWireTestSession(t, 2, 16*1024)
		check(t, sess, dhtIdleLookupInterval, false)
	})
}

// TestDHTIntervalJitterA2 checks lookup intervals are spread over ±20%, so
// sessions that started together drift out of phase.
func TestDHTIntervalJitterA2(t *testing.T) {
	const d = 100 * time.Second
	lo, hi := d, time.Duration(0)
	for i := 0; i < 2000; i++ {
		got := jitterDHTInterval(d)
		if got < d*8/10 || got > d*12/10 {
			t.Fatalf("jittered interval %v outside [%v, %v]", got, d*8/10, d*12/10)
		}
		lo, hi = min(lo, got), max(hi, got)
	}
	if lo > d*85/100 || hi < d*115/100 {
		t.Fatalf("jitter spans only [%v, %v] of ±20%%", lo, hi)
	}
}

// TestDHTFirstLookupSpreadA2 covers the startup burst: every restored torrent
// looked up exactly 1 s after start. A magnet fetching metadata still does;
// other sessions spread their first lookup over up to dhtStartupSpreadStep per
// running DHT loop, at most dhtStartupSpreadMax.
func TestDHTFirstLookupSpreadA2(t *testing.T) {
	if got := dhtFirstLookupAfter(true, 500); got != dhtFirstLookupDelay {
		t.Fatalf("a metadata fetch waits %v for its first lookup, want %v", got, dhtFirstLookupDelay)
	}
	for _, c := range []struct {
		running int32
		spread  time.Duration
	}{
		{1, dhtStartupSpreadStep},
		{40, 40 * dhtStartupSpreadStep},
		{100000, dhtStartupSpreadMax},
	} {
		lo, hi := time.Duration(1<<62), time.Duration(0)
		for i := 0; i < 500; i++ {
			got := dhtFirstLookupAfter(false, c.running)
			if got < dhtFirstLookupDelay || got >= dhtFirstLookupDelay+c.spread {
				t.Fatalf("%d loops: first lookup after %v, want in [%v, %v)", c.running, got, dhtFirstLookupDelay, dhtFirstLookupDelay+c.spread)
			}
			lo, hi = min(lo, got), max(hi, got)
		}
		if hi-lo < c.spread/2 {
			t.Fatalf("%d loops: first lookups span only %v of %v", c.running, hi-lo, c.spread)
		}
	}
}

// TestDHTLookupAfterResumeA2 checks a resume brings the next lookup forward:
// lookups are skipped while paused, and with 15-minute intervals a resumed
// session would otherwise wait up to that long to find peers again.
func TestDHTLookupAfterResumeA2(t *testing.T) {
	defer swapDuration(&dhtFirstLookupDelay, 10*time.Millisecond)()
	defer swapDuration(&dhtDownloadLookupInterval, time.Hour)()
	defer swapDuration(&dhtMetadataLookupInterval, time.Hour)()
	defer swapDuration(&dhtResumeLookupDelay, 20*time.Millisecond)()
	lookups := make(chan struct{}, 16)
	oldLookup := startDHTLookup
	startDHTLookup = func(*dht.DHT, [20]byte, uint16, bool) { lookups <- struct{}{} }
	defer func() { startDHTLookup = oldLookup }()

	d, err := dht.NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	defer d.Close()
	sess := newWireTestSession(t, 4, 16*1024)
	sess.AttachDHT(d)
	sess.mu.Lock()
	sess.sharedInbound, sess.Port = true, 6881
	sess.mu.Unlock()
	sess.wg.Add(1)
	go sess.dhtLoop()
	defer sess.Close() // stops the loop before the hook is restored

	waitLookup := func(what string) {
		t.Helper()
		select {
		case <-lookups:
		case <-time.After(2 * time.Second):
			t.Fatalf("no DHT lookup %s", what)
		}
	}
	waitLookup("after start")
	sess.Pause()
	select {
	case <-lookups:
		t.Fatal("a DHT lookup ran while paused")
	case <-time.After(100 * time.Millisecond):
	}
	sess.Resume()
	waitLookup("after resume")

	// Start resumes a started session too, and must wake the loop the same way.
	sess.Pause()
	sess.mu.Lock()
	sess.started = true
	sess.mu.Unlock()
	sess.Start()
	waitLookup("after a resume through Start")
}
