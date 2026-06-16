package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sainttorrent/pkg/tracker"
	"time"
)

const trackerDefaultNumWant = 200

// maxTrackerResponse caps how many bytes of an HTTP tracker's announce response we
// buffer. Legitimate replies (a compact peer list plus a little metadata) are a few KB
// even at numwant=200; this ceiling stops a malicious or MITM'd tracker from streaming
// unbounded data into memory.
const maxTrackerResponse = 2 * 1024 * 1024

var trackerAnnounceTimeout = 15 * time.Second

// trackerLoop handles periodic tracker announces.
// P1 FIX: Resume uses resumeCh signal instead of spawning untracked goroutines.
func (s *Session) trackerLoop() {
	defer s.wg.Done()
	var nextInterval time.Duration = 0 // announce immediately on start

	for {
		s.mu.RLock()
		paused := s.paused
		hasEvents := len(s.trackerEvents) > 0
		s.mu.RUnlock()

		var interval int
		if !paused || hasEvents {
			interval = s.announceAndConnect()
		}
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		s.mu.RLock()
		hasMoreEvents := len(s.trackerEvents) > 0
		s.mu.RUnlock()

		if interval == 0 && hasMoreEvents {
			// Announce failed, back off to prevent spamming trackers
			nextInterval = 15 * time.Second
		} else if hasMoreEvents {
			// Flush transition events quickly
			nextInterval = 100 * time.Millisecond
		} else if interval > 0 {
			nextInterval = time.Duration(interval) * time.Second
		} else {
			nextInterval = 30 * time.Second
		}

		if !hasMoreEvents {
			if nextInterval < 5*time.Second {
				nextInterval = 5 * time.Second
			}
			if nextInterval > 1*time.Hour {
				nextInterval = 1 * time.Hour
			}
		}

		timer := time.NewTimer(nextInterval)
		select {
		case <-timer.C:
		case <-s.resumeCh:
			// Event triggered — announce immediately
			timer.Stop()
		case <-s.ctx.Done():
			timer.Stop()
			return
		}
	}
}

type trackerAnnounceResult struct {
	peers      []tracker.Peer
	interval   int
	complete   int
	incomplete int
	err        error
}

func announceTracker(ctx context.Context, tr string, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, event string, timeout time.Duration) trackerAnnounceResult {
	announceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if bytes.HasPrefix([]byte(tr), []byte("udp")) {
		resp, err := tracker.UDPAnnounce(announceCtx, tr, infoHash, peerID, port, uploaded, downloaded, left, event, trackerDefaultNumWant)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}
		return trackerAnnounceResult{
			peers:      resp.Peers,
			interval:   resp.Interval,
			complete:   resp.Complete,
			incomplete: resp.Incomplete,
		}
	}

	if bytes.HasPrefix([]byte(tr), []byte("http")) {
		u, err := tracker.BuildTrackerURL(tr, infoHash, peerID, port, uploaded, downloaded, left, true, event, trackerDefaultNumWant)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		req, err := http.NewRequestWithContext(announceCtx, "GET", u, nil)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		client := &http.Client{Timeout: timeout}
		resp, err := client.Do(req)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		buf := new(bytes.Buffer)
		// Bound how much we buffer: a tracker announce reply is a few KB even at
		// numwant=200. The cap stops a malicious or MITM'd tracker from streaming
		// unbounded data into memory. Reading one byte past the cap lets us detect
		// and reject an over-limit response rather than silently truncating it.
		_, err = io.Copy(buf, io.LimitReader(resp.Body, maxTrackerResponse+1))
		resp.Body.Close()
		if err != nil {
			return trackerAnnounceResult{err: err}
		}
		if buf.Len() > maxTrackerResponse {
			return trackerAnnounceResult{err: fmt.Errorf("tracker response exceeds %d bytes", maxTrackerResponse)}
		}

		trackerResp, err := tracker.ParseTrackerResponse(buf.Bytes())
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		return trackerAnnounceResult{
			peers:      trackerResp.Peers,
			interval:   trackerResp.Interval,
			complete:   trackerResp.Complete,
			incomplete: trackerResp.Incomplete,
		}
	}

	return trackerAnnounceResult{err: fmt.Errorf("unsupported tracker scheme: %s", tr)}
}

func (s *Session) announceAndConnect() int {
	s.mu.Lock()
	trackers := append([]string(nil), s.Torrent.Trackers...)
	if len(trackers) == 0 {
		// No trackers configured: consume all queued events as completed/success
		if len(s.trackerEvents) > 0 {
			for _, ev := range s.trackerEvents {
				if ev == "stopped" {
					s.stoppedAnnounced = true
				} else if ev == "started" {
					s.stoppedAnnounced = false
				}
			}
			s.trackerEvents = nil
		}
		s.mu.Unlock()
		return 0
	}

	var downloaded, left int64
	if s.metadataMode || s.Storage == nil || len(s.PieceStates) == 0 {
		left = 1
	} else {
		stats := s.completionStatsLocked()
		downloaded = stats.completedTotalBytes
		left = stats.totalBytes - stats.completedTotalBytes
		if left < 0 {
			left = 0
		}
	}
	port := s.Port
	uploaded := s.Uploaded.Load()
	infoHash := s.Torrent.InfoHash
	peerID := s.PeerID
	var event string
	if len(s.trackerEvents) > 0 {
		event = s.trackerEvents[0]
		s.trackerEvents = s.trackerEvents[1:]
	}
	paused := s.paused
	s.mu.Unlock()

	var peers []tracker.Peer
	var interval int
	var trackerErr error
	var trackerSeeders int
	var trackerLeechers int

	results := make(chan trackerAnnounceResult, len(trackers))
	for _, tr := range trackers {
		trackerURL := tr
		go func() {
			results <- announceTracker(s.ctx, trackerURL, infoHash, peerID, port, uploaded, downloaded, left, event, trackerAnnounceTimeout)
		}()
	}

	seenPeers := make(map[string]bool)
	trackerSuccess := false
	for range trackers {
		result := <-results
		if result.err != nil {
			if !trackerSuccess {
				trackerErr = result.err
			}
			continue
		}

		trackerSuccess = true
		trackerErr = nil
		if result.interval > 0 && (interval == 0 || result.interval < interval) {
			interval = result.interval
		}
		trackerSeeders = max(trackerSeeders, result.complete)
		trackerLeechers = max(trackerLeechers, result.incomplete)
		for _, p := range result.peers {
			if p.Port == 0 || p.IP == nil || p.IP.IsUnspecified() {
				continue
			}
			peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
			if seenPeers[peerAddr] {
				continue
			}
			seenPeers[peerAddr] = true
			peers = append(peers, p)
		}
	}

	s.mu.Lock()
	s.lastTrackerErr = trackerErr
	if trackerSuccess {
		s.trackerSeeders = trackerSeeders
		s.trackerLeechers = trackerLeechers
	}
	if trackerErr != nil {
		if event != "" {
			s.trackerEvents = append([]string{event}, s.trackerEvents...)
			if event == "stopped" {
				s.stoppedAnnounced = false
			}
		}
	} else {
		if event == "stopped" {
			s.stoppedAnnounced = true
		} else if event == "started" {
			s.stoppedAnnounced = false
		}
	}
	s.mu.Unlock()

	if trackerErr != nil {
		return 0
	}

	if paused {
		return interval
	}

	// Connect to new peers. The outbound semaphore in connectToPeer is the hard cap on
	// concurrent connections; this loop additionally bounds how many new dials we start
	// in one announce cycle so a tracker returning a huge peer list can't spawn a
	// goroutine storm. slotsHeld is snapshotted once before the loop (len() is a safe,
	// lock-free read, 0 for nil test sessions) so goroutines that acquire a slot mid-loop
	// are not double-counted against launched — double-counting previously throttled
	// connection ramp-up under load.
	slotsHeld := len(s.outboundSlots)
	launched := 0
	for _, p := range peers {
		if p.Port == 0 || p.IP == nil || p.IP.IsUnspecified() {
			continue
		}
		peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
		s.mu.Lock()
		if s.closed || s.paused {
			s.mu.Unlock()
			break
		}
		if slotsHeld+launched >= maxOutboundPeers {
			s.mu.Unlock()
			break
		}
		pState, exists := s.Peers[peerAddr]
		shouldDial := false
		if !exists {
			shouldDial = true
		} else {
			// A tracker response is authoritative evidence that this endpoint is
			// dialable, even if the same address was first seen as an inbound peer.
			pState.Dialable = true
			if !pState.Active && !pState.Dialing && time.Since(pState.LastAttempt) > peerRedialBackoff {
				shouldDial = true
			}
		}
		if shouldDial {
			if !exists {
				s.prunePeersLocked()
				s.Peers[peerAddr] = &PeerState{
					IP:          p.IP.String(),
					Port:        p.Port,
					Choked:      true,
					Active:      false,
					AmChoking:   true,
					LastAttempt: time.Now(),
					Dialable:    true,
					Dialing:     true,
				}
			} else {
				s.Peers[peerAddr].LastAttempt = time.Now()
				s.Peers[peerAddr].Dialing = true
			}
			s.wg.Add(1)
			launched++
			go func(tp tracker.Peer) {
				defer s.wg.Done()
				s.connectToPeer(tp)
			}(p)
		}
		s.mu.Unlock()
	}

	return interval
}

// TrackerSwarmStats returns the largest seed/leecher counts from the latest
// successful tracker announce cycle.
func (s *Session) TrackerSwarmStats() (seeders, leechers int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trackerSeeders, s.trackerLeechers
}

func (s *Session) queueTrackerEventLocked(event string) {
	if len(s.trackerEvents) > 0 && s.trackerEvents[len(s.trackerEvents)-1] == event {
		return
	}
	s.trackerEvents = append(s.trackerEvents, event)
}

func (s *Session) announceStopped() {
	s.mu.Lock()
	if s.stoppedAnnounced {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// Short timeout (2s) on a background context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if s.announceWithEvent(ctx, "stopped") {
		s.mu.Lock()
		s.stoppedAnnounced = true
		s.mu.Unlock()
	}
}

func (s *Session) announceWithEvent(ctx context.Context, event string) bool {
	trackers := s.Torrent.Trackers
	if len(trackers) == 0 {
		return true // No trackers configured, counts as success
	}

	s.mu.RLock()
	var downloaded, left int64
	if s.metadataMode || s.Storage == nil || len(s.PieceStates) == 0 {
		left = 1
	} else {
		stats := s.completionStatsLocked()
		downloaded = stats.completedTotalBytes
		left = stats.totalBytes - stats.completedTotalBytes
		if left < 0 {
			left = 0
		}
	}
	port := s.Port
	uploaded := s.Uploaded.Load()
	s.mu.RUnlock()

	success := false
	for _, tr := range trackers {
		if bytes.HasPrefix([]byte(tr), []byte("udp")) {
			_, err := tracker.UDPAnnounce(ctx, tr, s.Torrent.InfoHash, s.PeerID, port, uploaded, downloaded, left, event, trackerDefaultNumWant)
			if err == nil {
				success = true
			}
		} else if bytes.HasPrefix([]byte(tr), []byte("http")) {
			u, err := tracker.BuildTrackerURL(tr, s.Torrent.InfoHash, s.PeerID, port, uploaded, downloaded, left, true, event, trackerDefaultNumWant)
			if err != nil {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
			if err != nil {
				continue
			}
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				success = true
			}
		}
	}
	return success
}
