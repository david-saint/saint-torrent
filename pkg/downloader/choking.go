package downloader

import (
	"sainttorrent/pkg/peer"
	"sort"
	"time"
)

func (s *Session) chokeLoop() {
	defer s.wg.Done()
	s.mu.Lock()
	s.chokeTimer = time.NewTicker(10 * time.Second)
	s.optimisticTimer = time.NewTicker(30 * time.Second)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.chokeTimer != nil {
			s.chokeTimer.Stop()
		}
		if s.optimisticTimer != nil {
			s.optimisticTimer.Stop()
		}
		s.mu.Unlock()
	}()

	var optimisticPeer string

	for {
		select {
		case <-s.chokeTimer.C:
			s.recalculateChoking(&optimisticPeer)
		case <-s.optimisticTimer.C:
			s.recalculateOptimistic(&optimisticPeer)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Session) recalculateChoking(optimisticPeer *string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.paused {
		return
	}

	// 1. Determine if we are seeding wanted content or still downloading it.
	stats := s.completionStatsLocked()
	isSeeding := stats.wantedBytes == 0 || (stats.wantedPieces > 0 && stats.completedWantedPieces == stats.wantedPieces)

	// 2. Collect all active peers that are interested in us
	type peerCandidate struct {
		addr   string
		speed  float64
		client *peer.Client
	}
	var candidates []peerCandidate

	for addr, pState := range s.Peers {
		if !pState.Active || !pState.Interested {
			continue
		}
		client, activeExists := s.activePeers[addr]
		if !activeExists {
			continue
		}

		var speed float64
		if isSeeding {
			speed = pState.UploadSpeed
		} else {
			speed = pState.DownloadSpeed
		}

		candidates = append(candidates, peerCandidate{
			addr:   addr,
			speed:  speed,
			client: client,
		})
	}

	// 3. Sort candidates (descending speed)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].speed > candidates[j].speed
	})

	// 4. Select top 4 peers to unchoke
	unchokedCount := 0
	topPeers := make(map[string]bool)

	for _, cand := range candidates {
		if unchokedCount < 4 {
			topPeers[cand.addr] = true
			unchokedCount++
			// Unchoke if choking
			pState := s.Peers[cand.addr]
			if pState.AmChoking {
				pState.AmChoking = false
				s.sendPeerControlLocked(cand.client, (*peer.Client).SendUnchoke)
			}
		} else {
			break
		}
	}

	// 5. Choke everyone else (except the optimistic peer, if set and interested)
	for addr, pState := range s.Peers {
		if !pState.Active {
			continue
		}
		client, activeExists := s.activePeers[addr]
		if !activeExists {
			continue
		}

		// Skip top unchoked peers
		if topPeers[addr] {
			continue
		}

		// Skip the optimistic peer
		if *optimisticPeer != "" && addr == *optimisticPeer && pState.Interested {
			// Keep optimistic peer unchoked and account it as one upload slot.
			if pState.AmChoking && unchokedCount < 4 {
				pState.AmChoking = false
				topPeers[addr] = true
				unchokedCount++
				s.sendPeerControlLocked(client, (*peer.Client).SendUnchoke)
			} else if !pState.AmChoking {
				topPeers[addr] = true
			}
			continue
		}

		// Otherwise, choke
		if !pState.AmChoking {
			pState.AmChoking = true
			s.sendPeerControlLocked(client, (*peer.Client).SendChoke)
		}
	}
}

func (s *Session) recalculateOptimistic(optimisticPeer *string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.paused {
		return
	}

	// Gather candidates for optimistic unchoke:
	// They must be active, interested in us, and currently choked by us
	var candidates []string
	for addr, pState := range s.Peers {
		if pState.Active && pState.Interested && pState.AmChoking {
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return
	}

	// Pick a random candidate using time
	idx := time.Now().UnixNano() % int64(len(candidates))
	chosenAddr := candidates[idx]

	*optimisticPeer = chosenAddr

	pState := s.Peers[chosenAddr]
	pState.AmChoking = false
	if client, ok := s.activePeers[chosenAddr]; ok {
		s.sendPeerControlLocked(client, (*peer.Client).SendUnchoke)
	}
}
