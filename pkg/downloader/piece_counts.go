package downloader

// PieceCounts tallies a session's piece states.
type PieceCounts struct {
	Total       int
	Empty       int
	Downloading int
	Completed   int
	Unverified  int
	// Unknown counts states outside the PieceState constants.
	Unknown int
}

// PieceCounts counts the session's piece states in one pass under one read
// lock. Unlike GetPieceStates it copies nothing, so a monitoring client (the
// stats API) costs no allocation proportional to the piece count, which can be
// millions.
func (s *Session) PieceCounts() PieceCounts {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var byState [PieceUnverified + 1]int
	unknown := 0
	for _, st := range s.PieceStates {
		if st >= 0 && st < PieceState(len(byState)) {
			byState[st]++
		} else {
			unknown++
		}
	}
	return PieceCounts{
		Total:       len(s.PieceStates),
		Empty:       byState[PieceEmpty],
		Downloading: byState[PieceDownloading],
		Completed:   byState[PieceCompleted],
		Unverified:  byState[PieceUnverified],
		Unknown:     unknown,
	}
}
