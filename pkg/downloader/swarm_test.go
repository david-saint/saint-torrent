package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// threePieces builds three distinct 16-byte payloads for a 3-piece test torrent.
func threePieces() [][]byte {
	return [][]byte{
		[]byte("piece-zero-block"),
		[]byte("piece-one--block"),
		[]byte("piece-two--block"),
	}
}

// --- #7: rarest-first piece selection ---

// TestRarestFirstSelection proves the picker, among equal-priority pieces, prefers the
// rarest (lowest swarm availability) and falls back to lowest index on a tie.
func TestRarestFirstSelection(t *testing.T) {
	sess := newPieceTestSession(t, 16, threePieces())

	setAvail := func(avail ...int) {
		sess.mu.Lock()
		for i, a := range avail {
			sess.setPieceAvailabilityLocked(i, a)
		}
		sess.mu.Unlock()
	}
	pick := func() int {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.selectNeededPieceLocked(func(int64) bool { return true })
	}

	// Piece 1 is rarest -> chosen despite not being lowest index.
	setAvail(3, 1, 2)
	if got := pick(); got != 1 {
		t.Fatalf("expected rarest piece 1, got %d", got)
	}

	// Piece 2 is now rarest.
	setAvail(2, 2, 1)
	if got := pick(); got != 2 {
		t.Fatalf("expected rarest piece 2, got %d", got)
	}

	// Equal availability -> lowest index wins (preserves the old tie-break).
	setAvail(1, 1, 1)
	if got := pick(); got != 0 {
		t.Fatalf("expected lowest-index piece 0 on availability tie, got %d", got)
	}

	// A peer that lacks the rarest piece must still be offered one it has.
	setAvail(5, 1, 5)
	sess.mu.Lock()
	got := sess.selectNeededPieceLocked(func(idx int64) bool { return idx != 1 })
	sess.mu.Unlock()
	if got != 0 {
		t.Fatalf("expected piece 0 when peer lacks the rarest piece, got %d", got)
	}
}

// TestPieceAvailabilityTracking proves Have/bitfield/disconnect bookkeeping keeps the
// per-piece availability counts exact (and never lets a count go negative).
func TestPieceAvailabilityTracking(t *testing.T) {
	// Four pieces so a peer bitfield is exactly one byte.
	sess := newPieceTestSession(t, 16, [][]byte{
		[]byte("p0--------------"),
		[]byte("p1--------------"),
		[]byte("p2--------------"),
		[]byte("p3--------------"),
	})

	avail := func() []int {
		sess.mu.RLock()
		defer sess.mu.RUnlock()
		return append([]int(nil), sess.pieceAvailability...)
	}
	wantAvail := func(want ...int) {
		t.Helper()
		got := avail()
		for i, w := range want {
			if got[i] != w {
				t.Fatalf("availability=%v want %v", got, want)
			}
		}
	}

	// Two Haves for piece 1 from two peers.
	sess.addPieceAvailability(1)
	sess.addPieceAvailability(1)
	wantAvail(0, 2, 0, 0)

	// A peer advertising pieces 0, 2, 3 (bits 0,2,3 set MSB-first -> 0xB0).
	sess.applyBitfieldAvailability(nil, []byte{0xB0})
	wantAvail(1, 2, 1, 1)

	// That peer disconnects: its bitfield contribution is removed.
	sess.removePeerAvailability([]byte{0xB0})
	wantAvail(0, 2, 0, 0)

	// The two Have peers disconnect (each had only piece 1 set -> 0x40).
	sess.removePeerAvailability([]byte{0x40})
	sess.removePeerAvailability([]byte{0x40})
	wantAvail(0, 0, 0, 0)

	// Underflow is clamped, never negative.
	sess.removePeerAvailability([]byte{0xF0})
	wantAvail(0, 0, 0, 0)
}

func TestAvailabilityUpdatesMoveNeededBuckets(t *testing.T) {
	sess := newPieceTestSession(t, 16, threePieces())
	pick := func() int {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.selectNeededPieceLocked(func(int64) bool { return true })
	}

	// Initially all pieces are equally available, so lowest index wins.
	if got := pick(); got != 0 {
		t.Fatalf("expected initial lowest-index piece 0, got %d", got)
	}

	// A bitfield for pieces 0 and 2 makes piece 1 rarest, and the bucket index must
	// reflect that without a full rebuild.
	sess.applyBitfieldAvailability(nil, []byte{0xA0})
	if got := pick(); got != 1 {
		t.Fatalf("expected bitfield update to make piece 1 rarest, got %d", got)
	}

	// A Have for piece 1 ties availability again, restoring the lowest-index tie
	// break through another incremental bucket move.
	sess.addPieceAvailability(1)
	if got := pick(); got != 0 {
		t.Fatalf("expected Have update to restore lowest-index piece 0, got %d", got)
	}

	// Bias piece 0 upward so the disconnect path must move piece 2 into the rarest
	// bucket; otherwise piece 0 could win by tie-break even if disconnect did nothing.
	sess.addPieceAvailability(0)
	if got := pick(); got != 1 {
		t.Fatalf("expected second Have update to make piece 1 the rarest tie winner, got %d", got)
	}

	// Removing the bitfield contribution leaves piece 2 uniquely rarest.
	sess.removePeerAvailability([]byte{0xA0})
	if got := pick(); got != 2 {
		t.Fatalf("expected disconnect update to move piece 2 into the rarest bucket, got %d", got)
	}
}

// TestApplyBitfieldAvailabilityDelta proves a peer that re-sends or shrinks its
// advertised bitfield is folded as a delta rather than double-counted.
func TestApplyBitfieldAvailabilityDelta(t *testing.T) {
	sess := newPieceTestSession(t, 16, [][]byte{
		[]byte("p0--------------"),
		[]byte("p1--------------"),
		[]byte("p2--------------"),
		[]byte("p3--------------"),
	})

	// First bitfield: pieces 0,1 (0xC0).
	sess.applyBitfieldAvailability(nil, []byte{0xC0})
	// Re-sent bitfield now also has piece 3 and drops piece 0 (0x50 = bits 1,3).
	sess.applyBitfieldAvailability([]byte{0xC0}, []byte{0x50})

	sess.mu.RLock()
	got := append([]int(nil), sess.pieceAvailability...)
	sess.mu.RUnlock()
	if want := []int{0, 1, 0, 1}; got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("availability=%v want %v", got, want)
	}
}

// --- #8: endgame mode ---

// TestEndgameActiveAndSelection proves endgame turns on once no empty wanted piece is
// left to claim, and that selectEndgamePieceLocked then offers the rarest in-progress
// piece the peer has and does not already own.
func TestEndgameActiveAndSelection(t *testing.T) {
	sess := newPieceTestSession(t, 16, threePieces())

	// Fresh torrent: pieces still empty -> not endgame.
	sess.mu.RLock()
	if sess.endgameActiveLocked() {
		t.Fatal("endgame should be inactive while empty pieces remain")
	}
	sess.mu.RUnlock()

	// Claim every piece (PieceDownloading) so the needed set drains -> endgame.
	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.PieceStates[i] = PieceDownloading
	}
	sess.recomputeNeededLocked()
	for i, availability := range []int{3, 1, 2} {
		sess.setPieceAvailabilityLocked(i, availability)
	}
	if !sess.endgameActiveLocked() {
		t.Fatal("endgame should be active once all pieces are claimed")
	}

	// Rarest in-progress piece the peer has, none owned -> piece 1.
	if got := sess.selectEndgamePieceLocked(func(int64) bool { return true }, nil); got != 1 {
		t.Fatalf("expected rarest in-progress piece 1, got %d", got)
	}

	// Owning piece 1 already -> next rarest is piece 2.
	if got := sess.selectEndgamePieceLocked(func(int64) bool { return true }, map[int64]bool{1: true}); got != 2 {
		t.Fatalf("expected next rarest in-progress piece 2, got %d", got)
	}

	// A peer that only has piece 0 gets piece 0.
	if got := sess.selectEndgamePieceLocked(func(idx int64) bool { return idx == 0 }, nil); got != 0 {
		t.Fatalf("expected piece 0 for a peer that only has it, got %d", got)
	}

	// No PieceDownloading candidates -> -1.
	sess.setPieceStateLocked(0, PieceCompleted)
	sess.setPieceStateLocked(1, PieceCompleted)
	sess.setPieceStateLocked(2, PieceCompleted)
	if got := sess.selectEndgamePieceLocked(func(int64) bool { return true }, nil); got != -1 {
		t.Fatalf("expected no endgame candidate when nothing is downloading, got %d", got)
	}
	sess.mu.Unlock()
}

// TestProcessCompletedPieceSkipsAlreadyCompleted proves the endgame redundant-copy
// guard: a second completion for an already-finished piece is dropped before the hash
// check, so even bad data neither resets the piece nor records an error.
func TestProcessCompletedPieceSkipsAlreadyCompleted(t *testing.T) {
	data := []byte("the one true piece payload for slot 0!!!")
	sess := newPieceTestSession(t, int64(len(data)), [][]byte{data})

	// First (winning) copy completes the piece.
	sess.mu.Lock()
	sess.PieceStates[0] = PieceDownloading
	sess.removeNeededLocked(0)
	sess.mu.Unlock()
	sess.processCompletedPiece(pieceWriteJob{index: 0, hash: sha1.Sum(data), data: data})
	if got := sess.GetPieceStates()[0]; got != PieceCompleted {
		t.Fatalf("expected piece 0 completed by the first copy, got %v", got)
	}

	// A redundant second copy arrives with WRONG bytes. The already-done guard must
	// short-circuit before hashing, so the piece stays complete and no error is set.
	sess.processCompletedPiece(pieceWriteJob{index: 0, hash: sha1.Sum(data), data: []byte("garbage that does not match the hash!!!!")})
	if got := sess.GetPieceStates()[0]; got != PieceCompleted {
		t.Fatalf("redundant copy must not disturb the completed piece, got %v", got)
	}
	if err := sess.LastError(); err != nil {
		t.Fatalf("redundant copy must not record an error, got %v", err)
	}
}

// TestEndgameRedundantFetchAndCancel is an end-to-end proof of #8: once a slow peer has
// claimed the only piece and stalled, a second peer must be able to fetch that same
// piece (redundantly) so the download still completes, and the stalled peer must be sent
// a Cancel for its now-redundant outstanding block.
func TestEndgameRedundantFetchAndCancel(t *testing.T) {
	const pieceLen = BlockSize // single-block piece
	data := make([]byte, pieceLen)
	for i := range data {
		data[i] = byte(i*5 + 1)
	}
	tor := &torrent.Torrent{
		Name:        "endgame.bin",
		PieceLength: int64(pieceLen),
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(pieceLen), Path: []string{"endgame.bin"}}},
	}

	// handshakeAndBitfield performs the server side of a handshake and advertises the
	// single piece (bit 0 -> 0x80).
	handshakeAndBitfield := func(conn net.Conn, peerID string) bool {
		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return false
		}
		var pid [20]byte
		copy(pid[:], peerID)
		if _, err := conn.Write((&peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: hs.InfoHash, PeerID: pid}).Serialize()); err != nil {
			return false
		}
		_, err = conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}}).Serialize())
		return err == nil
	}

	// Slow peer: unchokes, accepts the block request (claiming the piece), then withholds
	// data while nudging the client with periodic Unchokes so its loop keeps turning. It
	// records whether it receives a Cancel for the piece.
	var slowGotCancel atomic.Bool
	var slowClaimed atomic.Bool
	slowLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen slow: %v", err)
	}
	defer slowLn.Close()
	go func() {
		conn, err := slowLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if !handshakeAndBitfield(conn, "-SLOW01-endgametest!") {
			return
		}
		nudge := false
		for {
			_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					if nudge {
						// Keep the client's read loop iterating so it can notice the
						// piece was completed elsewhere and send us a Cancel.
						_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
					}
					continue
				}
				return
			}
			if msg == nil {
				continue
			}
			switch msg.ID {
			case peer.MsgInterested:
				_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
			case peer.MsgRequest:
				// Claim observed; deliberately withhold the block data.
				slowClaimed.Store(true)
				nudge = true
			case peer.MsgCancel:
				slowGotCancel.Store(true)
			}
		}
	}()

	// Fast peer: unchokes and serves the block in full.
	fastLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fast: %v", err)
	}
	defer fastLn.Close()
	go func() {
		conn, err := fastLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if !handshakeAndBitfield(conn, "-FAST01-endgametest!") {
			return
		}
		for {
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			switch msg.ID {
			case peer.MsgInterested:
				_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
			case peer.MsgRequest:
				if len(msg.Payload) != 12 {
					continue
				}
				index := binary.BigEndian.Uint32(msg.Payload[0:4])
				begin := binary.BigEndian.Uint32(msg.Payload[4:8])
				length := binary.BigEndian.Uint32(msg.Payload[8:12])
				block := data[begin : begin+length]
				payload := make([]byte, 8+len(block))
				binary.BigEndian.PutUint32(payload[0:4], index)
				binary.BigEndian.PutUint32(payload[4:8], begin)
				copy(payload[8:], block)
				_, _ = conn.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize())
			}
		}
	}()

	dir := t.TempDir()
	st, err := storage.NewStorage(dir, []storage.FileInfo{{Path: "endgame.bin", Length: int64(pieceLen)}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	var pid [20]byte
	copy(pid[:], "-ST0001-endgame1234")
	sess, err := NewSession(tor, st, pid, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	sess.Start()
	defer sess.Close()

	_, slowPort, _ := net.SplitHostPort(slowLn.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + slowPort)

	// Wait until the slow peer has claimed the only piece (it is now PieceDownloading).
	claimed := false
	for i := 0; i < 200; i++ {
		if slowClaimed.Load() && sess.GetPieceStates()[0] == PieceDownloading {
			claimed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !claimed {
		t.Fatal("slow peer never claimed the piece; cannot exercise endgame")
	}

	sess.mu.Lock()
	if err := sess.validateDownloadingIndexLocked(); err != nil {
		sess.mu.Unlock()
		t.Fatalf("Invariant validation failed during download: %v", err)
	}
	sess.mu.Unlock()

	// Only now bring in the fast peer; it can only get the piece via endgame duplication.
	_, fastPort, _ := net.SplitHostPort(fastLn.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + fastPort)

	completed := false
	for i := 0; i < 200; i++ {
		if sess.PercentComplete() == 100.0 {
			completed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !completed {
		t.Fatalf("endgame redundant fetch did not complete the download; %.1f%% done", sess.PercentComplete())
	}

	sess.mu.Lock()
	if err := sess.validateDownloadingIndexLocked(); err != nil {
		sess.mu.Unlock()
		t.Fatalf("Invariant validation failed after completion: %v", err)
	}
	sess.mu.Unlock()

	cancelled := false
	for i := 0; i < 200; i++ {
		if slowGotCancel.Load() {
			cancelled = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !cancelled {
		t.Fatal("stalled peer was never sent a Cancel after the piece completed elsewhere")
	}
}

// TestProcessCompletedPieceEndgameHashFailurePreservesOwner proves that a corrupt
// redundant endgame copy does not reset the owning peer's PieceDownloading state,
// while still closing the bad connection, recording a strike, and reporting failure.
func TestProcessCompletedPieceEndgameHashFailurePreservesOwner(t *testing.T) {
	data := []byte("the one true piece payload for slot 0!!!")
	sess := newPieceTestSession(t, int64(len(data)), [][]byte{data})

	sess.mu.Lock()
	sess.setPieceStateLocked(0, PieceDownloading)
	sess.mu.Unlock()

	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	src := &pieceSource{host: "198.51.100.42"}
	resCh := make(chan pieceWriteResult, 1)

	sess.processCompletedPiece(pieceWriteJob{
		index:   0,
		hash:    sha1.Sum(data),
		data:    []byte("garbage that does not match the hash!!!!"),
		conn:    clientConn,
		source:  src,
		result:  resCh,
		endgame: true,
	})

	if got := sess.GetPieceStates()[0]; got != PieceDownloading {
		t.Fatalf("corrupt endgame copy reset piece 0 state to %v, want PieceDownloading", got)
	}
	res := <-resCh
	if res.status != pieceWriteHashFailed {
		t.Fatalf("result status = %v, want pieceWriteHashFailed", res.status)
	}
	// Connection must be closed by processCompletedPiece.
	_ = serverConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	var buf [1]byte
	if _, err := serverConn.Read(buf[:]); err == nil {
		t.Fatal("expected corrupt endgame peer connection to be closed")
	}
	sess.mu.Lock()
	strikes := sess.admission.hostStrikesLocked(src.host, time.Now()).count
	sess.mu.Unlock()
	if strikes != 1 || !src.struck {
		t.Fatalf("expected 1 hash-failure strike for %s (struck=%v), got %d", src.host, src.struck, strikes)
	}
}

// TestEndgameCorruptDuplicateDoesNotResetOwner is an end-to-end proof for #93:
// when peer A owns a piece (PieceDownloading) and peer B sends a corrupt redundant
// endgame copy, peer B is disconnected, piece 0 stays PieceDownloading, and peer A
// neither receives a Cancel nor re-requests block 0.
func TestEndgameCorruptDuplicateDoesNotResetOwner(t *testing.T) {
	const pieceLen = 2 * BlockSize
	data := make([]byte, pieceLen)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	tor := &torrent.Torrent{
		Name:        "endgame-corrupt.bin",
		PieceLength: int64(pieceLen),
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(pieceLen), Path: []string{"endgame-corrupt.bin"}}},
	}

	handshakeAndBitfield := func(conn net.Conn, peerID string) bool {
		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return false
		}
		var pid [20]byte
		copy(pid[:], peerID)
		if _, err := conn.Write((&peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: hs.InfoHash, PeerID: pid}).Serialize()); err != nil {
			return false
		}
		_, err = conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}}).Serialize())
		return err == nil
	}

	sendBlock := func(conn net.Conn, index, begin uint32, block []byte) error {
		payload := make([]byte, 8+len(block))
		binary.BigEndian.PutUint32(payload[0:4], index)
		binary.BigEndian.PutUint32(payload[4:8], begin)
		copy(payload[8:], block)
		_, err := conn.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize())
		return err
	}

	var ownerClaimed atomic.Bool
	var ownerGotCancel atomic.Bool
	var ownerBlock0Count atomic.Int32
	releaseBlock1 := make(chan struct{})

	ownerLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen owner: %v", err)
	}
	defer ownerLn.Close()
	go func() {
		conn, err := ownerLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if !handshakeAndBitfield(conn, "-OWNR01-endgametest!") {
			return
		}
		heldBlock1 := false
		for {
			_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					select {
					case <-releaseBlock1:
						if heldBlock1 {
							heldBlock1 = false
							_ = sendBlock(conn, 0, BlockSize, data[BlockSize:])
						}
					default:
						// Nudge the client's loop so dropCompletedElsewhere would run if
						// PieceStates[0] had been erroneously reset.
						_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
					}
					continue
				}
				return
			}
			if msg == nil {
				continue
			}
			switch msg.ID {
			case peer.MsgInterested:
				_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
			case peer.MsgRequest:
				if len(msg.Payload) != 12 {
					continue
				}
				index := binary.BigEndian.Uint32(msg.Payload[0:4])
				begin := binary.BigEndian.Uint32(msg.Payload[4:8])
				length := binary.BigEndian.Uint32(msg.Payload[8:12])
				if begin == 0 {
					ownerBlock0Count.Add(1)
					ownerClaimed.Store(true)
					_ = sendBlock(conn, index, begin, data[begin:begin+length])
				} else {
					heldBlock1 = true
				}
			case peer.MsgCancel:
				ownerGotCancel.Store(true)
			}
		}
	}()

	var badServedAll atomic.Bool
	var badClosed atomic.Bool
	badLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen bad: %v", err)
	}
	defer badLn.Close()
	go func() {
		conn, err := badLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if !handshakeAndBitfield(conn, "-BAD001-endgametest!") {
			return
		}
		served := 0
		for {
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				if served >= 2 {
					badClosed.Store(true)
				}
				return
			}
			if msg == nil {
				continue
			}
			switch msg.ID {
			case peer.MsgInterested:
				_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
			case peer.MsgRequest:
				if len(msg.Payload) != 12 {
					continue
				}
				index := binary.BigEndian.Uint32(msg.Payload[0:4])
				begin := binary.BigEndian.Uint32(msg.Payload[4:8])
				length := binary.BigEndian.Uint32(msg.Payload[8:12])
				corrupt := make([]byte, length)
				for i := range corrupt {
					corrupt[i] = data[int(begin)+i] ^ 0xff
				}
				_ = sendBlock(conn, index, begin, corrupt)
				served++
				if served == 2 {
					badServedAll.Store(true)
				}
			}
		}
	}()

	dir := t.TempDir()
	st, err := storage.NewStorage(dir, []storage.FileInfo{{Path: "endgame-corrupt.bin", Length: int64(pieceLen)}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer st.Close()
	var pid [20]byte
	copy(pid[:], "-ST0001-endgame1234")
	sess, err := NewSession(tor, st, pid, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	sess.Start()
	defer sess.Close()

	_, ownerPort, _ := net.SplitHostPort(ownerLn.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + ownerPort)

	claimed := false
	for i := 0; i < 200; i++ {
		if ownerClaimed.Load() && sess.GetPieceStates()[0] == PieceDownloading {
			claimed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !claimed {
		t.Fatal("owner peer never claimed piece 0")
	}

	_, badPort, _ := net.SplitHostPort(badLn.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + badPort)

	// Wait until the bad peer has served its corrupt copy and been disconnected.
	for i := 0; i < 200; i++ {
		if badServedAll.Load() && badClosed.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !badServedAll.Load() || !badClosed.Load() {
		t.Fatalf("bad peer did not complete corrupt endgame cycle (served=%v closed=%v)", badServedAll.Load(), badClosed.Load())
	}

	// Give the owner peer's loop a couple of nudge cycles to react if it were going to.
	time.Sleep(150 * time.Millisecond)

	if got := sess.GetPieceStates()[0]; got != PieceDownloading {
		t.Fatalf("piece 0 state after corrupt endgame copy = %v, want PieceDownloading", got)
	}
	if ownerGotCancel.Load() {
		t.Fatal("owner peer received Cancel after corrupt endgame copy")
	}
	if count := ownerBlock0Count.Load(); count != 1 {
		t.Fatalf("owner peer block 0 request count = %d, want 1", count)
	}

	// Release the owner's remaining block and verify the download completes cleanly.
	close(releaseBlock1)
	completed := false
	for i := 0; i < 200; i++ {
		if sess.PercentComplete() == 100.0 {
			completed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !completed {
		t.Fatalf("download did not complete after releasing owner block 1 (%.1f%%)", sess.PercentComplete())
	}
}
