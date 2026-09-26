package downloader

import (
	"encoding/binary"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// withholdingPeer is the remote end of a runPeerMessageLoop connection that
// serves requests through serve (which returns false to withhold a block; remote
// is the peer's end of the connection) and records which pieces it was asked for.
type withholdingPeer struct {
	remote net.Conn
	done   chan struct{}

	mu        sync.Mutex
	requested map[uint32]bool
	lastReq   time.Time
}

func startWithholdingPeer(t *testing.T, sess *Session, port uint16, hello []*peer.Message, serve func(remote net.Conn, index, begin uint32) bool) *withholdingPeer {
	t.Helper()
	local, remote := net.Pipe()
	client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
	p := &withholdingPeer{remote: remote, done: make(chan struct{}), requested: make(map[uint32]bool)}
	go func() {
		sess.runPeerMessageLoop(client, local, "127.0.0.1:"+strconv.Itoa(int(port)), "127.0.0.1", port, fastReserved(), false)
		close(p.done)
	}()
	requests := make(chan []byte, 4096)
	go func() {
		defer close(requests)
		for {
			msg, err := peer.ParseMessage(remote)
			if err != nil {
				return
			}
			if msg != nil && msg.ID == peer.MsgRequest {
				p.mu.Lock()
				p.requested[binary.BigEndian.Uint32(msg.Payload[0:4])] = true
				p.lastReq = time.Now()
				p.mu.Unlock()
				requests <- msg.Payload
			}
		}
	}()
	go func() {
		for _, m := range hello {
			if _, err := remote.Write(m.Serialize()); err != nil {
				return
			}
		}
		for req := range requests {
			index := binary.BigEndian.Uint32(req[0:4])
			begin := binary.BigEndian.Uint32(req[4:8])
			if serve == nil || !serve(remote, index, begin) {
				continue
			}
			length := binary.BigEndian.Uint32(req[8:12])
			payload := make([]byte, 8+length)
			copy(payload, req[0:8])
			if _, err := remote.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize()); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = remote.Close()
		<-p.done
	})
	return p
}

// requestedPieces waits until the loop has stopped asking for more, then returns
// how many distinct pieces it requested.
func (p *withholdingPeer) requestedPieces(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		p.mu.Lock()
		quiet := !p.lastReq.IsZero() && time.Since(p.lastReq) > 300*time.Millisecond
		n := len(p.requested)
		p.mu.Unlock()
		if quiet {
			return n
		}
	}
	t.Fatal("the loop never stopped requesting")
	return 0
}

// A peer that serves every block of a piece but the last used to get 16 pieces
// opened on its connection, all held in memory until the missing blocks came:
// 256 MiB with 16 MiB pieces. Open pieces are now bounded by bytes as well.
func TestOpenPieceBytesPerConnectionAreCapped(t *testing.T) {
	const pieceLen = 64 * 1024 // 4 blocks
	// Restored by Cleanup, after the peer loop has exited.
	t.Cleanup(swapInt64(&peerOpenPieceBytesFloor, 4*pieceLen))
	sess := settled(newWireTestSession(t, 32, pieceLen))
	hello := []*peer.Message{{ID: peer.MsgHaveAll}, {ID: peer.MsgUnchoke}}
	p := startWithholdingPeer(t, sess, 8101, hello, func(_ net.Conn, index, begin uint32) bool {
		return begin != pieceLen-BlockSize // withhold each piece's last block
	})
	if n := p.requestedPieces(t); n != 4 {
		t.Fatalf("the loop opened %d pieces on a withholding connection, want 4 (the byte cap)", n)
	}
}

// endgameOnlySession is a session whose every piece is claimed by other
// peers, so a new connection can only take redundant endgame copies.
func endgameOnlySession(t *testing.T, numPieces int, pieceLen int64) *Session {
	t.Helper()
	sess := settled(newWireTestSession(t, numPieces, pieceLen))
	sess.mu.Lock()
	for i := range sess.PieceStates {
		sess.setPieceStateLocked(i, PieceDownloading) // claimed by other peers: endgame
	}
	sess.mu.Unlock()
	return sess
}

// Endgame copies are whole pieces fetched from block 0. A connection takes as
// many as its request window can fill: a flat two per connection held a fast
// peer to two pieces per round trip while slow peers sat on the rest, and made
// the endgame tail up to 3.4 times slower. It never takes more than its window.
func TestEndgameCopiesPerConnectionFollowTheWindow(t *testing.T) {
	const pieceLen = 64 * 1024 // 4 blocks
	sess := endgameOnlySession(t, 32, pieceLen)
	hello := []*peer.Message{{ID: peer.MsgHaveAll}, {ID: peer.MsgUnchoke}}
	p := startWithholdingPeer(t, sess, 8102, hello, nil)
	// The peer serves nothing, so the window stays at its initial size.
	windowPieces := divCeil(dynamicPipelineInitialWindowBlocks, pieceLen/BlockSize)
	if n := p.requestedPieces(t); n <= minEndgamePiecesPerPeer || n > windowPieces {
		t.Fatalf("the loop took %d endgame copies on one connection, want more than %d and at most the %d its window fills",
			n, minEndgamePiecesPerPeer, windowPieces)
	}
}

// Endgame copies count against a connection's open piece bytes like any open
// piece, so a withholding peer pins no more memory in endgame than outside it.
func TestEndgameCopiesPerConnectionAreByteCapped(t *testing.T) {
	const pieceLen = 64 * 1024 // 4 blocks
	// Restored by Cleanup, after the peer loop has exited.
	t.Cleanup(swapInt64(&peerOpenPieceBytesFloor, 4*pieceLen))
	sess := endgameOnlySession(t, 32, pieceLen)
	hello := []*peer.Message{{ID: peer.MsgHaveAll}, {ID: peer.MsgUnchoke}}
	p := startWithholdingPeer(t, sess, 8105, hello, nil)
	if n := p.requestedPieces(t); n != 4 {
		t.Fatalf("the loop took %d endgame copies on one connection, want 4 (the byte cap)", n)
	}
}

// A fast peer can keep a piece it granted as allowed_fast open across a choke,
// then withhold one of its blocks. The block timed out and was parked for a
// re-send that never happened while choked, so its retries never ran out and the
// connection held the piece and its blocks forever. It must be dropped.
func TestChokedAllowedFastWithholderIsDropped(t *testing.T) {
	t.Cleanup(swapDuration(&blockRequestTimeout, 100*time.Millisecond))
	const pieceLen = 32 * 1024 // 2 blocks
	sess := settled(newWireTestSession(t, 8, pieceLen))
	allowed := make([]byte, 4) // piece 0
	hello := []*peer.Message{
		// Piece 0 only: nothing else can complete (and fail its hash check), and the
		// other pieces stay needed, so this is not endgame.
		{ID: peer.MsgBitfield, Payload: []byte{0x80}},
		{ID: peer.MsgAllowedFast, Payload: allowed},
		{ID: peer.MsgUnchoke},
	}
	var choked sync.Once
	p := startWithholdingPeer(t, sess, 8103, hello, func(remote net.Conn, index, begin uint32) bool {
		if index == 0 && begin == BlockSize {
			// Choke once the loop has asked for the block we withhold.
			choked.Do(func() {
				go func() {
					_, _ = remote.Write((&peer.Message{ID: peer.MsgChoke}).Serialize())
					stop := time.After(10 * time.Second)
					for {
						select {
						case <-time.After(20 * time.Millisecond):
							// Keep-alives keep the read deadline fresh, as a real
							// withholder would.
							if _, err := remote.Write((*peer.Message)(nil).Serialize()); err != nil {
								return
							}
						case <-stop:
							return
						}
					}
				}()
			})
			return false
		}
		return true
	})
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("a choked peer withholding an allowed-fast block kept its connection")
	}
}

// An inbound peer that takes our requests and withholds the blocks is dropped
// once the requests run out of retries, which the timeout sweep in pump counts.
// pump runs after each message the loop acts on, but messages it discards skip
// it, so a peer streaming only those (here, Haves for pieces that do not exist)
// used to keep its requests alive, and the pieces behind them claimed, forever.
func TestWithholderStreamingDiscardedMessagesIsDropped(t *testing.T) {
	t.Cleanup(swapDuration(&blockRequestTimeout, 100*time.Millisecond))
	sess := settled(newWireTestSession(t, 8, 32*1024))
	hello := []*peer.Message{{ID: peer.MsgHaveAll}, {ID: peer.MsgUnchoke}}
	var streaming sync.Once
	p := startWithholdingPeer(t, sess, 8104, hello, func(remote net.Conn, _, _ uint32) bool {
		streaming.Do(func() {
			go func() {
				bogus := (&peer.Message{ID: peer.MsgHave, Payload: []byte{0xff, 0xff, 0xff, 0xf0}}).Serialize()
				stop := time.After(10 * time.Second)
				for {
					select {
					case <-time.After(5 * time.Millisecond):
						if _, err := remote.Write(bogus); err != nil {
							return
						}
					case <-stop:
						return
					}
				}
			}()
		})
		return false // withhold every block
	})
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("a peer withholding every block kept its connection by streaming discarded messages")
	}
}

func swapInt64(p *int64, v int64) func() {
	old := *p
	*p = v
	return func() { *p = old }
}
