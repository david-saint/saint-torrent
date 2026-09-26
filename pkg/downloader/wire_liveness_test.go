package downloader

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/peer"
)

// livenessPeerA1 is the remote end of a runPeerMessageLoop connection over
// net.Pipe that, unlike wirePeer, counts the keep-alives it receives and can run
// the loop as an outbound connection. It drains everything the loop sends.
type livenessPeerA1 struct {
	t          *testing.T
	addr       string
	remote     net.Conn
	keepAlives atomic.Int32
	in         chan *peer.Message
	done       chan struct{}
}

func startLivenessPeerA1(t *testing.T, sess *Session, port uint16, outbound bool) *livenessPeerA1 {
	t.Helper()
	local, remote := net.Pipe()
	client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
	p := &livenessPeerA1{
		t:      t,
		addr:   fmt.Sprintf("127.0.0.1:%d", port),
		remote: remote,
		in:     make(chan *peer.Message, 4096),
		done:   make(chan struct{}),
	}
	go func() {
		sess.runPeerMessageLoop(client, local, p.addr, "127.0.0.1", port, fastReserved(), outbound)
		close(p.done)
	}()
	go func() {
		r := bufio.NewReader(remote)
		for {
			msg, err := peer.ParseMessage(r)
			if err != nil {
				return
			}
			if msg == nil {
				p.keepAlives.Add(1)
				continue
			}
			select {
			case p.in <- msg:
			default: // nobody is looking; never block the loop's writes
			}
		}
	}()
	t.Cleanup(func() {
		_ = remote.Close()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("peer loop did not exit")
		}
	})
	return p
}

// write sends raw bytes and reports whether the loop took them all.
func (p *livenessPeerA1) write(b []byte) bool {
	_ = p.remote.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := p.remote.Write(b)
	return err == nil
}

func (p *livenessPeerA1) closedWithin(d time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

// disconnectReasonsA1 routes the package log to a file and returns a function
// that reports the reason logged when the connection at addr ended ("" if it
// has not). Call it before starting the connection.
func disconnectReasonsA1(t *testing.T) func(addr string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer.log")
	if err := logging.Configure(logging.Config{Path: path, Level: logging.LevelInfo}); err != nil {
		t.Fatalf("configure logging: %v", err)
	}
	t.Cleanup(func() { _ = logging.Close() })
	return func(addr string) string {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open log: %v", err)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var entry struct {
				Event  string         `json:"event"`
				Fields map[string]any `json:"fields"`
			}
			if json.Unmarshal(scanner.Bytes(), &entry) != nil || entry.Event != "peer_disconnected" {
				continue
			}
			if entry.Fields["peer"] == addr {
				reason, _ := entry.Fields["reason"].(string)
				return reason
			}
		}
		return ""
	}
}

// A connection on which we have nothing to say still hears from us: once a
// keep-alive interval passes without a write, the loop sends a keep-alive, so a
// peer that drops silent connections (libtorrent after 120 s) keeps us.
func TestQuietConnectionSendsKeepAlives(t *testing.T) {
	t.Cleanup(swapDuration(&peerKeepAliveInterval, 40*time.Millisecond))
	t.Cleanup(swapDuration(&peerIdleTickInterval, 20*time.Millisecond))
	sess, _ := newSeedingWireTestSession(t, 4, 16*1024)
	p := startLivenessPeerA1(t, settled(sess), 7700, false)

	deadline := time.Now().Add(3 * time.Second)
	for p.keepAlives.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("a quiet connection sent %d keep-alives in 3s", p.keepAlives.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The read deadline used to be 30 s, re-armed per message, so a quiet but honest
// peer (BEP 3 allows two minutes between keep-alives) was dropped. The deadline
// is now the dead-socket backstop, peerReadTimeout: a peer silent for longer than
// the old deadline (scaled) stays, one silent for longer than peerReadTimeout is
// dropped.
func TestQuietPeerOutlivesOldReadDeadline(t *testing.T) {
	t.Cleanup(swapDuration(&peerReadTimeout, 600*time.Millisecond))
	reasons := disconnectReasonsA1(t)
	sess, _ := newSeedingWireTestSession(t, 4, 16*1024)
	p := startLivenessPeerA1(t, settled(sess), 7701, false)

	// The old 30 s deadline is 1/5 of peerReadTimeout: stay silent for twice that.
	if p.closedWithin(2 * peerReadTimeout / 5) {
		t.Fatal("a peer silent for less than peerReadTimeout was dropped")
	}
	if !p.closedWithin(5 * time.Second) {
		t.Fatal("a peer silent for longer than peerReadTimeout was kept")
	}
	if got := reasons(p.addr); got != "read_error" {
		t.Fatalf("disconnect reason %q, want read_error", got)
	}
}

// An outbound peer that sends nothing at all after the handshake is reaped by the
// stall reaper. Before the liveness ticker the loop only ran its checks when a
// message arrived, so a silent peer held its slot until the read deadline.
func TestSilentOutboundPeerIsReapedByTicker(t *testing.T) {
	t.Cleanup(swapDuration(&peerStallTimeout, 200*time.Millisecond))
	t.Cleanup(swapDuration(&peerIdleTickInterval, 20*time.Millisecond))
	reasons := disconnectReasonsA1(t)
	sess := settled(newWireTestSession(t, 4, 16*1024))
	p := startLivenessPeerA1(t, sess, 7702, true)

	if !p.closedWithin(3 * time.Second) {
		t.Fatal("a silent outbound peer was not reaped")
	}
	if got := reasons(p.addr); got != "stalled" {
		t.Fatalf("disconnect reason %q, want stalled", got)
	}
}

// A message whose length its type does not allow drops the peer. One too large
// for a pooled buffer is refused from its header, without reading or allocating
// the payload (only the header is sent here); a small one is read, then refused.
func TestOversizedMessageDisconnects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     byte
		length uint32
	}{
		{"1 MiB unknown id", 30, 1 << 20},
		{"512 KiB unknown id", 30, 512 << 10},
		{"64 KiB have", byte(peer.MsgHave), 64 << 10},
		{"oversized piece", byte(peer.MsgPiece), 9 + BlockSize + 1},
		{"long have", byte(peer.MsgHave), 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reasons := disconnectReasonsA1(t)
			sess := settled(newWireTestSession(t, 4, 16*1024))
			p := startLivenessPeerA1(t, sess, 7703, false)
			frame := make([]byte, 5)
			if tc.length <= 9+BlockSize {
				frame = make([]byte, 4+tc.length)
			}
			binary.BigEndian.PutUint32(frame, tc.length)
			frame[4] = tc.id
			p.write(frame)
			if !p.closedWithin(3 * time.Second) {
				t.Fatal("peer was not dropped")
			}
			if got := reasons(p.addr); got != "invalid_message_length" {
				t.Fatalf("disconnect reason %q, want invalid_message_length", got)
			}
		})
	}
}

// send writes a message the loop must take.
func (p *livenessPeerA1) send(m *peer.Message) {
	p.t.Helper()
	if !p.write(m.Serialize()) {
		p.t.Fatalf("write message %d failed", m.ID)
	}
}

// pieceFrameA1 is a piece message carrying length bytes at (index, begin).
func pieceFrameA1(index, begin, length uint32) []byte {
	frame := make([]byte, 13+length)
	binary.BigEndian.PutUint32(frame[0:4], 9+length)
	frame[4] = byte(peer.MsgPiece)
	binary.BigEndian.PutUint32(frame[5:9], index)
	binary.BigEndian.PutUint32(frame[9:13], begin)
	return frame
}

// A peer streaming piece data we never asked for used to be kept for as long as
// it had pieces we want. It is dropped once the unrequested bytes pass
// unsolicitedFloodSlack (and the useful bytes).
func TestUnsolicitedPieceFloodDisconnects(t *testing.T) {
	reasons := disconnectReasonsA1(t)
	sess := settled(newWireTestSession(t, 4, 256*1024))
	p := startLivenessPeerA1(t, sess, 7704, false)
	p.send(&peer.Message{ID: peer.MsgHaveAll}) // it has everything we want

	sent := 0
	for sent < 1000 && p.write(pieceFrameA1(0, 0, BlockSize)) {
		sent++
	}
	if !p.closedWithin(3 * time.Second) {
		t.Fatal("a peer flooding unrequested blocks was not dropped")
	}
	if got := reasons(p.addr); got != "unsolicited_flood" {
		t.Fatalf("disconnect reason %q, want unsolicited_flood", got)
	}
	if limit := unsolicitedFloodSlack / BlockSize; sent < limit || sent > limit+16 {
		t.Fatalf("dropped after %d unrequested blocks, want just over %d", sent, limit)
	}
}

// requestsA1 collects the block requests the loop sends until a barrier (a
// request for a piece that cannot exist, answered by a reject) comes back.
func (p *livenessPeerA1) requestsA1() [][3]uint32 {
	p.t.Helper()
	p.send(&peer.Message{ID: peer.MsgRequest, Payload: blockPayload(0xffffffff, 0, 1)})
	var reqs [][3]uint32
	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg := <-p.in:
			switch {
			case msg.ID == peer.MsgRejectRequest && binary.BigEndian.Uint32(msg.Payload[0:4]) == 0xffffffff:
				return reqs
			case msg.ID == peer.MsgRequest:
				reqs = append(reqs, [3]uint32{
					binary.BigEndian.Uint32(msg.Payload[0:4]),
					binary.BigEndian.Uint32(msg.Payload[4:8]),
					binary.BigEndian.Uint32(msg.Payload[8:12]),
				})
			}
		case <-deadline:
			p.t.Fatal("timed out waiting for the barrier reject")
		}
	}
}

// An honest peer with a deep pipeline keeps answering requests we gave up on:
// after a choke it may still deliver everything in flight. Those late blocks are
// discarded but never count as a flood, however many choke rounds pile them up;
// only data beyond them does.
func TestLateBlocksAfterChokeAreNotAFlood(t *testing.T) {
	reasons := disconnectReasonsA1(t)
	sess := settled(newWireTestSession(t, 64, 256*1024))
	p := startLivenessPeerA1(t, sess, 7705, false)
	p.send(&peer.Message{ID: peer.MsgHaveAll})

	var late int
	for round := 0; late < 2*unsolicitedFloodSlack; round++ {
		p.send(&peer.Message{ID: peer.MsgUnchoke})
		reqs := p.requestsA1()
		if len(reqs) == 0 {
			t.Fatalf("round %d: no requests after an unchoke", round)
		}
		p.send(&peer.Message{ID: peer.MsgChoke})
		for _, r := range reqs {
			if !p.write(pieceFrameA1(r[0], r[1], r[2])) {
				t.Fatalf("round %d: dropped after %d late bytes", round, late)
			}
			late += int(r[2])
		}
		p.requestsA1() // every late block has been handled
		t.Logf("round %d: %d requests, %d late bytes so far", round, len(reqs), late)
	}
	if p.closedWithin(0) {
		t.Fatalf("dropped after %d late bytes: %q", late, reasons(p.addr))
	}

	// Data beyond what we gave up on is still limited.
	for i := 0; i < 2*unsolicitedFloodSlack/BlockSize && p.write(pieceFrameA1(0, 0, BlockSize)); i++ {
	}
	if !p.closedWithin(3 * time.Second) {
		t.Fatal("a flood after the late blocks was not dropped")
	}
	if got := reasons(p.addr); got != "unsolicited_flood" {
		t.Fatalf("disconnect reason %q, want unsolicited_flood", got)
	}
}
