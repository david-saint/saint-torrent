package downloader

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/utp"
)

// dialedConnA2 is one end of a net.Pipe standing in for a dialed connection;
// closed is closed once the dialer closes it.
type dialedConnA2 struct {
	conn   net.Conn
	closed chan struct{}
}

func newDialedConnA2(t *testing.T) *dialedConnA2 {
	t.Helper()
	local, remote := net.Pipe()
	c := &dialedConnA2{conn: local, closed: make(chan struct{})}
	go func() {
		_, _ = io.Copy(io.Discard, remote)
		close(c.closed)
	}()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	return c
}

func (c *dialedConnA2) closedWithin(d time.Duration) bool {
	select {
	case <-c.closed:
		return true
	case <-time.After(d):
		return false
	}
}

func dialOK(transport string, c *dialedConnA2) transportDialResult {
	return transportDialResult{transport: transport, conn: c.conn}
}

// TestDialPrefersTCPWithinGraceA2 covers the dial race keeping whichever
// transport connected first, so uTP (no congestion control or fast
// retransmit yet) won about half the races against peers that also take TCP.
// A uTP connection that comes first now waits a short grace for TCP, and TCP
// wins if it connects within it.
func TestDialPrefersTCPWithinGraceA2(t *testing.T) {
	defer swapDuration(&dialTCPGraceMin, 500*time.Millisecond)()
	defer swapDuration(&dialTCPGraceMax, 500*time.Millisecond)()
	utpConn, tcpConn := newDialedConnA2(t), newDialedConnA2(t)
	results := make(chan transportDialResult, 2)
	results <- dialOK("utp", utpConn)
	go func() {
		time.Sleep(50 * time.Millisecond)
		results <- dialOK("tcp", tcpConn)
	}()

	conn, transport, err := pickDialResult(results, 2, time.Now(), func() {}, func() {})
	if err != nil || transport != "tcp" || conn != tcpConn.conn {
		t.Fatalf("picked %q (%v), want the TCP connection that followed within the grace", transport, err)
	}
	if !utpConn.closedWithin(time.Second) {
		t.Fatal("the uTP connection that lost was left open")
	}
}

// TestDialLateTCPLosesToUTPA2 checks the grace is bounded: a TCP connection
// that has not come up when it runs out loses to the uTP one, its dial is
// cancelled, and a late success is closed.
func TestDialLateTCPLosesToUTPA2(t *testing.T) {
	defer swapDuration(&dialTCPGraceMin, 50*time.Millisecond)()
	defer swapDuration(&dialTCPGraceMax, 50*time.Millisecond)()
	utpConn, tcpConn := newDialedConnA2(t), newDialedConnA2(t)
	results := make(chan transportDialResult, 2)
	results <- dialOK("utp", utpConn)
	var tcpCancelled atomic.Bool

	start := time.Now()
	conn, transport, err := pickDialResult(results, 2, start, func() { tcpCancelled.Store(true) }, func() {})
	if err != nil || transport != "utp" || conn != utpConn.conn {
		t.Fatalf("picked %q (%v), want uTP once the grace ran out", transport, err)
	}
	if el := time.Since(start); el < dialTCPGraceMin {
		t.Fatalf("uTP picked after %v, before the %v grace for TCP ran out", el, dialTCPGraceMin)
	}
	if !tcpCancelled.Load() {
		t.Fatal("the TCP dial was not cancelled")
	}
	results <- dialOK("tcp", tcpConn)
	if !tcpConn.closedWithin(time.Second) {
		t.Fatal("a TCP connection that came up after the pick was left open")
	}
	if utpConn.closedWithin(50 * time.Millisecond) {
		t.Fatal("the picked uTP connection was closed")
	}
}

// TestDialFailureOrderingsA2 covers the remaining orderings: a failure waits
// for the other transport, a uTP connection is used at once when TCP has
// failed, and both failing reports both errors.
func TestDialFailureOrderingsA2(t *testing.T) {
	defer swapDuration(&dialTCPGraceMin, 5*time.Second)()
	defer swapDuration(&dialTCPGraceMax, 5*time.Second)()
	refused := errors.New("refused")
	fail := func(transport string) transportDialResult {
		return transportDialResult{transport: transport, err: refused}
	}
	for _, tc := range []struct {
		name  string
		order func(utpConn, tcpConn *dialedConnA2) [2]transportDialResult
		want  string // transport picked, "" for an error
	}{
		{"tcp fails then utp connects", func(u, _ *dialedConnA2) [2]transportDialResult {
			return [2]transportDialResult{fail("tcp"), dialOK("utp", u)}
		}, "utp"},
		{"utp fails then tcp connects", func(_, c *dialedConnA2) [2]transportDialResult {
			return [2]transportDialResult{fail("utp"), dialOK("tcp", c)}
		}, "tcp"},
		{"utp connects then tcp fails", func(u, _ *dialedConnA2) [2]transportDialResult {
			return [2]transportDialResult{dialOK("utp", u), fail("tcp")}
		}, "utp"},
		{"tcp connects first", func(u, c *dialedConnA2) [2]transportDialResult {
			return [2]transportDialResult{dialOK("tcp", c), dialOK("utp", u)}
		}, "tcp"},
		{"both fail", func(_, _ *dialedConnA2) [2]transportDialResult {
			return [2]transportDialResult{fail("utp"), fail("tcp")}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			utpConn, tcpConn := newDialedConnA2(t), newDialedConnA2(t)
			results := make(chan transportDialResult, 2)
			for _, r := range tc.order(utpConn, tcpConn) {
				results <- r
			}
			var utpCancelled atomic.Bool
			start := time.Now()
			conn, transport, err := pickDialResult(results, 2, start, func() {}, func() { utpCancelled.Store(true) })
			if el := time.Since(start); el > time.Second {
				t.Fatalf("pick took %v; nothing is left to wait for", el)
			}
			switch tc.want {
			case "":
				if err == nil || !strings.Contains(err.Error(), "tcp dial failed") || !strings.Contains(err.Error(), "utp dial failed") {
					t.Fatalf("both dials failed: got %v, want both errors", err)
				}
				return
			case "tcp":
				if err != nil || transport != "tcp" || conn != tcpConn.conn {
					t.Fatalf("picked %q (%v), want TCP", transport, err)
				}
			case "utp":
				if err != nil || transport != "utp" || conn != utpConn.conn {
					t.Fatalf("picked %q (%v), want uTP", transport, err)
				}
			}
			if tc.name == "tcp connects first" {
				if !utpCancelled.Load() || !utpConn.closedWithin(time.Second) {
					t.Fatal("the uTP dial that lost to TCP was not cancelled and closed")
				}
			}
		})
	}
}

// TestUTPHandshakeTimeoutFallsBackToTCPA2 covers a peer whose uTP connection
// comes up but whose handshake over it never completes, as between a new and
// an older saintTorrent (their uTP numbering differs): the attempt waited out
// the handshake timeout and counted as failed. It is now retried at once over
// TCP, in the slots already held.
func TestUTPHandshakeTimeoutFallsBackToTCPA2(t *testing.T) {
	defer swapDuration(&peerDialHandshakeTimeout, 300*time.Millisecond)()
	defer swapDuration(&dialTCPGraceMin, 20*time.Millisecond)()
	defer swapDuration(&dialTCPGraceMax, 20*time.Millisecond)()
	sess := newWireTestSession(t, 4, 16*1024)

	// The peer's uTP side accepts connections but never answers a handshake.
	peerUTP, err := utp.NewSocket(0)
	if err != nil {
		t.Fatalf("peer utp: %v", err)
	}
	defer peerUTP.Close()
	utpListener := peerUTP.Listen()
	go func() {
		for {
			if _, err := utpListener.Accept(); err != nil {
				return
			}
		}
	}()
	// Its TCP side answers.
	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer tcpLn.Close()
	handshaken := make(chan struct{}, 1)
	go func() {
		conn, err := tcpLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}
		reply := &peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: hs.InfoHash, PeerID: peerIDFor(4242)}
		if _, err := conn.Write(reply.Serialize()); err != nil {
			return
		}
		handshaken <- struct{}{}
		_, _ = io.Copy(io.Discard, conn)
	}()

	// The TCP dial racing uTP is slower than the grace; the retry reaches the
	// TCP side.
	var tcpDials atomic.Int32
	oldDial := peerTCPDial
	peerTCPDial = func(ctx context.Context, _ string) (net.Conn, error) {
		if tcpDials.Add(1) == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", tcpLn.Addr().String())
	}
	defer func() { peerTCPDial = oldDial }()

	ourUTP, err := utp.NewSocket(0)
	if err != nil {
		t.Fatalf("our utp: %v", err)
	}
	defer ourUTP.Close()
	sess.attachUTPSocket(ourUTP)

	port := peerUTP.Port()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))
	sess.mu.Lock()
	sess.Peers[addr] = &PeerState{IP: "127.0.0.1", Port: port, AmChoking: true, Choked: true, Dialable: true, Dialing: true, LastAttempt: time.Now()}
	sess.mu.Unlock()
	done := make(chan struct{})
	go func() {
		sess.connectToPeer(trackerPeer("127.0.0.1", port))
		close(done)
	}()

	select {
	case <-handshaken:
	case <-time.After(5 * time.Second):
		t.Fatal("no TCP retry after the handshake over uTP timed out")
	}
	waitActiveA2(t, sess, addr)
	if ps, _ := knownPeerState(sess, addr); ps.FailCount != 0 {
		t.Fatalf("the uTP timeout was charged as a failed attempt (FailCount %d)", ps.FailCount)
	}
	if n := tcpDials.Load(); n != 2 {
		t.Fatalf("%d TCP dials, want the race's and one retry", n)
	}
	sess.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connectToPeer did not return after Close")
	}
}
