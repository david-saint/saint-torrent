package downloader

import (
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// keepAlive sends keep-alives every interval until stop closes, keeping the
// connection's read deadline fresh the way an idle slot-holder would.
func (w *wirePeer) keepAlive(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = w.remote.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := w.remote.Write((*peer.Message)(nil).Serialize()); err != nil {
					return
				}
			case <-stop:
				return
			}
		}
	}()
}

// alive reports whether the peer loop is still running after d.
func (w *wirePeer) alive(d time.Duration) bool {
	select {
	case <-w.done:
		return false
	case <-time.After(d):
		return true
	}
}

// settled marks a harness session's (never started) resume check as done, so
// its pieces count as known.
func settled(sess *Session) *Session {
	sess.mu.Lock()
	sess.verifying = false
	sess.mu.Unlock()
	return sess
}

// A connection on which neither side is interested and nothing moves used to be
// kept for as long as the peer sent keep-alives (an inbound one is never stall
// reaped). It must be dropped after peerInactivityTimeout; useful ones stay.
func TestIdleConnectionIsDropped(t *testing.T) {
	defer swapDuration(&peerInactivityTimeout, 300*time.Millisecond)()

	t.Run("nobody interested", func(t *testing.T) {
		sess, _ := newSeedingWireTestSession(t, 4, 16*1024)
		w := startWirePeer(t, settled(sess), 7600, fastReserved())
		stop := make(chan struct{})
		defer close(stop)
		w.keepAlive(50*time.Millisecond, stop)
		w.waitClosed(3 * time.Second)
	})

	t.Run("peer interested", func(t *testing.T) {
		sess, _ := newSeedingWireTestSession(t, 4, 16*1024)
		w := startWirePeer(t, settled(sess), 7601, fastReserved())
		w.send(&peer.Message{ID: peer.MsgInterested})
		stop := make(chan struct{})
		defer close(stop)
		w.keepAlive(50*time.Millisecond, stop)
		if !w.alive(1500 * time.Millisecond) {
			t.Fatal("dropped a peer that is interested in our pieces")
		}
	})

	t.Run("we want the peer's pieces", func(t *testing.T) {
		sess := settled(newWireTestSession(t, 4, 16*1024))
		w := startWirePeer(t, sess, 7602, fastReserved())
		w.send(&peer.Message{ID: peer.MsgHaveAll}) // but it never unchokes us
		stop := make(chan struct{})
		defer close(stop)
		w.keepAlive(50*time.Millisecond, stop)
		if !w.alive(1500 * time.Millisecond) {
			t.Fatal("dropped a peer that has pieces we want")
		}
	})
}
