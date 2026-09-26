package downloader

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"maps"
	"net"
	"sync"
	"time"

	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/peer"
)

const peerHandshakeTimeout = 10 * time.Second

var errPlaintextRefused = errors.New("plaintext peer refused: encryption is required")

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

type monitoredPeerConn struct {
	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func (m *monitoredPeerConn) set(conn net.Conn) {
	var closeConn net.Conn
	m.mu.Lock()
	if m.closed {
		closeConn = conn
	} else {
		m.conn = conn
	}
	m.mu.Unlock()
	if closeConn != nil {
		_ = closeConn.Close()
	}
}

func (m *monitoredPeerConn) close() {
	m.mu.Lock()
	conn := m.conn
	m.conn = nil
	m.closed = true
	m.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func newBufferedConn(conn net.Conn) *bufferedConn {
	if c, ok := conn.(*bufferedConn); ok {
		return c
	}
	return &bufferedConn{
		Conn: conn,
		r:    bufio.NewReader(conn),
	}
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *bufferedConn) Peek(n int) ([]byte, error) {
	return c.r.Peek(n)
}

func (c *bufferedConn) UnderlyingConn() net.Conn {
	return c.Conn
}

// secretKeySet indexes the managed torrents by the obfuscated info hash an MSE
// initiator sends (mse.ObfuscatedHash), so an inbound handshake finds its
// torrent with one map lookup instead of hashing every managed info hash.
// It is copy-on-write: with and without return a new set and never modify
// the receiver, so a snapshot taken under TorrentManager.mu stays valid, and
// lock-free, after the lock is released.
type secretKeySet map[[sha1.Size]byte]secretKeyEntry

type secretKeyEntry struct {
	infoHash [20]byte
	refs     int // sessions sharing this info hash
}

func (s secretKeySet) with(infoHash [20]byte) secretKeySet {
	next := maps.Clone(s)
	if next == nil {
		next = make(secretKeySet, 1)
	}
	key := mse.ObfuscatedHash(infoHash[:])
	e := next[key]
	e.infoHash = infoHash
	e.refs++
	next[key] = e
	return next
}

func (s secretKeySet) without(infoHash [20]byte) secretKeySet {
	key := mse.ObfuscatedHash(infoHash[:])
	e, ok := s[key]
	if !ok {
		return s
	}
	next := maps.Clone(s)
	if e.refs > 1 {
		e.refs--
		next[key] = e
	} else {
		delete(next, key)
	}
	return next
}

// lookup is an mse.SecretKeyLookup over the set.
func (s secretKeySet) lookup(obfuscated [sha1.Size]byte) ([]byte, bool) {
	e, ok := s[obfuscated]
	if !ok {
		return nil, false
	}
	return e.infoHash[:], true
}

func negotiateIncomingPeerConn(conn net.Conn, policy mse.Policy, secrets mse.SecretKeyLookup) (net.Conn, mse.Result, bool, error) {
	buffered := newBufferedConn(conn)
	if policy == mse.PolicyDisable {
		return buffered, mse.Result{}, false, nil
	}
	// A plaintext handshake is served under prefer and refused at once
	// otherwise: the MSE receiver waits for a 96-byte key a plaintext peer
	// never sends, so it would sit in a handshake slot until the deadline.
	prefix, err := buffered.Peek(mse.PlaintextHandshakePrefixLen())
	if err != nil {
		return nil, mse.Result{}, false, err
	}
	if mse.LooksLikePlaintextHandshake(prefix) {
		if policy != mse.PolicyPrefer {
			return nil, mse.Result{}, false, errPlaintextRefused
		}
		return buffered, mse.Result{}, false, nil
	}

	wrapped, res, err := mse.Receive(buffered, secrets, mse.SelectRC4)
	if err != nil {
		return nil, mse.Result{}, false, err
	}
	return wrapped, res, true, nil
}

func (s *Session) negotiateOutgoingPeerConn(peerAddr string, conn net.Conn, monitor *monitoredPeerConn) (net.Conn, error) {
	s.mu.RLock()
	policy := s.EncryptionPolicy
	infoHash := s.Torrent.InfoHash
	s.mu.RUnlock()
	if policy == mse.PolicyDisable {
		return conn, nil
	}

	wrapped, _, err := mse.Initiate(conn, infoHash[:], nil, mse.CryptoMethodRC4)
	if err == nil {
		if monitor != nil {
			monitor.set(wrapped)
		}
		return wrapped, nil
	}
	_ = conn.Close()
	if monitor != nil {
		monitor.set(nil)
	}
	if policy == mse.PolicyRequire {
		return nil, fmt.Errorf("mse handshake failed: %w", err)
	}

	fallback, dialErr := s.dialPeer(peerAddr)
	if dialErr != nil {
		return nil, errors.Join(
			fmt.Errorf("mse handshake failed: %w", err),
			fmt.Errorf("plaintext fallback dial failed: %w", dialErr),
		)
	}
	tunePeerConn(fallback)
	if monitor != nil {
		monitor.set(fallback)
	}
	if ctxErr := s.ctx.Err(); ctxErr != nil {
		_ = fallback.Close()
		return nil, ctxErr
	}
	return fallback, nil
}

func (s *Session) parseIncomingHandshake(conn net.Conn) (net.Conn, *peer.Handshake, error) {
	s.mu.RLock()
	policy := s.EncryptionPolicy
	infoHash := s.Torrent.InfoHash
	s.mu.RUnlock()

	wrapped, res, encrypted, err := negotiateIncomingPeerConn(conn, policy, mse.SecretKeys(infoHash[:]))
	if err != nil {
		return nil, nil, err
	}
	handshake, err := peer.ParseHandshake(wrapped)
	if err != nil {
		return nil, nil, err
	}
	if encrypted && !bytes.Equal(res.SecretKey, handshake.InfoHash[:]) {
		return nil, nil, fmt.Errorf("mse secret %x does not match peer handshake info hash %x", res.SecretKey, handshake.InfoHash)
	}
	return wrapped, handshake, nil
}
