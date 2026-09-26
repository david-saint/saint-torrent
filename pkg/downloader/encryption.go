package downloader

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
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

// secretKeyIndex indexes the managed torrents by the obfuscated info hash an
// MSE initiator sends (mse.ObfuscatedHash), so an inbound handshake finds its
// torrent with one map lookup instead of hashing every managed info hash.
// It has its own lock rather than being a copy-on-write snapshot: a lookup
// still never waits on TorrentManager.mu, and adding or removing a torrent
// stays O(1) instead of cloning the whole index, which made restoring N
// torrents at startup O(N²) under the manager lock.
type secretKeyIndex struct {
	mu     sync.RWMutex
	byHash map[[sha1.Size]byte]secretKeyEntry
}

type secretKeyEntry struct {
	infoHash [20]byte
	refs     int // sessions sharing this info hash
}

func (x *secretKeyIndex) add(infoHash [20]byte) {
	key := mse.ObfuscatedHash(infoHash[:])
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.byHash == nil {
		x.byHash = make(map[[sha1.Size]byte]secretKeyEntry)
	}
	e := x.byHash[key]
	e.infoHash = infoHash
	e.refs++
	x.byHash[key] = e
}

func (x *secretKeyIndex) remove(infoHash [20]byte) {
	key := mse.ObfuscatedHash(infoHash[:])
	x.mu.Lock()
	defer x.mu.Unlock()
	e, ok := x.byHash[key]
	switch {
	case !ok:
	case e.refs > 1:
		e.refs--
		x.byHash[key] = e
	default:
		delete(x.byHash, key)
	}
}

func (x *secretKeyIndex) clear() {
	x.mu.Lock()
	x.byHash = nil
	x.mu.Unlock()
}

// lookup is an mse.SecretKeyLookup over the index.
func (x *secretKeyIndex) lookup(obfuscated [sha1.Size]byte) ([]byte, bool) {
	x.mu.RLock()
	e, ok := x.byHash[obfuscated]
	x.mu.RUnlock()
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
