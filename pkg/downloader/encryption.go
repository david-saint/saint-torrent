package downloader

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/peer"
)

const peerHandshakeTimeout = 10 * time.Second

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

func secretKeyIter(secrets ...[20]byte) mse.SecretKeyIter {
	return func(callback func([]byte) bool) {
		for i := range secrets {
			if !callback(secrets[i][:]) {
				return
			}
		}
	}
}

func negotiateIncomingPeerConn(conn net.Conn, policy mse.Policy, secrets mse.SecretKeyIter) (net.Conn, mse.Result, bool, error) {
	buffered := newBufferedConn(conn)
	if policy == mse.PolicyDisable {
		return buffered, mse.Result{}, false, nil
	}
	if policy == mse.PolicyPrefer {
		prefix, err := buffered.Peek(mse.PlaintextHandshakePrefixLen())
		if err != nil {
			return nil, mse.Result{}, false, err
		}
		if mse.LooksLikePlaintextHandshake(prefix) {
			return buffered, mse.Result{}, false, nil
		}
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

	wrapped, res, encrypted, err := negotiateIncomingPeerConn(conn, policy, secretKeyIter(infoHash))
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
