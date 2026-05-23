package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"time"
)

// BEP 15 protocol constants.
const (
	// udpConnectMagic is the magic connection_id sent in the initial connect request.
	udpConnectMagic uint64 = 0x41727101980

	// actionConnect is the action value for a connect request/response.
	actionConnect uint32 = 0

	// actionAnnounce is the action value for an announce request/response.
	actionAnnounce uint32 = 1

	// actionError is the action value for an error response.
	actionError uint32 = 3

	// udpMaxRetries is the maximum number of retries before giving up (n=0..8).
	udpMaxRetries = 8

	// udpConnectRequestSize is the byte size of a connect request packet.
	udpConnectRequestSize = 16

	// udpConnectResponseSize is the minimum byte size of a connect response packet.
	udpConnectResponseSize = 16

	// udpAnnounceRequestSize is the byte size of an announce request packet.
	udpAnnounceRequestSize = 98

	// udpAnnounceResponseMinSize is the minimum byte size of an announce response
	// (header only, no peers).
	udpAnnounceResponseMinSize = 20

	// eventNone is the "none" event value for an announce request.
	eventNone uint32 = 0
)

// udpTimeout returns the timeout duration for retry attempt n, per BEP 15:
// 15 * 2^n seconds.
func udpTimeout(n int) time.Duration {
	d := 15 * time.Second
	for i := 0; i < n; i++ {
		d *= 2
	}
	return d
}

// newTransactionID generates a cryptographically random 32-bit transaction ID.
func newTransactionID() (uint32, error) {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("generating transaction ID: %w", err)
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

// UDPAnnounce performs a full UDP tracker announce as specified in BEP 15.
// It resolves the host from announceURL, performs the connect handshake to
// obtain a connection_id, then sends the announce request and parses the
// compact peer list from the response.
//
// The announceURL should be of the form "udp://host:port/announce".
// The function respects context cancellation and applies exponential backoff
// retries on timeouts. Callers should pass a bounded context for app-level
// tracker fallback; cancellation closes the UDP connection promptly.
func UDPAnnounce(ctx context.Context, announceURL string, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, event string) (*TrackerResponse, error) {
	u, err := url.Parse(announceURL)
	if err != nil {
		return nil, fmt.Errorf("parsing announce URL: %w", err)
	}
	if u.Scheme != "udp" {
		return nil, fmt.Errorf("unsupported scheme %q, expected \"udp\"", u.Scheme)
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		return nil, fmt.Errorf("invalid host %q: %w", host, err)
	}

	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.DialContext(ctx, "udp", host)
	if err != nil {
		return nil, fmt.Errorf("dialing UDP %s: %w", host, err)
	}
	defer conn.Close()

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	// Step 1: Connect handshake.
	connectionID, err := udpConnect(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("UDP connect: %w", err)
	}

	// Step 2: Announce.
	resp, err := udpAnnounceRequest(ctx, conn, connectionID, infoHash, peerID, port, uploaded, downloaded, left, event)
	if err != nil {
		return nil, fmt.Errorf("UDP announce: %w", err)
	}

	return resp, nil
}

// udpConnect performs the BEP 15 connect handshake on an already-dialed UDP
// connection. It retries with exponential backoff up to udpMaxRetries times.
// Returns the connection_id from the tracker on success.
func udpConnect(ctx context.Context, conn net.Conn) (uint64, error) {
	txnID, err := newTransactionID()
	if err != nil {
		return 0, err
	}

	// Build connect request: [8 bytes magic] [4 bytes action=0] [4 bytes txn_id]
	var req [udpConnectRequestSize]byte
	binary.BigEndian.PutUint64(req[0:8], udpConnectMagic)
	binary.BigEndian.PutUint32(req[8:12], actionConnect)
	binary.BigEndian.PutUint32(req[12:16], txnID)

	for n := 0; n <= udpMaxRetries; n++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		timeout := udpTimeout(n)
		deadline := time.Now().Add(timeout)

		// If context has a deadline that's earlier, use that.
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}

		if err := conn.SetWriteDeadline(deadline); err != nil {
			return 0, fmt.Errorf("setting write deadline: %w", err)
		}
		if _, err := conn.Write(req[:]); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, ctxErr
			}
			// On timeout, retry.
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return 0, fmt.Errorf("writing connect request: %w", err)
		}

		if err := conn.SetReadDeadline(deadline); err != nil {
			return 0, fmt.Errorf("setting read deadline: %w", err)
		}

		var buf [2048]byte
		nRead, err := conn.Read(buf[:])
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, ctxErr
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return 0, fmt.Errorf("reading connect response: %w", err)
		}

		if nRead < udpConnectResponseSize {
			return 0, fmt.Errorf("connect response too short: %d bytes", nRead)
		}

		respAction := binary.BigEndian.Uint32(buf[0:4])
		respTxnID := binary.BigEndian.Uint32(buf[4:8])

		if respTxnID != txnID {
			return 0, fmt.Errorf("connect response transaction ID mismatch: expected %d, got %d", txnID, respTxnID)
		}

		if respAction == actionError {
			msg := string(buf[8:nRead])
			return 0, fmt.Errorf("tracker error: %s", msg)
		}

		if respAction != actionConnect {
			return 0, fmt.Errorf("unexpected connect response action: %d", respAction)
		}

		connectionID := binary.BigEndian.Uint64(buf[8:16])
		return connectionID, nil
	}

	return 0, fmt.Errorf("connect timed out after %d retries", udpMaxRetries+1)
}

// udpAnnounceRequest performs the BEP 15 announce on an already-connected UDP
// connection using the given connection_id. It retries with exponential backoff
// up to udpMaxRetries times.
func udpAnnounceRequest(ctx context.Context, conn net.Conn, connectionID uint64, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, event string) (*TrackerResponse, error) {
	txnID, err := newTransactionID()
	if err != nil {
		return nil, err
	}

	// Build announce request (98 bytes):
	// [0..8]   connection_id
	// [8..12]  action = 1
	// [12..16] transaction_id
	// [16..36] info_hash
	// [36..56] peer_id
	// [56..64] downloaded
	// [64..72] left
	// [72..80] uploaded
	// [80..84] event (0 = none)
	// [84..88] IP address (0 = default)
	// [88..92] key (random)
	// [92..96] num_want (-1 = default)
	// [96..98] port
	var req [udpAnnounceRequestSize]byte
	binary.BigEndian.PutUint64(req[0:8], connectionID)
	binary.BigEndian.PutUint32(req[8:12], actionAnnounce)
	binary.BigEndian.PutUint32(req[12:16], txnID)
	copy(req[16:36], infoHash[:])
	copy(req[36:56], peerID[:])
	binary.BigEndian.PutUint64(req[56:64], uint64(downloaded))
	binary.BigEndian.PutUint64(req[64:72], uint64(left))
	binary.BigEndian.PutUint64(req[72:80], uint64(uploaded))
	var eventVal uint32
	switch event {
	case "completed":
		eventVal = 1
	case "started":
		eventVal = 2
	case "stopped":
		eventVal = 3
	default:
		eventVal = 0
	}
	binary.BigEndian.PutUint32(req[80:84], eventVal)
	binary.BigEndian.PutUint32(req[84:88], 0) // IP address = 0 (default)
	// key: random 32-bit value for identification across requests
	keyID, _ := newTransactionID()
	binary.BigEndian.PutUint32(req[88:92], keyID)
	binary.BigEndian.PutUint32(req[92:96], 0xFFFFFFFF) // num_want = -1 (default)
	binary.BigEndian.PutUint16(req[96:98], port)

	for n := 0; n <= udpMaxRetries; n++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		timeout := udpTimeout(n)
		deadline := time.Now().Add(timeout)

		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}

		if err := conn.SetWriteDeadline(deadline); err != nil {
			return nil, fmt.Errorf("setting write deadline: %w", err)
		}
		if _, err := conn.Write(req[:]); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return nil, fmt.Errorf("writing announce request: %w", err)
		}

		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, fmt.Errorf("setting read deadline: %w", err)
		}

		var buf [4096]byte
		nRead, err := conn.Read(buf[:])
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return nil, fmt.Errorf("reading announce response: %w", err)
		}

		if nRead < udpAnnounceResponseMinSize {
			return nil, fmt.Errorf("announce response too short: %d bytes", nRead)
		}

		respAction := binary.BigEndian.Uint32(buf[0:4])
		respTxnID := binary.BigEndian.Uint32(buf[4:8])

		if respTxnID != txnID {
			return nil, fmt.Errorf("announce response transaction ID mismatch: expected %d, got %d", txnID, respTxnID)
		}

		if respAction == actionError {
			msg := string(buf[8:nRead])
			return nil, fmt.Errorf("tracker error: %s", msg)
		}

		if respAction != actionAnnounce {
			return nil, fmt.Errorf("unexpected announce response action: %d", respAction)
		}

		return parseUDPAnnounceResponse(buf[:nRead])
	}

	return nil, fmt.Errorf("announce timed out after %d retries", udpMaxRetries+1)
}

// parseUDPAnnounceResponse parses a raw UDP announce response into a
// TrackerResponse. The response layout is:
//
//	[0..4]   action = 1
//	[4..8]   transaction_id
//	[8..12]  interval
//	[12..16] leechers
//	[16..20] seeders
//	[20..]   peers (6 bytes each: 4 IP + 2 port)
func parseUDPAnnounceResponse(data []byte) (*TrackerResponse, error) {
	if len(data) < udpAnnounceResponseMinSize {
		return nil, fmt.Errorf("announce response too short: %d bytes", len(data))
	}

	interval := int(binary.BigEndian.Uint32(data[8:12]))
	leechers := int(binary.BigEndian.Uint32(data[12:16]))
	seeders := int(binary.BigEndian.Uint32(data[16:20]))

	peerData := data[20:]
	if len(peerData)%6 != 0 {
		return nil, fmt.Errorf("peer data length %d is not a multiple of 6", len(peerData))
	}

	numPeers := len(peerData) / 6
	peers := make([]Peer, 0, numPeers)
	for i := 0; i < numPeers; i++ {
		offset := i * 6
		ip := make(net.IP, 4)
		copy(ip, peerData[offset:offset+4])
		port := binary.BigEndian.Uint16(peerData[offset+4 : offset+6])
		peers = append(peers, Peer{IP: ip, Port: port})
	}

	return &TrackerResponse{
		Interval:   interval,
		Peers:      peers,
		Complete:   seeders,
		Incomplete: leechers,
	}, nil
}
