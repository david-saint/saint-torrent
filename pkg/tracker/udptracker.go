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

	// actionScrape is the action value for a scrape request/response.
	actionScrape uint32 = 2

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

	// udpScrapeRequestHeaderSize is the byte size of a scrape request header,
	// before the appended 20-byte info hashes.
	udpScrapeRequestHeaderSize = 16

	// udpScrapeResponseHeaderSize is the byte size of a scrape response header
	// (action + transaction_id), before the per-torrent stat triples.
	udpScrapeResponseHeaderSize = 8

	// udpScrapeStatSize is the byte size of one per-torrent stat triple in a
	// scrape response: seeders, completed, leechers (3 * 4 bytes).
	udpScrapeStatSize = 12

	// udpMaxScrapeHashes is the maximum number of info hashes a single UDP
	// scrape request may carry (BEP 15).
	udpMaxScrapeHashes = 74

	// eventNone is the "none" event value for an announce request.
	eventNone uint32 = 0
)

// defaultUDPTimeout bounds a UDP announce or scrape when the caller passes a
// context without its own deadline, so an exported call can never pin a
// goroutine and UDP socket indefinitely. Across the connect and announce
// phases, BEP 15 backoff (15s * 2^n up to n=8) otherwise totals ~7650s (~2h)
// for a dead tracker. It mirrors defaultScrapeTimeout on the HTTP path.
const defaultUDPTimeout = 60 * time.Second

// ensureUDPDeadline returns ctx unchanged when it already carries a deadline,
// otherwise it derives a child bounded by defaultUDPTimeout. The returned
// cancel func must always be called. It lets UDPAnnounce and UDPScrape apply a
// default cap the same way HTTPScrape does, so an unbounded caller context
// cannot hang a round trip for the full BEP 15 backoff.
func ensureUDPDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultUDPTimeout)
}

// udpDial parses a "udp://host:port/..." announce URL, dials the tracker, and
// starts a watchdog that closes the connection when ctx is cancelled. The
// returned cleanup func stops the watchdog and closes the connection; callers
// must defer it. It is shared by UDPAnnounce and UDPScrape.
func udpDial(ctx context.Context, announceURL string) (net.Conn, func(), error) {
	u, err := url.Parse(announceURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing announce URL: %w", err)
	}
	if u.Scheme != "udp" {
		return nil, nil, fmt.Errorf("unsupported scheme %q, expected \"udp\"", u.Scheme)
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		return nil, nil, fmt.Errorf("invalid host %q: %w", host, err)
	}

	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.DialContext(ctx, "udp", host)
	if err != nil {
		return nil, nil, fmt.Errorf("dialing UDP %s: %w", host, err)
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	cleanup := func() {
		close(done)
		_ = conn.Close()
	}
	return conn, cleanup, nil
}

// udpRoundTrip writes req on conn and waits for a single response, retrying
// with BEP 15 exponential backoff up to udpMaxRetries times. It validates the
// response transaction ID, surfaces tracker error packets (action=3), and
// requires the response action to equal expectedAction. opName labels the
// operation in error messages. It is shared by udpConnect, udpAnnounceRequest,
// and udpScrapeRequest.
func udpRoundTrip(ctx context.Context, conn net.Conn, req []byte, txnID, expectedAction uint32, minRespSize int, opName string) ([]byte, error) {
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
		if _, err := conn.Write(req); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return nil, fmt.Errorf("writing %s request: %w", opName, err)
		}

		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, fmt.Errorf("setting read deadline: %w", err)
		}

		// Read responses until one carries our transaction ID or the deadline
		// fires. The connect and announce phases share a single connected
		// socket, and udpRoundTrip retransmits with the same transaction ID, so
		// a slow tracker can leave a late, stale response queued on the socket.
		// BEP 15 requires ignoring any datagram whose transaction ID does not
		// match the pending request and continuing to read for the remaining
		// deadline, rather than aborting the round trip over a recoverable
		// duplicate. The absolute read deadline (unchanged across reads) bounds
		// the total time spent draining stray datagrams.
		var buf [4096]byte
		for {
			nRead, err := conn.Read(buf[:])
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					break // deadline reached; retransmit on the next attempt
				}
				return nil, fmt.Errorf("reading %s response: %w", opName, err)
			}

			// A datagram needs at least the 8-byte action+transaction_id header
			// to be identifiable; discard shorter runts and keep reading.
			if nRead < 8 {
				continue
			}

			respAction := binary.BigEndian.Uint32(buf[0:4])
			respTxnID := binary.BigEndian.Uint32(buf[4:8])

			// Ignore stale duplicates and unrelated datagrams (BEP 15): discard
			// and keep reading within the deadline instead of failing the round
			// trip. Only once the transaction ID matches do we hold the tracker
			// to a well-formed response.
			if respTxnID != txnID {
				continue
			}
			if nRead < minRespSize {
				return nil, fmt.Errorf("%s response too short: %d bytes", opName, nRead)
			}
			if respAction == actionError {
				return nil, fmt.Errorf("tracker error: %s", string(buf[8:nRead]))
			}
			if respAction != expectedAction {
				return nil, fmt.Errorf("unexpected %s response action: %d", opName, respAction)
			}

			// Copy out of the stack buffer so the returned slice stays valid.
			out := make([]byte, nRead)
			copy(out, buf[:nRead])
			return out, nil
		}
	}

	return nil, fmt.Errorf("%s timed out after %d retries", opName, udpMaxRetries+1)
}

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
// tracker fallback; cancellation closes the UDP connection promptly. When the
// caller's context has no deadline, defaultUDPTimeout is injected so a dead
// tracker cannot pin the goroutine and socket for the full BEP 15 backoff.
func UDPAnnounce(ctx context.Context, announceURL string, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, event string, numWant ...int) (*TrackerResponse, error) {
	ctx, cancel := ensureUDPDeadline(ctx)
	defer cancel()

	conn, cleanup, err := udpDial(ctx, announceURL)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	// Step 1: Connect handshake.
	connectionID, err := udpConnect(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("UDP connect: %w", err)
	}

	// Step 2: Announce.
	resp, err := udpAnnounceRequest(ctx, conn, connectionID, infoHash, peerID, port, uploaded, downloaded, left, event, numWant...)
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

	resp, err := udpRoundTrip(ctx, conn, req[:], txnID, actionConnect, udpConnectResponseSize, "connect")
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(resp[8:16]), nil
}

// udpAnnounceRequest performs the BEP 15 announce on an already-connected UDP
// connection using the given connection_id. It retries with exponential backoff
// up to udpMaxRetries times.
func udpAnnounceRequest(ctx context.Context, conn net.Conn, connectionID uint64, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, event string, numWant ...int) (*TrackerResponse, error) {
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
	// [92..96] num_want
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
		eventVal = eventNone
	}
	binary.BigEndian.PutUint32(req[80:84], eventVal)
	binary.BigEndian.PutUint32(req[84:88], 0) // IP address = 0 (default)
	// key: random 32-bit value for identification across requests
	keyID, _ := newTransactionID()
	binary.BigEndian.PutUint32(req[88:92], keyID)
	want := defaultNumWant
	if len(numWant) > 0 {
		want = numWant[0]
	}
	binary.BigEndian.PutUint32(req[92:96], uint32(int32(want)))
	binary.BigEndian.PutUint16(req[96:98], port)

	resp, err := udpRoundTrip(ctx, conn, req[:], txnID, actionAnnounce, udpAnnounceResponseMinSize, "announce")
	if err != nil {
		return nil, err
	}
	return parseUDPAnnounceResponse(resp)
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

// UDPScrape performs a BEP 15 UDP tracker scrape (action=2) for one or more
// info hashes. It resolves the host from announceURL, reuses the connect
// handshake to obtain a connection_id, then sends the scrape request and parses
// the per-torrent swarm-health counts. The result is keyed by raw info hash.
//
// Because a BEP 15 scrape response carries no info-hash identifiers, hashes are
// matched to triples by position. To keep that mapping safe when batching
// multiple hashes, the tracker must return exactly one triple per requested hash
// or the whole response is rejected; see parseUDPScrapeResponse for the full
// policy.
//
// The announceURL should be of the form "udp://host:port/announce". The
// function respects context cancellation and applies the same exponential
// backoff retries on timeouts as UDPAnnounce. When the caller's context has no
// deadline, defaultUDPTimeout is injected so a dead tracker cannot pin the
// goroutine and socket for the full BEP 15 backoff.
func UDPScrape(ctx context.Context, announceURL string, infoHashes ...[20]byte) (map[[20]byte]ScrapeStats, error) {
	if len(infoHashes) == 0 {
		return nil, fmt.Errorf("UDP scrape requires at least one info hash")
	}
	if len(infoHashes) > udpMaxScrapeHashes {
		return nil, fmt.Errorf("UDP scrape supports at most %d info hashes, got %d", udpMaxScrapeHashes, len(infoHashes))
	}

	ctx, cancel := ensureUDPDeadline(ctx)
	defer cancel()

	conn, cleanup, err := udpDial(ctx, announceURL)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	connectionID, err := udpConnect(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("UDP connect: %w", err)
	}

	resp, err := udpScrapeRequest(ctx, conn, connectionID, infoHashes)
	if err != nil {
		return nil, fmt.Errorf("UDP scrape: %w", err)
	}
	return resp, nil
}

// udpScrapeRequest performs the BEP 15 scrape on an already-connected UDP
// connection using the given connection_id. It retries with exponential backoff
// up to udpMaxRetries times.
func udpScrapeRequest(ctx context.Context, conn net.Conn, connectionID uint64, infoHashes [][20]byte) (map[[20]byte]ScrapeStats, error) {
	txnID, err := newTransactionID()
	if err != nil {
		return nil, err
	}

	// Build scrape request:
	// [0..8]   connection_id
	// [8..12]  action = 2
	// [12..16] transaction_id
	// [16..]   info_hash (20 bytes each)
	req := make([]byte, udpScrapeRequestHeaderSize+20*len(infoHashes))
	binary.BigEndian.PutUint64(req[0:8], connectionID)
	binary.BigEndian.PutUint32(req[8:12], actionScrape)
	binary.BigEndian.PutUint32(req[12:16], txnID)
	for i, h := range infoHashes {
		copy(req[udpScrapeRequestHeaderSize+i*20:], h[:])
	}

	resp, err := udpRoundTrip(ctx, conn, req, txnID, actionScrape, udpScrapeResponseHeaderSize, "scrape")
	if err != nil {
		return nil, err
	}
	return parseUDPScrapeResponse(resp, infoHashes)
}

// parseUDPScrapeResponse parses a raw UDP scrape response into a map keyed by
// info hash. The response layout is:
//
//	[0..4]  action = 2
//	[4..8]  transaction_id
//	[8..]   per-torrent triples: seeders (4), completed (4), leechers (4)
//
// Mapping policy (issue #35): a BEP 15 scrape response is a flat sequence of
// triples that carries no info-hash identifiers, so the only correspondence to
// the requested hashes is positional — the spec mandates exactly one triple per
// requested hash, in request order. We therefore map purely by position, but
// only when the tracker returns a triple count that exactly matches the number
// of hashes requested. Any other count means the positional correspondence has
// been broken (a triple was dropped, added, or otherwise shifted), so we reject
// the whole response rather than risk assigning swarm counts to the wrong
// torrent. Reordering that preserves the count is undetectable from the wire and
// is the documented trust assumption of positional mapping.
//
// This strictness matters most for multi-hash (batched) scrapes, where a count
// mismatch would otherwise mis-map counts across torrents. On the single-hash
// live path the only compliant reply is one triple, which still maps normally;
// a non-compliant reply with the wrong count is now rejected rather than
// partially mapped. That is a behavior change only for an over-count reply
// (formerly the first triple was used), but it is harmless: that tracker's
// scrape is best-effort and simply ignored by the caller.
func parseUDPScrapeResponse(data []byte, infoHashes [][20]byte) (map[[20]byte]ScrapeStats, error) {
	if len(data) < udpScrapeResponseHeaderSize {
		return nil, fmt.Errorf("scrape response too short: %d bytes", len(data))
	}

	payload := data[udpScrapeResponseHeaderSize:]
	if len(payload)%udpScrapeStatSize != 0 {
		return nil, fmt.Errorf("scrape response has %d stat bytes, not a whole number of %d-byte triples",
			len(payload), udpScrapeStatSize)
	}

	// Positional mapping is only safe when each requested hash has exactly one
	// triple. Reject any count mismatch instead of mapping by position.
	triples := len(payload) / udpScrapeStatSize
	if triples != len(infoHashes) {
		return nil, fmt.Errorf("scrape response returned %d triples for %d requested info hash(es); "+
			"positional hash mapping requires an exact match", triples, len(infoHashes))
	}

	result := make(map[[20]byte]ScrapeStats, triples)
	for i := 0; i < triples; i++ {
		offset := i * udpScrapeStatSize
		result[infoHashes[i]] = ScrapeStats{
			Complete:   int(binary.BigEndian.Uint32(payload[offset : offset+4])),
			Downloaded: int(binary.BigEndian.Uint32(payload[offset+4 : offset+8])),
			Incomplete: int(binary.BigEndian.Uint32(payload[offset+8 : offset+12])),
		}
	}
	return result, nil
}
