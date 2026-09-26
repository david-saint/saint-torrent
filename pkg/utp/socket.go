package utp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dhtQueueSize    = 1024
	acceptQueueSize = 128

	// maxHalfOpen bounds the inbound connections whose SYN we have answered
	// but whose initiator has not yet acknowledged our SYN-ACK. A genuine
	// entry lives for about one round trip, so this absorbs any real burst of
	// incoming connections, while a SYN flood can hold no more than this many
	// small entries. The oldest entry is evicted to make room, so a flood has
	// to outpace real round trips to crowd out a genuine initiator.
	maxHalfOpen = 256
	// halfOpenTimeout forgets a half-open entry whose initiator never
	// acknowledged our SYN-ACK. Expiry is silent: no FIN or RESET goes to a
	// source that has not proven it can receive them.
	halfOpenTimeout = 10 * time.Second

	// maxResetsPerSecond caps the RESETs sent for packets that belong to no
	// connection. Each answers a packet from an unverified source one for
	// one, so without a cap a spoofing sender can bounce any volume off us
	// toward a third party; a peer that really lost its connection learns
	// from the first few, or from its own timeout.
	maxResetsPerSecond = 64

	// maxDHTDatagram is the most of a non-uTP datagram handed to the DHT: its
	// read loop (pkg/dht) reads into a 4096-byte buffer and never sees the
	// rest, so copying more only costs the shared read loop a 64 KiB copy per
	// junk datagram and lets the DHT queue pin up to 64 MiB.
	maxDHTDatagram = 4096

	// readErrorBackoffMin and readErrorBackoffMax bound the pause after a
	// failed read of the shared socket (see readLoop): the first failure
	// waits the minimum, each consecutive one doubles it up to the maximum,
	// and a successful read starts over.
	readErrorBackoffMin = 10 * time.Millisecond
	readErrorBackoffMax = 250 * time.Millisecond
)

var errListenerClosed = errors.New("utp: listener closed")

type udpPacket struct {
	data []byte
	addr *net.UDPAddr
}

// connKey identifies a Conn by remote address and connection id. The IP is
// stored as a normalized 16-byte array (v4 addresses map to the v4-in-v6 form)
// so the key stays comparable and can be built per received datagram without
// the addr.String() allocation the read loop would otherwise pay per packet.
type connKey struct {
	ip   [16]byte
	zone string
	port uint16
	id   uint16
}

// newConnKey builds a connKey from addr without allocating. To4/To16 return
// sub-slices (or the input) rather than fresh buffers for the shapes UDP
// sockets hand us, so no garbage is generated on the hot path.
func newConnKey(addr *net.UDPAddr, id uint16) connKey {
	k := connKey{zone: addr.Zone, port: uint16(addr.Port), id: id}
	ip := addr.IP
	if v4 := ip.To4(); v4 != nil {
		k.ip[10], k.ip[11] = 0xff, 0xff
		copy(k.ip[12:], v4)
	} else {
		copy(k.ip[:], ip.To16())
	}
	return k
}

// Socket owns one UDP socket and demultiplexes BEP 29 uTP packets from DHT
// packets. uTP packets are routed to Conn values; every non-uTP packet is exposed
// through DHTConn so the DHT and uTP can share one UDP port.
type Socket struct {
	conn *net.UDPConn
	// readFrom reads the next datagram for readLoop. It is conn.ReadFromUDP;
	// tests substitute it to inject read errors.
	readFrom func([]byte) (int, *net.UDPAddr, error)

	mu       sync.Mutex
	conns    map[connKey]*Conn
	listener *Listener
	closed   bool

	// halfOpen holds answered inbound SYNs until the initiator acknowledges
	// the SYN-ACK (see halfOpenConn). halfOpenRing records insertion order so
	// the oldest entry is evicted in O(1) when the table is full; a slot may
	// hold an entry already promoted or expired, which the map check skips.
	halfOpen     map[connKey]*halfOpenConn
	halfOpenRing [maxHalfOpen]*halfOpenConn
	halfOpenNext int

	// resetWindow and resetsSent budget unsolicited RESETs per second (see
	// maxResetsPerSecond).
	resetWindow time.Time
	resetsSent  int

	// bufPool hands out scratch buffers for packet marshaling so writePacket
	// does not allocate a fresh header+payload slice per send. sync.Pool keeps
	// this contention-free across the read loop and per-conn write goroutines.
	bufPool sync.Pool

	dhtConn *PacketConn
	done    chan struct{}
	once    sync.Once
}

// NewSocket binds a uTP/DHT shared UDP socket on listenPort. Passing 0 lets the
// OS choose a free port.
func NewSocket(listenPort int) (*Socket, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("0.0.0.0:%d", listenPort))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return NewSocketFromUDP(conn), nil
}

// NewSocketFromUDP wraps an existing UDP socket. The Socket takes ownership of
// conn and closes it from Close.
func NewSocketFromUDP(conn *net.UDPConn) *Socket {
	_ = conn.SetReadBuffer(4 * 1024 * 1024)
	_ = conn.SetWriteBuffer(4 * 1024 * 1024)
	s := newSocket(conn, conn.ReadFromUDP)
	go s.readLoop()
	return s
}

// newSocket builds a Socket around conn that reads through readFrom, without
// starting its read loop.
func newSocket(conn *net.UDPConn, readFrom func([]byte) (int, *net.UDPAddr, error)) *Socket {
	s := &Socket{
		conn:     conn,
		readFrom: readFrom,
		conns:    make(map[connKey]*Conn),
		halfOpen: make(map[connKey]*halfOpenConn),
		done:     make(chan struct{}),
	}
	s.bufPool.New = func() any {
		b := make([]byte, 0, headerSize+maxPayloadSize)
		return &b
	}
	s.dhtConn = newPacketConn(s)
	return s
}

// DHTConn returns a UDP-like packet connection that receives only non-uTP
// packets from the shared socket.
func (s *Socket) DHTConn() *PacketConn {
	return s.dhtConn
}

// Listen returns the uTP listener attached to this socket. A socket supports one
// listener because incoming SYN packets have no torrent info-hash until the
// BitTorrent handshake is read by the downloader.
func (s *Socket) Listen() *Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener
	}
	l := &Listener{
		socket:   s,
		acceptCh: make(chan *Conn, acceptQueueSize),
		closed:   make(chan struct{}),
	}
	s.listener = l
	return l
}

// DialContext opens a uTP connection to addr, which must be host:port.
func (s *Socket) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	if udpAddr.IP == nil || udpAddr.Port <= 0 {
		return nil, fmt.Errorf("utp: invalid remote address %q", addr)
	}
	return s.dialContext(ctx, udpAddr)
}

func (s *Socket) dialContext(ctx context.Context, addr *net.UDPAddr) (*Conn, error) {
	baseID := randomUint16()
	conn := newOutboundConn(s, addr, baseID)
	if err := s.register(conn); err != nil {
		return nil, err
	}
	if err := conn.dial(ctx); err != nil {
		s.unregister(conn)
		conn.closeWithError(err, false)
		return nil, err
	}
	return conn, nil
}

// Port returns the local UDP port used by this socket.
func (s *Socket) Port() uint16 {
	addr, ok := s.conn.LocalAddr().(*net.UDPAddr)
	if !ok || addr.Port <= 0 || addr.Port > 65535 {
		return 0
	}
	return uint16(addr.Port)
}

func (s *Socket) localAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *Socket) nowMicros() uint32 {
	return uint32(time.Now().UnixMicro())
}

func (s *Socket) register(c *Conn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	key := newConnKey(c.remote, c.recvID)
	if _, exists := s.conns[key]; exists {
		return fmt.Errorf("utp: connection id collision for %s", c.remote)
	}
	s.conns[key] = c
	return nil
}

func (s *Socket) unregister(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := newConnKey(c.remote, c.recvID)
	if current := s.conns[key]; current == c {
		delete(s.conns, key)
	}
}

func (s *Socket) writePacket(p packet, addr *net.UDPAddr) error {
	bufp := s.bufPool.Get().(*[]byte)
	b := p.marshalInto((*bufp)[:0])
	_, err := s.conn.WriteToUDP(b, addr)
	*bufp = b
	s.bufPool.Put(bufp)
	return err
}

func (s *Socket) readLoop() {
	buf := make([]byte, 64*1024)
	var backoff time.Duration
	for {
		n, addr, err := s.readFrom(buf)
		if err != nil {
			backoff = nextReadErrorBackoff(backoff)
			if !s.pauseAfterReadError(err, backoff) {
				return
			}
			continue
		}
		backoff = 0
		if IsPacket(buf[:n]) {
			// handleUTPPacket runs synchronously in this goroutine and the
			// Conn copies any payload it retains (into readBuf or the pending
			// map), so buf can be reused immediately without a per-packet copy.
			s.handleUTPPacket(buf[:n], addr)
			continue
		}
		// The DHT path hands the datagram to another goroutine via a channel,
		// so it must own a copy that outlives the next ReadFromUDP. Only the
		// part the DHT will ever read is copied.
		data := append([]byte(nil), buf[:min(n, maxDHTDatagram)]...)
		s.dhtConn.deliver(udpPacket{data: data, addr: cloneUDPAddr(addr)})
	}
}

// pauseAfterReadError handles a failed read of the shared socket and reports
// whether readLoop should read again. Only a closed socket ends the loop. The
// socket carries every uTP conn and the DHT, and other read errors (ENOBUFS
// or ENOMEM under memory pressure, an ICMP error some platforms surface on
// the next read) are transient, so giving up on one would disable uTP and the
// DHT until restart. Those wait out backoff instead, so a persistent error
// cannot spin the loop; Close cuts the wait short.
func (s *Socket) pauseAfterReadError(err error, backoff time.Duration) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	if errors.Is(err, net.ErrClosed) {
		// The UDP socket was closed without Close, which owns it: nothing
		// more can arrive, so release the conns, the listener and the DHT
		// view instead of leaving them waiting on a dead socket.
		_ = s.Close()
		return false
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-s.done:
		return false
	case <-timer.C:
		return true
	}
}

// nextReadErrorBackoff returns the pause after a failed read given the pause
// after the previous consecutive failure, zero if there was none.
func nextReadErrorBackoff(prev time.Duration) time.Duration {
	if prev < readErrorBackoffMin {
		return readErrorBackoffMin
	}
	return min(2*prev, readErrorBackoffMax)
}

func (s *Socket) handleUTPPacket(data []byte, addr *net.UDPAddr) {
	p, err := parsePacket(data)
	if err != nil {
		return
	}

	// A SYN carries the initiator's recv_id, so our side of that connection is
	// keyed one higher; every other packet is keyed by its own connection id.
	key := newConnKey(addr, p.connID)
	if p.typ == packetTypeSyn {
		key.id++
		s.handleSyn(p, key, addr)
		return
	}

	var (
		c         *Conn
		listener  *Listener
		halfOpen  bool
		sendReset bool
	)
	s.mu.Lock()
	c = s.conns[key]
	if c == nil {
		// Only packets for unknown conns get here, so the half-open lookup,
		// the promotion and the reset budget cost established connections
		// nothing.
		now := time.Now()
		if h := s.liveHalfOpenLocked(key, now); h != nil {
			halfOpen = true
			if h.acknowledgedBy(p) {
				c = s.promoteLocked(h, p.ackNr)
				listener = s.listener
			}
		} else if p.typ != packetTypeReset {
			sendReset = s.allowResetLocked(now)
		}
	}
	s.mu.Unlock()

	if c == nil {
		// A packet for a half-open entry that does not acknowledge our
		// SYN-ACK came from a source that has not shown it receives what we
		// send, so it is dropped without any reply. Other stray packets get
		// a RESET while the budget lasts.
		if sendReset {
			s.writeReset(p, addr)
		}
		return
	}

	c.handlePacket(p)
	if halfOpen {
		// The promoting packet was processed first, so a payload it carries
		// (the BitTorrent handshake or MSE key) is readable by the time the
		// conn is handed out. listener can be nil or closed: the entry may
		// have outlived it.
		if listener == nil || !listener.enqueue(c) {
			c.closeWithError(errListenerClosed, true)
		}
	}
}

// handleSyn answers an inbound SYN. A retransmit for a conn already promoted
// is re-acked by that Conn. Otherwise the SYN is only recorded in the
// half-open table and answered with a SYN-ACK: no Conn exists and nothing
// reaches the listener until the initiator acknowledges it, so a spoofed SYN
// buys one table slot and one 20-byte STATE, never a writable connection.
func (s *Socket) handleSyn(p packet, key connKey, addr *net.UDPAddr) {
	now := time.Now()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	// The existing conn is consulted before the listener: an inbound conn
	// parked at this key owns the connection the SYN belongs to whether or not
	// the listener is still open. TorrentManager.Close closes the listener
	// ahead of the sessions still using conns it handed out, and
	// Listener.Close clears Socket.listener without unregistering accepted
	// conns, so answering a retransmit with a RESET here would carry the
	// initiator's connID and tear down a live inbound stream.
	if existing := s.conns[key]; existing != nil {
		if existing.inbound {
			s.mu.Unlock()
			existing.handlePacket(p)
			return
		}
		// An outbound conn whose recv_id collides with this key is left
		// untouched and the SYN is refused.
		sendReset := s.allowResetLocked(now)
		s.mu.Unlock()
		if sendReset {
			s.writeReset(p, addr)
		}
		return
	}
	h := s.liveHalfOpenLocked(key, now)
	if h == nil || h.synSeq != p.seqNr {
		// A new connection. A SYN with a different seq_nr at a live entry's
		// key is a fresh attempt that reuses the connection id, so it
		// replaces the entry rather than being answered with a SYN-ACK that
		// acks the wrong SYN.
		if s.listener == nil || s.listener.isClosed() {
			sendReset := s.allowResetLocked(now)
			s.mu.Unlock()
			if sendReset {
				s.writeReset(p, addr)
			}
			return
		}
		h = &halfOpenConn{
			key:     key,
			addr:    cloneUDPAddr(addr),
			connID:  p.connID,
			synSeq:  p.seqNr,
			seq:     randomUint16(),
			created: now,
		}
		s.addHalfOpenLocked(h)
	}
	// A retransmit of the same SYN gets the identical SYN-ACK again, in case
	// the first one was lost.
	h.timestampDiff = s.nowMicros() - p.timestamp
	reply := h.synAck(s.nowMicros())
	s.mu.Unlock()
	_ = s.writePacket(reply, addr)
}

// allowResetLocked spends one unit of the unsolicited-RESET budget, reporting
// false once the current second's budget is used up.
func (s *Socket) allowResetLocked(now time.Time) bool {
	if now.Sub(s.resetWindow) >= time.Second {
		s.resetWindow = now
		s.resetsSent = 0
	}
	if s.resetsSent >= maxResetsPerSecond {
		return false
	}
	s.resetsSent++
	return true
}

// writeReset answers p, which belongs to no connection we can serve, with a
// RESET. Callers spend the budget first (allowResetLocked).
func (s *Socket) writeReset(p packet, addr *net.UDPAddr) {
	_ = s.writePacket(packet{
		typ:       packetTypeReset,
		connID:    p.connID,
		timestamp: s.nowMicros(),
		seqNr:     p.ackNr,
		ackNr:     p.seqNr,
	}, addr)
}

// halfOpenConn is an inbound connection whose SYN we answered but whose
// initiator has not yet shown that it receives our packets. It carries only
// what the SYN-ACK and the promoted Conn need.
type halfOpenConn struct {
	key           connKey
	addr          *net.UDPAddr
	connID        uint16 // the SYN's connection id, our send id
	synSeq        uint16
	seq           uint16 // our SYN-ACK's random seq_nr
	timestampDiff uint32
	created       time.Time
}

func (h *halfOpenConn) synAck(now uint32) packet {
	return packet{
		typ:           packetTypeState,
		connID:        h.connID,
		timestamp:     now,
		timestampDiff: h.timestampDiff,
		wndSize:       receiveWindowSize,
		seqNr:         h.seq,
		ackNr:         h.synSeq,
	}
}

// acknowledgedBy reports whether p proves its sender received our SYN-ACK:
// only a host that saw the random seq_nr we sent to addr can ack it. libutp and
// libtorrent initiators ack seq-1, because a SYN-ACK's seq_nr is the next seq
// the acceptor will send; earlier saintTorrent releases ack seq itself. Only
// DATA and STATE complete the handshake, as in libutp.
func (h *halfOpenConn) acknowledgedBy(p packet) bool {
	if p.typ != packetTypeData && p.typ != packetTypeState {
		return false
	}
	return p.ackNr == h.seq-1 || p.ackNr == h.seq
}

// liveHalfOpenLocked returns the half-open entry for key, forgetting it once
// it has expired.
func (s *Socket) liveHalfOpenLocked(key connKey, now time.Time) *halfOpenConn {
	h := s.halfOpen[key]
	if h != nil && now.Sub(h.created) >= halfOpenTimeout {
		delete(s.halfOpen, key)
		return nil
	}
	return h
}

// addHalfOpenLocked records h, evicting the oldest entry when the table is
// full. Every live entry occupies exactly one ring slot, so the table never
// holds more than maxHalfOpen entries.
func (s *Socket) addHalfOpenLocked(h *halfOpenConn) {
	if old := s.halfOpenRing[s.halfOpenNext]; old != nil && s.halfOpen[old.key] == old {
		delete(s.halfOpen, old.key)
	}
	s.halfOpenRing[s.halfOpenNext] = h
	s.halfOpenNext = (s.halfOpenNext + 1) % maxHalfOpen
	s.halfOpen[h.key] = h
}

// promoteLocked turns a half-open entry whose SYN-ACK was acknowledged by ack
// into an established Conn registered for routing.
func (s *Socket) promoteLocked(h *halfOpenConn, ack uint16) *Conn {
	delete(s.halfOpen, h.key)
	if s.closed {
		return nil
	}
	// Our first DATA goes where the initiator expects it: at the SYN-ACK's
	// seq_nr for a libutp-style initiator (ack seq-1), one past it for an
	// earlier saintTorrent initiator, which treated the SYN-ACK as consuming
	// its seq_nr (ack seq).
	localSeq := h.seq
	if ack == h.seq {
		localSeq++
	}
	c := newInboundConn(s, h.addr, h.connID, h.synSeq, localSeq)
	s.conns[h.key] = c
	return c
}

// Close closes the shared UDP socket and every active uTP connection. The DHT
// packet connection is also closed, unblocking DHT reads.
func (s *Socket) Close() error {
	var conns []*Conn
	var listener *Listener
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		for _, c := range s.conns {
			conns = append(conns, c)
		}
		s.conns = make(map[connKey]*Conn)
		clear(s.halfOpen)
		s.halfOpenRing = [maxHalfOpen]*halfOpenConn{}
		listener = s.listener
		s.listener = nil
		close(s.done)
		s.mu.Unlock()

		if listener != nil {
			_ = listener.Close()
		}
		s.dhtConn.Close()
		for _, c := range conns {
			c.closeWithError(net.ErrClosed, false)
		}
		_ = s.conn.Close()
	})
	return nil
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	out := *addr
	if addr.IP != nil {
		out.IP = append(net.IP(nil), addr.IP...)
	}
	if addr.Zone != "" {
		out.Zone = addr.Zone
	}
	return &out
}

// Listener accepts inbound uTP connections as net.Conn values.
type Listener struct {
	socket *Socket

	acceptCh chan *Conn
	closed   chan struct{}
	once     sync.Once
}

// Accept waits for and returns the next inbound uTP connection.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.acceptCh:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	case <-l.socket.done:
		return nil, net.ErrClosed
	}
}

func (l *Listener) enqueue(c *Conn) bool {
	if l.isClosed() {
		return false
	}
	select {
	case <-l.closed:
		return false
	case l.acceptCh <- c:
		return true
	default:
		return false
	}
}

func (l *Listener) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

// Close closes the listener without closing the shared UDP socket. Conns that
// were accepted into the queue but never handed to a caller are drained and
// closed here so their receive buffers (up to the receive window each) are
// released instead of lingering until the whole Socket is closed.
func (l *Listener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		l.socket.mu.Lock()
		if l.socket.listener == l {
			l.socket.listener = nil
		}
		l.socket.mu.Unlock()
		for {
			select {
			case c := <-l.acceptCh:
				c.closeWithError(errListenerClosed, true)
			default:
				return
			}
		}
	})
	return nil
}

// Addr returns the shared UDP socket's local address.
func (l *Listener) Addr() net.Addr {
	return l.socket.localAddr()
}

// PacketConn is the DHT side of a shared uTP/DHT socket.
type PacketConn struct {
	socket *Socket

	incoming chan udpPacket
	closed   chan struct{}
	once     sync.Once

	// dropped counts non-uTP packets discarded because the incoming queue was
	// full. DHT traffic is loss-tolerant, so deliver drops rather than block the
	// shared UDP read loop; this counter is for diagnostics only.
	dropped atomic.Uint64
}

func newPacketConn(socket *Socket) *PacketConn {
	return &PacketConn{
		socket:   socket,
		incoming: make(chan udpPacket, dhtQueueSize),
		closed:   make(chan struct{}),
	}
}

// ReadFromUDP reads the next non-uTP UDP packet.
func (c *PacketConn) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	select {
	case pkt := <-c.incoming:
		return copy(b, pkt.data), cloneUDPAddr(pkt.addr), nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	case <-c.socket.done:
		return 0, nil, net.ErrClosed
	}
}

// WriteToUDP writes a DHT packet through the shared UDP socket.
func (c *PacketConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.socket.done:
		return 0, net.ErrClosed
	default:
	}
	return c.socket.conn.WriteToUDP(b, addr)
}

// LocalAddr returns the shared UDP socket's local address.
func (c *PacketConn) LocalAddr() net.Addr {
	return c.socket.localAddr()
}

// Close closes the DHT view of the socket without closing the shared UDP socket.
func (c *PacketConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})
	return nil
}

// deliver hands a non-uTP packet to the DHT consumer. It runs on the shared UDP
// read loop, the only goroutine reading the socket, so it must never block: if
// the incoming queue is full it drops the packet (DHT traffic is loss-tolerant
// by design) instead of head-of-line blocking throughput-critical uTP data and
// ACK processing on a slow or stalled DHT consumer.
func (c *PacketConn) deliver(pkt udpPacket) {
	select {
	case <-c.closed:
	case <-c.socket.done:
	case c.incoming <- pkt:
	default:
		c.dropped.Add(1)
	}
}

// DroppedPackets returns the number of non-uTP packets dropped because the DHT
// queue was full. Exposed for diagnostics.
func (c *PacketConn) DroppedPackets() uint64 {
	return c.dropped.Load()
}
