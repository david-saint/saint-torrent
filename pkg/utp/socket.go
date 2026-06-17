package utp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	dhtQueueSize    = 1024
	acceptQueueSize = 128
)

var errListenerClosed = errors.New("utp: listener closed")

type udpPacket struct {
	data []byte
	addr *net.UDPAddr
}

type connKey struct {
	addr string
	id   uint16
}

// Socket owns one UDP socket and demultiplexes BEP 29 uTP packets from DHT
// packets. uTP packets are routed to Conn values; every non-uTP packet is exposed
// through DHTConn so the DHT and uTP can share one UDP port.
type Socket struct {
	conn *net.UDPConn

	mu       sync.Mutex
	conns    map[connKey]*Conn
	listener *Listener
	closed   bool

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
	s := &Socket{
		conn:  conn,
		conns: make(map[connKey]*Conn),
		done:  make(chan struct{}),
	}
	s.dhtConn = newPacketConn(s)
	go s.readLoop()
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
	key := connKey{addr: c.remote.String(), id: c.recvID}
	if _, exists := s.conns[key]; exists {
		return fmt.Errorf("utp: connection id collision for %s", c.remote)
	}
	s.conns[key] = c
	return nil
}

func (s *Socket) unregister(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := connKey{addr: c.remote.String(), id: c.recvID}
	if current := s.conns[key]; current == c {
		delete(s.conns, key)
	}
}

func (s *Socket) writePacket(p packet, addr *net.UDPAddr) error {
	_, err := s.conn.WriteToUDP(p.marshal(), addr)
	return err
}

func (s *Socket) readLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				s.Close()
				return
			}
		}
		data := append([]byte(nil), buf[:n]...)
		if IsPacket(data) {
			s.handleUTPPacket(data, addr)
			continue
		}
		s.dhtConn.deliver(udpPacket{data: data, addr: cloneUDPAddr(addr)})
	}
}

func (s *Socket) handleUTPPacket(data []byte, addr *net.UDPAddr) {
	p, err := parsePacket(data)
	if err != nil {
		return
	}

	key := connKey{addr: addr.String(), id: p.connID}
	s.mu.Lock()
	c := s.conns[key]
	listener := s.listener
	closed := s.closed
	if c == nil && !closed && p.typ == packetTypeSyn && listener != nil && !listener.isClosed() {
		recvKey := connKey{addr: addr.String(), id: p.connID + 1}
		c = s.conns[recvKey]
		if c == nil {
			c = newInboundConn(s, cloneUDPAddr(addr), p.connID, p.seqNr)
			s.conns[recvKey] = c
		}
	}
	s.mu.Unlock()

	if c == nil {
		if p.typ != packetTypeReset {
			_ = s.writePacket(packet{
				typ:       packetTypeReset,
				connID:    p.connID,
				timestamp: s.nowMicros(),
				seqNr:     p.ackNr,
				ackNr:     p.seqNr,
			}, addr)
		}
		return
	}

	wasAccepted := c.isAccepted()
	c.handlePacket(p)
	if p.typ == packetTypeSyn && !wasAccepted {
		if listener == nil || !listener.enqueue(c) {
			c.closeWithError(errListenerClosed, true)
		} else {
			c.markAccepted()
		}
	}
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

// Close closes the listener without closing the shared UDP socket.
func (l *Listener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		l.socket.mu.Lock()
		if l.socket.listener == l {
			l.socket.listener = nil
		}
		l.socket.mu.Unlock()
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

func (c *PacketConn) deliver(pkt udpPacket) {
	select {
	case <-c.closed:
	case <-c.socket.done:
	case c.incoming <- pkt:
	}
}
