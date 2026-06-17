package utp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const (
	initialRetransmitTimeout = 300 * time.Millisecond
	maxRetransmitTimeout     = 2 * time.Second
	receiveWindowSize        = 1 << 20
	sendWindowSize           = 1 << 20
)

var errReset = errors.New("utp: connection reset")

type timeoutError struct{}

func (timeoutError) Error() string   { return "utp: i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// Conn is a BEP 29 uTP stream exposed as a net.Conn.
type Conn struct {
	socket *Socket
	remote *net.UDPAddr

	sendID uint16
	recvID uint16

	mu                sync.Mutex
	localSeq          uint16
	remoteSeq         uint16
	remoteSeqSet      bool
	stateSent         bool
	established       chan struct{}
	establishedClosed bool
	establishErr      error
	accepted          bool
	pending           map[uint16][]byte
	pendingBytes      int
	pendingFin        bool
	pendingFinSeq     uint16
	readBuf           bytes.Buffer
	remoteClosed      bool
	closed            bool
	closeErr          error
	waiters           map[uint16]chan struct{}
	lastTimestampDiff uint32
	readDeadline      time.Time
	writeDeadline     time.Time
	readNotify        chan struct{}
	readDeadlineSet   chan struct{}
	writeDeadlineSet  chan struct{}
	done              chan struct{}
	closeOnce         sync.Once
}

func newOutboundConn(socket *Socket, remote *net.UDPAddr, baseID uint16) *Conn {
	seq := randomUint16()
	return newConn(socket, remote, baseID+1, baseID, seq, 0, false)
}

func newInboundConn(socket *Socket, remote *net.UDPAddr, recvID uint16, remoteSeq uint16) *Conn {
	c := newConn(socket, remote, recvID, recvID+1, randomUint16(), remoteSeq, true)
	c.establishedClosed = true
	close(c.established)
	return c
}

func newConn(socket *Socket, remote *net.UDPAddr, sendID, recvID, localSeq, remoteSeq uint16, remoteSeqSet bool) *Conn {
	return &Conn{
		socket:           socket,
		remote:           cloneUDPAddr(remote),
		sendID:           sendID,
		recvID:           recvID,
		localSeq:         localSeq,
		remoteSeq:        remoteSeq,
		remoteSeqSet:     remoteSeqSet,
		established:      make(chan struct{}),
		pending:          make(map[uint16][]byte),
		waiters:          make(map[uint16]chan struct{}),
		readNotify:       make(chan struct{}, 1),
		readDeadlineSet:  make(chan struct{}, 1),
		writeDeadlineSet: make(chan struct{}, 1),
		done:             make(chan struct{}),
	}
}

func (c *Conn) dial(ctx context.Context) error {
	for {
		c.mu.Lock()
		if c.closed {
			err := c.closeErr
			c.mu.Unlock()
			if err == nil {
				err = net.ErrClosed
			}
			return err
		}
		p := c.packetLocked(packetTypeSyn, c.localSeq, nil)
		c.mu.Unlock()

		if err := c.socket.writePacket(p, c.remote); err != nil {
			return err
		}

		timer := time.NewTimer(initialRetransmitTimeout)
		select {
		case <-c.established:
			timer.Stop()
			c.mu.Lock()
			err := c.establishErr
			c.mu.Unlock()
			return err
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-c.done:
			timer.Stop()
			c.mu.Lock()
			err := c.closeErr
			c.mu.Unlock()
			if err == nil {
				err = net.ErrClosed
			}
			return err
		case <-timer.C:
		}
	}
}

func (c *Conn) handlePacket(p packet) {
	if p.typ != packetTypeSyn {
		c.mu.Lock()
		c.processAckLocked(p.ackNr)
		c.mu.Unlock()
	}

	ackAfter := false
	switch p.typ {
	case packetTypeSyn:
		ackAfter = c.handleSyn(p)
	case packetTypeState:
		c.handleState(p)
	case packetTypeData:
		ackAfter = c.handleData(p)
	case packetTypeFin:
		ackAfter = c.handleFin(p)
	case packetTypeReset:
		c.closeWithError(errReset, false)
	}
	if ackAfter {
		c.sendState()
	}
}

func (c *Conn) handleSyn(p packet) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.updateTimestampDiffLocked(p)
	c.remoteSeq = p.seqNr
	c.remoteSeqSet = true
	if !c.stateSent {
		c.stateSent = true
		c.localSeq++
	}
	return true
}

func (c *Conn) handleState(p packet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.updateTimestampDiffLocked(p)
	if !c.establishedClosed {
		c.remoteSeq = p.seqNr
		c.remoteSeqSet = true
		c.localSeq++
		c.establishedClosed = true
		close(c.established)
	}
}

func (c *Conn) handleData(p packet) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.updateTimestampDiffLocked(p)
	if !c.remoteSeqSet {
		c.remoteSeq = p.seqNr - 1
		c.remoteSeqSet = true
	}
	next := c.remoteSeq + 1
	switch {
	case p.seqNr == next:
		if len(p.payload) > 0 {
			if !c.canBufferLocked(len(p.payload)) {
				return false
			}
			_, _ = c.readBuf.Write(p.payload)
			c.signalReadLocked()
		}
		c.remoteSeq = p.seqNr
		for {
			next = c.remoteSeq + 1
			payload, ok := c.pending[next]
			if !ok {
				break
			}
			delete(c.pending, next)
			c.pendingBytes -= len(payload)
			if len(payload) > 0 {
				_, _ = c.readBuf.Write(payload)
				c.signalReadLocked()
			}
			c.remoteSeq = next
		}
		c.applyPendingFinLocked()
	case seqLT(next, p.seqNr):
		if _, exists := c.pending[p.seqNr]; !exists && c.canBufferLocked(len(p.payload)) {
			c.pending[p.seqNr] = append([]byte(nil), p.payload...)
			c.pendingBytes += len(p.payload)
		}
	}
	return true
}

func (c *Conn) handleFin(p packet) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.updateTimestampDiffLocked(p)
	if !c.remoteSeqSet || p.seqNr == c.remoteSeq+1 || seqLTE(p.seqNr, c.remoteSeq) {
		if !c.remoteSeqSet || p.seqNr == c.remoteSeq+1 {
			c.remoteSeq = p.seqNr
			c.remoteSeqSet = true
		}
		c.remoteClosed = true
		c.signalReadLocked()
	} else if !c.pendingFin || seqLT(p.seqNr, c.pendingFinSeq) {
		c.pendingFin = true
		c.pendingFinSeq = p.seqNr
	}
	return true
}

func (c *Conn) applyPendingFinLocked() {
	if c.pendingFin && c.pendingFinSeq == c.remoteSeq+1 {
		c.remoteSeq = c.pendingFinSeq
		c.pendingFin = false
		c.remoteClosed = true
		c.signalReadLocked()
	}
}

func (c *Conn) canBufferLocked(n int) bool {
	return n >= 0 && c.readBuf.Len()+c.pendingBytes+n <= receiveWindowSize
}

func (c *Conn) updateTimestampDiffLocked(p packet) {
	c.lastTimestampDiff = c.socket.nowMicros() - p.timestamp
}

func (c *Conn) processAckLocked(ack uint16) {
	for seq, ch := range c.waiters {
		if seqLTE(seq, ack) {
			close(ch)
			delete(c.waiters, seq)
		}
	}
}

func (c *Conn) packetLocked(typ packetType, seq uint16, payload []byte) packet {
	connID := c.sendID
	if typ == packetTypeSyn {
		connID = c.recvID
	}
	return packet{
		typ:           typ,
		connID:        connID,
		timestamp:     c.socket.nowMicros(),
		timestampDiff: c.lastTimestampDiff,
		wndSize:       uint32(max(0, receiveWindowSize-c.readBuf.Len()-c.pendingBytes)),
		seqNr:         seq,
		ackNr:         c.remoteSeq,
		payload:       payload,
	}
}

func (c *Conn) packetForSeq(typ packetType, seq uint16, payload []byte) packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.packetLocked(typ, seq, payload)
}

func (c *Conn) sendState() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	seq := c.localSeq - 1
	p := c.packetLocked(packetTypeState, seq, nil)
	c.mu.Unlock()
	_ = c.socket.writePacket(p, c.remote)
}

// Read implements net.Conn.
func (c *Conn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		c.mu.Lock()
		if c.readBuf.Len() > 0 {
			n, _ := c.readBuf.Read(b)
			c.mu.Unlock()
			c.sendState()
			return n, nil
		}
		if c.remoteClosed {
			c.mu.Unlock()
			return 0, io.EOF
		}
		if c.closed {
			err := c.closeErr
			c.mu.Unlock()
			if err == nil {
				err = net.ErrClosed
			}
			return 0, err
		}
		deadline := c.readDeadline
		c.mu.Unlock()

		if !deadline.IsZero() && time.Now().After(deadline) {
			return 0, timeoutError{}
		}

		var deadlineC <-chan time.Time
		var deadlineTimer *time.Timer
		if !deadline.IsZero() {
			deadlineTimer = time.NewTimer(time.Until(deadline))
			deadlineC = deadlineTimer.C
		}

		select {
		case <-c.readNotify:
		case <-c.readDeadlineSet:
		case <-c.done:
		case <-deadlineC:
			if deadlineTimer != nil {
				deadlineTimer.Stop()
			}
			return 0, timeoutError{}
		}
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
	}
}

// Write implements net.Conn.
func (c *Conn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if err := c.waitEstablishedForWrite(); err != nil {
		return 0, err
	}

	written := 0
	outstanding := make([]outPacket, 0, (sendWindowSize/maxPayloadSize)+1)
	outstandingBytes := 0
	for written < len(b) {
		for written < len(b) && outstandingBytes < sendWindowSize {
			end := written + maxPayloadSize
			if end > len(b) {
				end = len(b)
			}
			chunk := append([]byte(nil), b[written:end]...)
			if outstandingBytes+len(chunk) > sendWindowSize && len(outstanding) > 0 {
				break
			}

			pkt, err := c.queueWritePacket(chunk)
			if err != nil {
				c.removeOutstanding(outstanding)
				return written - outstandingBytes, err
			}
			if err := c.socket.writePacket(pkt.packet, c.remote); err != nil {
				c.removeWaiter(pkt.seq, pkt.waiter)
				c.removeOutstanding(outstanding)
				return written - outstandingBytes, err
			}
			outstanding = append(outstanding, pkt)
			outstandingBytes += len(chunk)
			written = end
		}

		if len(outstanding) == 0 {
			continue
		}
		first := outstanding[0]
		if err := c.waitAck(first.seq, first.waiter, first.payload); err != nil {
			c.removeWaiter(first.seq, first.waiter)
			c.removeOutstanding(outstanding[1:])
			return written - outstandingBytes, err
		}
		outstandingBytes -= len(first.payload)
		outstanding = outstanding[1:]
	}
	for len(outstanding) > 0 {
		first := outstanding[0]
		if err := c.waitAck(first.seq, first.waiter, first.payload); err != nil {
			c.removeWaiter(first.seq, first.waiter)
			c.removeOutstanding(outstanding[1:])
			return written - outstandingBytes, err
		}
		outstandingBytes -= len(first.payload)
		outstanding = outstanding[1:]
	}
	return written, nil
}

type outPacket struct {
	seq     uint16
	payload []byte
	waiter  chan struct{}
	packet  packet
}

func (c *Conn) waitEstablishedForWrite() error {
	for {
		c.mu.Lock()
		if c.establishedClosed {
			err := c.establishErr
			c.mu.Unlock()
			return err
		}
		if c.closed {
			err := c.closeErr
			c.mu.Unlock()
			if err == nil {
				err = net.ErrClosed
			}
			return err
		}
		deadline := c.writeDeadline
		c.mu.Unlock()

		if !deadline.IsZero() && time.Now().After(deadline) {
			return timeoutError{}
		}

		var deadlineC <-chan time.Time
		var deadlineTimer *time.Timer
		if !deadline.IsZero() {
			deadlineTimer = time.NewTimer(time.Until(deadline))
			deadlineC = deadlineTimer.C
		}
		select {
		case <-c.established:
			stopTimer(deadlineTimer)
			return c.errIfClosed()
		case <-c.writeDeadlineSet:
			stopTimer(deadlineTimer)
		case <-c.done:
			stopTimer(deadlineTimer)
			return c.currentErr()
		case <-deadlineC:
			stopTimer(deadlineTimer)
			return timeoutError{}
		}
	}
}

func (c *Conn) queueWritePacket(chunk []byte) (outPacket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		if c.closeErr != nil {
			return outPacket{}, c.closeErr
		}
		return outPacket{}, net.ErrClosed
	}
	if !c.writeDeadline.IsZero() && time.Now().After(c.writeDeadline) {
		return outPacket{}, timeoutError{}
	}
	seq := c.localSeq
	c.localSeq++
	waiter := make(chan struct{})
	c.waiters[seq] = waiter
	return outPacket{
		seq:     seq,
		payload: chunk,
		waiter:  waiter,
		packet:  c.packetLocked(packetTypeData, seq, chunk),
	}, nil
}

func (c *Conn) waitAck(seq uint16, waiter <-chan struct{}, payload []byte) error {
	timeout := initialRetransmitTimeout
	for {
		deadline := c.writeDeadlineSnapshot()
		if !deadline.IsZero() && time.Now().After(deadline) {
			return timeoutError{}
		}

		retryTimer := time.NewTimer(timeout)
		var deadlineC <-chan time.Time
		var deadlineTimer *time.Timer
		if !deadline.IsZero() {
			deadlineTimer = time.NewTimer(time.Until(deadline))
			deadlineC = deadlineTimer.C
		}

		select {
		case <-waiter:
			stopTimer(retryTimer)
			stopTimer(deadlineTimer)
			if err := c.errIfClosed(); err != nil {
				return err
			}
			return nil
		case <-retryTimer.C:
			stopTimer(deadlineTimer)
			p := c.packetForSeq(packetTypeData, seq, payload)
			if err := c.socket.writePacket(p, c.remote); err != nil {
				return err
			}
			timeout *= 2
			if timeout > maxRetransmitTimeout {
				timeout = maxRetransmitTimeout
			}
		case <-c.writeDeadlineSet:
			stopTimer(retryTimer)
			stopTimer(deadlineTimer)
		case <-c.done:
			stopTimer(retryTimer)
			stopTimer(deadlineTimer)
			return c.currentErr()
		case <-deadlineC:
			stopTimer(retryTimer)
			stopTimer(deadlineTimer)
			return timeoutError{}
		}
	}
}

func stopTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func (c *Conn) removeWaiter(seq uint16, waiter <-chan struct{}) {
	c.mu.Lock()
	if current, ok := c.waiters[seq]; ok && current == waiter {
		delete(c.waiters, seq)
	}
	c.mu.Unlock()
}

func (c *Conn) removeOutstanding(outstanding []outPacket) {
	for _, pkt := range outstanding {
		c.removeWaiter(pkt.seq, pkt.waiter)
	}
}

func (c *Conn) writeDeadlineSnapshot() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeDeadline
}

func (c *Conn) currentErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return net.ErrClosed
}

func (c *Conn) errIfClosed() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		return nil
	}
	if c.closeErr != nil {
		return c.closeErr
	}
	return net.ErrClosed
}

// Close implements net.Conn.
func (c *Conn) Close() error {
	c.closeWithError(net.ErrClosed, true)
	return nil
}

func (c *Conn) closeWithError(err error, sendFin bool) {
	var fin *packet
	c.closeOnce.Do(func() {
		c.mu.Lock()
		if err == nil {
			err = net.ErrClosed
		}
		c.closed = true
		c.closeErr = err
		if sendFin && c.establishedClosed && !c.remoteClosed {
			seq := c.localSeq
			c.localSeq++
			p := c.packetLocked(packetTypeFin, seq, nil)
			fin = &p
		}
		for seq, ch := range c.waiters {
			close(ch)
			delete(c.waiters, seq)
		}
		if !c.establishedClosed {
			c.establishErr = err
			c.establishedClosed = true
			close(c.established)
		}
		close(c.done)
		c.signalReadLocked()
		c.signalReadDeadlineLocked()
		c.signalWriteDeadlineLocked()
		c.mu.Unlock()

		if fin != nil {
			_ = c.socket.writePacket(*fin, c.remote)
		}
		c.socket.unregister(c)
	})
}

func (c *Conn) isAccepted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accepted
}

func (c *Conn) markAccepted() {
	c.mu.Lock()
	c.accepted = true
	c.mu.Unlock()
}

func (c *Conn) signalReadLocked() {
	select {
	case c.readNotify <- struct{}{}:
	default:
	}
}

func (c *Conn) signalReadDeadlineLocked() {
	select {
	case c.readDeadlineSet <- struct{}{}:
	default:
	}
}

func (c *Conn) signalWriteDeadlineLocked() {
	select {
	case c.writeDeadlineSet <- struct{}{}:
	default:
	}
}

// LocalAddr implements net.Conn.
func (c *Conn) LocalAddr() net.Addr {
	return c.socket.localAddr()
}

// RemoteAddr implements net.Conn.
func (c *Conn) RemoteAddr() net.Addr {
	return cloneUDPAddr(c.remote)
}

// SetDeadline implements net.Conn.
func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.writeDeadline = t
	c.signalReadDeadlineLocked()
	c.signalWriteDeadlineLocked()
	c.mu.Unlock()
	return nil
}

// SetReadDeadline implements net.Conn.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.signalReadDeadlineLocked()
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline implements net.Conn.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.signalWriteDeadlineLocked()
	c.mu.Unlock()
	return nil
}

func randomUint16() uint16 {
	var buf [2]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return binary.BigEndian.Uint16(buf[:])
	}
	return uint16(time.Now().UnixNano())
}
