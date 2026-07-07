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

	// ackCoalesceCount is how many in-order data packets we let accumulate
	// before forcing a STATE. Acking every other packet halves the ack
	// syscalls on receive without slowing the ack clock: the coalescing is
	// count-based (never a fixed delay in steady state), so it cannot cap
	// throughput the way a delayed-ack timer would on a low-latency link.
	ackCoalesceCount = 2
	// delayedAckTimeout bounds how long a lone in-order packet's ack is held
	// waiting for a follow-up packet to coalesce with. It only fires when the
	// stream pauses on an odd packet; it is well under the retransmit timeout
	// so it never provokes a spurious retransmit.
	delayedAckTimeout = 5 * time.Millisecond
)

// ackDisposition tells handlePacket whether and how promptly a received packet
// must be acknowledged.
type ackDisposition int

const (
	ackNone      ackDisposition = iota // no STATE owed (e.g. closed, or receive window full)
	ackCoalesce                        // in-order data: may be batched with the next ack
	ackImmediate                       // out-of-order/duplicate/control: ack right away
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
	waiterBase        uint16 // oldest seq that may still have a waiter; ack processing walks forward from here
	unsentAcks        int    // in-order data packets received since the last STATE we sent
	ackTimer          *time.Timer
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

	switch p.typ {
	case packetTypeSyn:
		if c.handleSyn(p) {
			c.flushAck()
		}
	case packetTypeState:
		c.handleState(p)
	case packetTypeData:
		switch c.handleData(p) {
		case ackImmediate:
			c.flushAck()
		case ackCoalesce:
			c.scheduleAck()
		}
	case packetTypeFin:
		if c.handleFin(p) {
			c.flushAck()
		}
	case packetTypeReset:
		c.closeWithError(errReset, false)
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

func (c *Conn) handleData(p packet) ackDisposition {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ackNone
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
				// Receive window is full: drop without acking so the sender
				// backs off and retransmits once the app drains the buffer.
				return ackNone
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
		return ackCoalesce
	case seqLT(next, p.seqNr):
		if _, exists := c.pending[p.seqNr]; !exists && c.canBufferLocked(len(p.payload)) {
			c.pending[p.seqNr] = append([]byte(nil), p.payload...)
			c.pendingBytes += len(p.payload)
		}
		// A gap means loss: ack immediately so the sender sees the duplicate
		// ack and can retransmit without waiting on its timer.
		return ackImmediate
	default:
		// Old/duplicate packet: ack immediately in case our earlier ack was
		// lost. This is rare and off the steady-state path.
		return ackImmediate
	}
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

// processAckLocked wakes every write waiter whose sequence number is covered by
// the cumulative ack. Waiters are assigned in increasing seq order, so instead
// of scanning the whole map (O(window) per incoming packet) we walk forward
// from the oldest outstanding seq and touch only the newly-acked entries. The
// walk is bounded by localSeq so a bogus far-future ack cannot loop.
func (c *Conn) processAckLocked(ack uint16) {
	if len(c.waiters) == 0 {
		return
	}
	base := c.waiterBase
	for seqLTE(base, ack) && seqLT(base, c.localSeq) {
		if ch, ok := c.waiters[base]; ok {
			close(ch)
			delete(c.waiters, base)
		}
		base++
	}
	c.waiterBase = base
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
		wndSize:       uint32(c.availableWindowLocked()),
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

func (c *Conn) availableWindowLocked() int {
	w := receiveWindowSize - c.readBuf.Len() - c.pendingBytes
	if w < 0 {
		return 0
	}
	return w
}

// flushAck sends a STATE now and clears any coalesced/held ack. Used for
// control packets, out-of-order data, and receive-window updates that must
// reach the peer promptly.
func (c *Conn) flushAck() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.unsentAcks = 0
	if c.ackTimer != nil {
		c.ackTimer.Stop()
	}
	seq := c.localSeq - 1
	p := c.packetLocked(packetTypeState, seq, nil)
	c.mu.Unlock()
	_ = c.socket.writePacket(p, c.remote)
}

// scheduleAck records an in-order data packet and acks in bursts: it sends a
// STATE immediately once ackCoalesceCount packets have accumulated (no time
// delay, so the ack clock is not slowed), otherwise it arms a short timer to
// flush a lone trailing ack if the stream pauses.
func (c *Conn) scheduleAck() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.unsentAcks++
	if c.unsentAcks >= ackCoalesceCount {
		c.unsentAcks = 0
		if c.ackTimer != nil {
			c.ackTimer.Stop()
		}
		seq := c.localSeq - 1
		p := c.packetLocked(packetTypeState, seq, nil)
		c.mu.Unlock()
		_ = c.socket.writePacket(p, c.remote)
		return
	}
	c.armAckTimerLocked()
	c.mu.Unlock()
}

func (c *Conn) armAckTimerLocked() {
	if c.ackTimer == nil {
		c.ackTimer = time.AfterFunc(delayedAckTimeout, c.flushDelayedAck)
		return
	}
	// The timer is not running here (a prior flush stopped it or it fired and
	// reset unsentAcks), so reusing it avoids allocating a timer per hold.
	c.ackTimer.Reset(delayedAckTimeout)
}

// flushDelayedAck runs from the ack timer and sends the held STATE. It is
// idempotent: if the ack was already flushed (unsentAcks == 0) or the conn is
// closed it does nothing, so a race with scheduleAck cannot double-ack.
func (c *Conn) flushDelayedAck() {
	c.mu.Lock()
	if c.closed || c.unsentAcks == 0 {
		c.mu.Unlock()
		return
	}
	c.unsentAcks = 0
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
			// Only advertise a reopened window when it was small enough that
			// the sender may have stalled on it (below one packet). In the
			// common fast-reader case the window never shrinks that far, so we
			// skip the per-Read STATE entirely; when it does, the retransmit
			// timer is the correctness backstop regardless.
			wndBefore := c.availableWindowLocked()
			n, _ := c.readBuf.Read(b)
			sendUpdate := wndBefore < maxPayloadSize && c.availableWindowLocked() >= maxPayloadSize
			c.mu.Unlock()
			if sendUpdate {
				c.flushAck()
			}
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
	if len(c.waiters) == 0 {
		// First outstanding packet in this batch: anchor the ack walk here.
		c.waiterBase = seq
	}
	c.waiters[seq] = waiter
	return outPacket{
		seq:     seq,
		payload: chunk,
		waiter:  waiter,
		packet:  c.packetLocked(packetTypeData, seq, chunk),
	}, nil
}

func (c *Conn) waitAck(seq uint16, waiter <-chan struct{}, payload []byte) error {
	// One retransmit timer (and at most one deadline timer) is allocated for
	// the whole wait and reset each iteration, instead of allocating fresh
	// timers per loop — up to two per in-flight packet — which churns the
	// runtime timer heap proportionally to packets sent.
	timeout := initialRetransmitTimeout
	retryTimer := time.NewTimer(timeout)
	defer stopTimer(retryTimer)
	var deadlineTimer *time.Timer
	defer func() { stopTimer(deadlineTimer) }()

	first := true
	for {
		deadline := c.writeDeadlineSnapshot()
		if !deadline.IsZero() && time.Now().After(deadline) {
			return timeoutError{}
		}
		if first {
			first = false
		} else {
			resetTimer(retryTimer, timeout)
		}

		var deadlineC <-chan time.Time
		if !deadline.IsZero() {
			if deadlineTimer == nil {
				deadlineTimer = time.NewTimer(time.Until(deadline))
			} else {
				resetTimer(deadlineTimer, time.Until(deadline))
			}
			deadlineC = deadlineTimer.C
		}

		select {
		case <-waiter:
			return c.errIfClosed()
		case <-retryTimer.C:
			p := c.packetForSeq(packetTypeData, seq, payload)
			if err := c.socket.writePacket(p, c.remote); err != nil {
				return err
			}
			timeout *= 2
			if timeout > maxRetransmitTimeout {
				timeout = maxRetransmitTimeout
			}
		case <-c.writeDeadlineSet:
			// Deadline changed; loop re-reads it and re-arms the timers.
		case <-c.done:
			return c.currentErr()
		case <-deadlineC:
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

// resetTimer safely re-arms a running or already-fired timer for a new
// duration, draining a pending fire so the next select sees only the new one.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
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
		if c.ackTimer != nil {
			c.ackTimer.Stop()
		}
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
