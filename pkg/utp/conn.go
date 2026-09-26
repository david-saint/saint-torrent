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

	// ackNrSlack is how far an ack_nr may trail the highest one seen and
	// still be believed. libutp and libtorrent allow 3; a little more lets a
	// FIN or RESET reordered behind a few STATEs through, while a blind
	// injector still has to hit a window of a few dozen values in 65536.
	ackNrSlack = 16
	// maxReorderDistance bounds how far past the next expected seq_nr an
	// out-of-order packet is buffered. Our sender keeps at most
	// sendWindowSize/maxPayloadSize (~890) packets in flight and a 1 MiB
	// window of minimum-MTU packets is ~2000, so no legitimate packet is
	// dropped while the pending map is capped at 4096 entries per conn.
	maxReorderDistance = 4096
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
//
// Sequence numbers follow libutp and libtorrent, which BEP 29's pseudo-code
// does not pin down: localSeq is always the seq_nr the next DATA or FIN will
// consume (a SYN consumes one too), and a STATE carries localSeq without
// consuming it. A SYN-ACK's seq_nr is therefore the acceptor's first DATA
// seq_nr, and the initiator acks seq_nr-1 until that DATA arrives. remoteSeq
// is the last in-order seq_nr received, which every packet acks.
type Conn struct {
	socket *Socket
	remote *net.UDPAddr

	sendID uint16
	recvID uint16
	// inbound marks a Conn created by newInboundConn. It is set before the
	// Conn is published to Socket.conns and never mutated afterwards.
	inbound bool

	mu        sync.Mutex
	localSeq  uint16
	remoteSeq uint16
	// lastAck is the highest ack_nr the peer has sent, i.e. everything up to
	// it has been received. Only acks in [lastAck-ackNrSlack, localSeq-1]
	// can come from a peer that receives our packets (see ackInRangeLocked).
	lastAck           uint16
	established       chan struct{}
	establishedClosed bool
	establishErr      error
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
	// The SYN consumes the random initial seq_nr (see dial) and is not yet
	// acknowledged.
	synSeq := randomUint16()
	c := newConn(socket, remote, baseID+1, baseID, synSeq+1, 0)
	c.lastAck = synSeq - 1
	return c
}

// newInboundConn builds the Conn for an inbound connection whose initiator has
// acknowledged our SYN-ACK (see Socket.promoteLocked), so it starts
// established. synConnID and synSeq come from the initiator's SYN; localSeq is
// the seq_nr our first DATA will carry.
func newInboundConn(socket *Socket, remote *net.UDPAddr, synConnID, synSeq, localSeq uint16) *Conn {
	c := newConn(socket, remote, synConnID, synConnID+1, localSeq, synSeq)
	c.inbound = true
	// Nothing of ours needs acking yet: the initiator's first ack is the
	// localSeq-1 that promoted the conn.
	c.lastAck = localSeq - 1
	c.establishedClosed = true
	close(c.established)
	return c
}

func newConn(socket *Socket, remote *net.UDPAddr, sendID, recvID, localSeq, remoteSeq uint16) *Conn {
	return &Conn{
		socket:           socket,
		remote:           cloneUDPAddr(remote),
		sendID:           sendID,
		recvID:           recvID,
		localSeq:         localSeq,
		remoteSeq:        remoteSeq,
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
		p := c.packetLocked(packetTypeSyn, c.localSeq-1, nil)
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
	switch p.typ {
	case packetTypeSyn:
		if c.handleSyn(p) {
			c.flushAck()
		}
		return
	case packetTypeReset:
		if c.resetPlausible(p.ackNr) {
			c.closeWithError(errReset, false)
		}
		return
	}

	// An ack beyond what we sent, or far behind what the peer already acked,
	// is not from a peer that receives our packets: acting on it would let a
	// blind injector complete our writes. It is ignored, but in-order payload
	// on the same packet is still delivered; a FIN with such an ack is dropped
	// like a RESET would be.
	c.mu.Lock()
	ackOK := c.ackInRangeLocked(p.ackNr, c.localSeq-1)
	if ackOK {
		c.processAckLocked(p.ackNr)
	}
	c.mu.Unlock()

	switch p.typ {
	case packetTypeState:
		if ackOK && c.handleState(p) {
			// Complete the handshake at once, like TCP's final ACK: an
			// acceptor keeps the conn half-open until something acks its
			// SYN-ACK, so this lets it hand the conn to Accept before our
			// first write.
			c.flushAck()
		}
	case packetTypeData:
		switch c.handleData(p) {
		case ackImmediate:
			c.flushAck()
		case ackCoalesce:
			c.scheduleAck()
		}
	case packetTypeFin:
		if ackOK && c.handleFin(p) {
			c.flushAck()
		}
	}
}

// ackInRangeLocked reports whether ack lies in [lastAck-ackNrSlack, hi]. The
// uint16 subtraction keeps the window test correct across wraparound.
func (c *Conn) ackInRangeLocked(ack, hi uint16) bool {
	lo := c.lastAck - ackNrSlack
	return ack-lo <= hi-lo
}

// resetPlausible reports whether a RESET's ack_nr names a packet we sent. A
// peer that lost our connection resets it with ack_nr set to the seq_nr of
// the packet it could not place, which is at most localSeq (the seq_nr our
// STATEs carry); anything else is a blind guess at the connection id.
func (c *Conn) resetPlausible(ack uint16) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ackInRangeLocked(ack, c.localSeq)
}

// handleSyn is meaningful only for an inbound conn; a SYN reaching any other
// Conn is ignored. The first SYN was answered while the connection was still
// half-open, so one reaching the Conn is a retransmit and only needs a re-ack.
// It reports whether a STATE ack is owed.
func (c *Conn) handleSyn(p packet) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.inbound {
		return false
	}
	c.updateTimestampDiffLocked(p)
	return true
}

// handleState reports whether p was the SYN-ACK that established this
// outbound conn: only a STATE acking our SYN's own seq_nr is.
func (c *Conn) handleState(p packet) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.updateTimestampDiffLocked(p)
	if c.establishedClosed || p.ackNr != c.localSeq-1 {
		return false
	}
	// The SYN-ACK's seq_nr is the acceptor's first DATA seq_nr, so
	// everything before it counts as received.
	c.remoteSeq = p.seqNr - 1
	c.establishedClosed = true
	close(c.established)
	return true
}

// inReorderWindowLocked reports whether seq is ahead of the next expected
// seq_nr by no more than maxReorderDistance.
func (c *Conn) inReorderWindowLocked(seq uint16) bool {
	d := seq - c.remoteSeq
	return d > 1 && d <= maxReorderDistance
}

func (c *Conn) handleData(p packet) ackDisposition {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Until the SYN-ACK arrives there is no stream position to place DATA at.
	if c.closed || !c.establishedClosed {
		return ackNone
	}
	c.updateTimestampDiffLocked(p)
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
		if !c.inReorderWindowLocked(p.seqNr) {
			// Further ahead than any window a real sender keeps in flight:
			// buffering it would only let a peer grow the pending map with
			// tiny packets, so it is dropped without an ack.
			return ackNone
		}
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

// handleFin accepts the FIN at the next expected seq_nr, parks one that is
// ahead within the reorder window until the gap fills, and re-acks a
// retransmit of the FIN already applied. Any other seq_nr is no FIN the peer
// can have sent, so it is dropped rather than ending the stream.
func (c *Conn) handleFin(p packet) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.establishedClosed {
		return false
	}
	c.updateTimestampDiffLocked(p)
	switch {
	case p.seqNr == c.remoteSeq+1:
		c.remoteSeq = p.seqNr
		c.remoteClosed = true
		c.signalReadLocked()
	case p.seqNr == c.remoteSeq && c.remoteClosed:
		// Our ack of the FIN was lost; the re-ack below replaces it.
	case c.inReorderWindowLocked(p.seqNr):
		if !c.pendingFin || seqLT(p.seqNr, c.pendingFinSeq) {
			c.pendingFin = true
			c.pendingFinSeq = p.seqNr
		}
	default:
		return false
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
// walk is bounded by localSeq so a bogus far-future ack cannot loop. Callers
// pass only acks that passed ackInRangeLocked.
func (c *Conn) processAckLocked(ack uint16) {
	if seqLT(c.lastAck, ack) {
		c.lastAck = ack
	}
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

// statePacketLocked builds a STATE. Like libutp's, it carries the next
// seq_nr we will send without consuming it: a receiver that has everything
// we sent sees it as the next expected seq_nr, while one carrying the last
// consumed seq_nr would be discarded by libutp as an old packet, ack and all.
func (c *Conn) statePacketLocked() packet {
	return c.packetLocked(packetTypeState, c.localSeq, nil)
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
	p := c.statePacketLocked()
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
		p := c.statePacketLocked()
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
	p := c.statePacketLocked()
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
