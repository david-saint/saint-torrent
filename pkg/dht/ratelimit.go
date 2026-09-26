package dht

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"time"
)

const (
	// queryRateInterval is the sustained per-IP query rate we answer: one query
	// every 200ms, i.e. 5/s, matching libtorrent's dht_block_ratelimit.
	queryRateInterval = int64(200 * time.Millisecond)
	// queryRateBurst is how many back-to-back queries an IP may send before the
	// sustained rate applies. Honest nodes send a handful per lookup at most.
	queryRateBurst = 10
	// queryBlockDuration is how long an IP that exceeded its rate is ignored,
	// matching libtorrent's dht_block_timeout. Every query sent while blocked
	// restarts the window, so a flood keeps itself blocked.
	queryBlockDuration = int64(5 * time.Minute)
	// queryLimiterSlots sizes the fixed limiter table (a power of two). It only
	// has to catch heavy senders, not remember every IP.
	queryLimiterSlots = 4096
)

// queryLimiterSlot tracks one source IP with the generic cell rate algorithm:
// tat is the theoretical arrival time of the next conforming query, so a single
// timestamp replaces a token count plus refill time.
type queryLimiterSlot struct {
	tat          int64
	blockedUntil int64
	ip           [4]byte
	used         bool
}

// queryLimiter rate-limits inbound DHT queries per source IPv4 address, so a
// spoofed flood cannot use us to reflect responses at a victim and one IP
// cannot monopolise the read loop. It is a fixed, allocation-free table owned
// by the DHT read goroutine, so it needs no lock. Times are monotonic
// nanoseconds.
type queryLimiter struct {
	key   uint64
	slots [queryLimiterSlots]queryLimiterSlot
}

func newQueryLimiter() *queryLimiter {
	l := &queryLimiter{}
	var k [8]byte
	_, _ = io.ReadFull(rand.Reader, k[:])
	l.key = binary.LittleEndian.Uint64(k[:])
	return l
}

// slot maps ip to its table slot with a keyed mix, so a remote sender cannot
// pick addresses that collide with a victim's slot.
func (l *queryLimiter) slot(ip [4]byte) *queryLimiterSlot {
	x := uint64(binary.BigEndian.Uint32(ip[:])) ^ l.key
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return &l.slots[x&(queryLimiterSlots-1)]
}

// allow reports whether a query from ip arriving at now may be answered.
func (l *queryLimiter) allow(ip [4]byte, now int64) bool {
	s := l.slot(ip)
	if !s.used || s.ip != ip {
		// Another address holds this slot. An active block is kept, so a
		// colliding address can never lift it; the newcomer goes untracked.
		if s.used && now < s.blockedUntil {
			return true
		}
		*s = queryLimiterSlot{ip: ip, used: true, tat: now}
	}
	if now < s.blockedUntil {
		s.blockedUntil = now + queryBlockDuration
		return false
	}
	tat := s.tat
	if tat < now {
		tat = now
	}
	if tat-now > (queryRateBurst-1)*queryRateInterval {
		s.blockedUntil = now + queryBlockDuration
		return false
	}
	s.tat = tat + queryRateInterval
	return true
}
