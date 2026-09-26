// Package dht implements a BEP 5 Kademlia DHT client.
package dht

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/netpolicy"
)

// Node represents a contact in the Kademlia routing table.
type Node struct {
	ID       [20]byte
	Addr     *net.UDPAddr
	LastSeen time.Time
}

// DiscoveredPeer is a peer IP/port combination found for a torrent's info-hash.
type DiscoveredPeer struct {
	InfoHash [20]byte
	IP       net.IP
	Port     uint16
}

type bucket struct {
	nodes          []Node
	pingInProgress bool
}

// PacketConn is the UDP subset DHT needs. It is satisfied by *net.UDPConn and
// by the uTP/DHT shared packet connection.
type PacketConn interface {
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	WriteToUDP([]byte, *net.UDPAddr) (int, error)
	LocalAddr() net.Addr
	Close() error
}

// DHT implements a BEP 5 Kademlia DHT client.
type DHT struct {
	nodeID       [20]byte
	conn         PacketConn
	mu           sync.RWMutex
	buckets      [160]*bucket
	peersMu      sync.Mutex // guards peers; separate from mu so announce_peer storage never blocks getCloserNodes/generateToken/addNode
	peers        *peerStore
	tokenSecrets [2][20]byte
	tokenCreated time.Time
	peerChan     chan DiscoveredPeer
	transactions map[string]transaction
	txMu         sync.Mutex
	txCounter    uint32

	// nodeAddrs and nodeIPs index the buckets under mu: the ID stored at each
	// endpoint, and how many contacts each IP holds. They make presence and
	// one-per-IP checks O(1) instead of a scan of every bucket.
	nodeAddrs map[nodeAddrKey][20]byte
	nodeIPs   map[[4]byte]int

	inFlightProbes    map[nodeAddrKey]struct{} // in-flight probes, keyed by probeKey
	querySenderProbes int                      // in-flight probes started by inbound queries

	// limiter is owned by the read goroutine; respBudget locks itself; started
	// anchors the monotonic clock both run on.
	limiter    *queryLimiter
	respBudget responseBudget
	started    time.Time

	addrMu sync.Mutex
	// addrChanges holds only the node IDs with a verification in flight, so a
	// completed attempt sitting in cooldown never consumes a pending slot.
	addrChanges map[[20]byte]struct{}
	// addrCooldowns holds post-attempt cooldowns: one short entry per node ID
	// and one long entry per (node ID, candidate address) pair.
	addrCooldowns map[addrChangeKey]time.Time

	// sched queues and paces get_peers lookups; see lookup_scheduler.go.
	sched lookupScheduler

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	goMu        sync.Mutex
	closed      bool
	closeOnce   sync.Once
	downloadDir string
}

type transaction struct {
	ch   chan interface{}
	addr *net.UDPAddr
}

const (
	// bucketSize is K, the contacts kept per routing-table bucket (BEP 5).
	bucketSize = 8
	// nodeQuestionableAfter is how long a contact may go unheard before BEP 5
	// calls it questionable. Only questionable contacts are pinged to make room
	// for a newcomer; a bucket of fresher contacts discards the newcomer.
	nodeQuestionableAfter = 15 * time.Minute
	// maxNodePingFailures is how many consecutive pings a questionable contact
	// may miss before it is replaced; BEP 5 suggests trying once more.
	maxNodePingFailures = 2
	// maxInFlightProbes bounds concurrent pings to unverified endpoints: PORT
	// messages, unknown query senders and bootstrap referrals.
	maxInFlightProbes = 100
	// maxQuerySenderProbes is the share of those that unknown query senders may
	// hold, so a spoofed query flood from many source addresses cannot starve
	// probes of bootstrap referrals and PORT-advertised nodes.
	maxQuerySenderProbes = 32
	// nodeProbeTimeout bounds each of those probes.
	nodeProbeTimeout = 5 * time.Second
	// nodePingTimeout bounds every routing-table liveness ping.
	nodePingTimeout = 2 * time.Second
	// maxPendingAddrChanges caps how many node IDs may have an address change
	// verification in flight at once, so a flood of spoofed sightings cannot
	// grow memory or the number of probes we emit.
	maxPendingAddrChanges = 64
	// maxAddrChangeCooldowns bounds the cooldown map. It is deliberately much
	// larger than the in-flight cap: a full cooldown map is never a reason to
	// refuse a verification, only a reason to evict an older cooldown.
	maxAddrChangeCooldowns = 512
	// addrChangeCooldown is how long one (node ID, candidate address) pair is
	// ignored after a verification attempt that did not adopt it, so the same
	// spoofed candidate cannot be retried.
	addrChangeCooldown = 5 * time.Minute
	// addrChangeIDCooldown is the short per-ID window that bounds how often any
	// burst of sightings can make us probe a node's stored address. A genuine
	// move claimed from a new address waits at most this long.
	addrChangeIDCooldown = 45 * time.Second
)

// addrChangeKey identifies a cooldown entry. An empty addr is the short per-ID
// tier; a non-empty addr is the long per-candidate tier.
type addrChangeKey struct {
	id   [20]byte
	addr string
}

// addrChangeOutcome reports how an address-change verification ended.
type addrChangeOutcome int

const (
	// addrChangeFailed means the candidate never proved itself.
	addrChangeFailed addrChangeOutcome = iota
	// addrChangeIncumbentAnswered means the stored address replied, so the
	// candidate was discarded without ever being probed.
	addrChangeIncumbentAnswered
	// addrChangeAdopted means the candidate proved itself and now holds the entry.
	addrChangeAdopted
)

// NewDHT creates and starts a DHT client.
func NewDHT(downloadDir string, listenPort int) (*DHT, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("0.0.0.0:%d", listenPort))
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return NewDHTWithConn(downloadDir, conn)
}

// NewDHTWithConn creates and starts a DHT client using an already-bound UDP
// packet connection. DHT.Close calls conn.Close; ownership of any parent shared
// socket still belongs to the caller that created that socket.
func NewDHTWithConn(downloadDir string, conn PacketConn) (*DHT, error) {
	if conn == nil {
		return nil, errors.New("nil DHT packet connection")
	}

	ctx, cancel := context.WithCancel(context.Background())

	d := &DHT{
		conn:           conn,
		nodeAddrs:      make(map[nodeAddrKey][20]byte),
		nodeIPs:        make(map[[4]byte]int),
		peers:          newPeerStore(),
		peerChan:       make(chan DiscoveredPeer, 256),
		transactions:   make(map[string]transaction),
		inFlightProbes: make(map[nodeAddrKey]struct{}),
		addrChanges:    make(map[[20]byte]struct{}),
		addrCooldowns:  make(map[addrChangeKey]time.Time),
		limiter:        newQueryLimiter(),
		started:        time.Now(),
		ctx:            ctx,
		cancel:         cancel,
		downloadDir:    downloadDir,
	}
	_, _ = io.ReadFull(rand.Reader, d.tokenSecrets[0][:])
	_, _ = io.ReadFull(rand.Reader, d.tokenSecrets[1][:])
	d.tokenCreated = time.Now()

	// A fresh node ID every run: the ID rides in every KRPC message, so a
	// persisted one would link this client's sessions across networks (for
	// example with a VPN on and off) for as long as it was kept. Saved
	// contacts are still reused below to bootstrap.
	d.nodeID = d.generateNodeID()
	d.loadNodes()

	d.goTracked(func() {
		d.readLoop()
	})

	// Bootstrap (DNS resolution + queries) off the constructor path so NewDHT returns
	// immediately even when the network or DNS resolver is slow.
	d.goTracked(d.bootstrap)

	// Periodic bootstrapping to maintain DHT connectivity if count is low
	d.goTracked(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				if d.NodesCount() < 8 {
					d.bootstrap()
				}
			}
		}
	})

	if logging.Enabled() {
		logging.Info("dht_client_started",
			logging.Uint16("port", d.Port()),
		)
	}
	return d, nil
}

// Port returns the local UDP port used by the DHT listener.
func (d *DHT) Port() uint16 {
	if d == nil || d.conn == nil {
		return 0
	}
	addr, ok := d.conn.LocalAddr().(*net.UDPAddr)
	if !ok || addr.Port <= 0 || addr.Port > 65535 {
		return 0
	}
	return uint16(addr.Port)
}

func (d *DHT) goTracked(fn func()) {
	d.goMu.Lock()
	defer d.goMu.Unlock()
	if d.closed {
		return
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		fn()
	}()
}

// PeerChan returns the channel where discovered peers are published.
func (d *DHT) PeerChan() <-chan DiscoveredPeer {
	return d.peerChan
}

// NodesCount returns the total number of nodes in the routing table.
func (d *DHT) NodesCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	count := 0
	for _, b := range d.buckets {
		if b != nil {
			count += len(b.nodes)
		}
	}
	return count
}

// Close stops the DHT listener, discards queued lookups and saves the routing
// table.
func (d *DHT) Close() {
	d.closeOnce.Do(func() {
		// Close the scheduler before cancelling, so a lookup that ends on the
		// cancellation cannot free a slot for a queued one.
		d.sched.close()
		d.cancel()
		_ = d.conn.Close()
		d.goMu.Lock()
		d.closed = true
		d.goMu.Unlock()
		d.wg.Wait()
		close(d.peerChan)
		d.saveNodes()
	})
}

func (d *DHT) readLoop() {
	buf := make([]byte, 4096)
	for {
		n, addr, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-d.ctx.Done():
				return
			default:
				if errors.Is(err, net.ErrClosed) {
					return
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
		d.handlePacket(buf[:n], addr)
	}
}

func (d *DHT) handlePacket(data []byte, addr *net.UDPAddr) {
	parsed, err := bencode.Unmarshal(data)
	if err != nil {
		return
	}

	dict, ok := parsed.(map[string]interface{})
	if !ok {
		return
	}

	tStr, _ := dict["t"].(string)
	yStr, _ := dict["y"].(string)

	switch yStr {
	case "q":
		// Responses to our own transactions skip this gate, so lookups and
		// liveness checks are never throttled.
		if !d.admitQuery(addr) {
			return
		}
		qStr, _ := dict["q"].(string)
		aDict, _ := dict["a"].(map[string]interface{})
		d.handleQuery(tStr, qStr, aDict, addr)
	case "r":
		rDict, _ := dict["r"].(map[string]interface{})
		d.handleResponse(tStr, rDict, addr)
	}
}

// admitQuery reports whether a query from addr should be handled. Sources that
// can never be a peer are dropped, and every other IPv4 source is held to a
// per-IP rate so we cannot be used to reflect responses at a spoofed victim.
// Loopback is exempt: it can only originate on this host, and local test
// networks run many nodes on 127.0.0.1. The DHT is IPv4-only, so IPv6 sources
// (possible on a dual-stack socket) are dropped rather than left unlimited.
func (d *DHT) admitQuery(addr *net.UDPAddr) bool {
	if addr == nil || addr.Port <= 0 || addr.Port > 65535 {
		return false
	}
	ip4 := addr.IP.To4()
	if ip4 == nil {
		return false
	}
	var ip [4]byte
	copy(ip[:], ip4)
	switch netpolicy.Classify(netip.AddrFrom4(ip)) {
	case netpolicy.ScopeInvalid:
		return false
	case netpolicy.ScopeLoopback:
		return true
	}
	return d.limiter.allow(ip, int64(time.Since(d.started)))
}

func (d *DHT) handleQuery(t string, q string, a map[string]interface{}, addr *net.UDPAddr) {
	if a == nil {
		return
	}
	idStr, _ := a["id"].(string)
	if len(idStr) != 20 {
		return
	}
	var senderID [20]byte
	copy(senderID[:], idStr)

	switch q {
	case "ping":
		d.noteQuerySender(senderID, addr)
		d.sendResponse(t, map[string]interface{}{
			"id": string(d.nodeID[:]),
		}, addr)

	case "find_node":
		targetStr, _ := a["target"].(string)
		if len(targetStr) != 20 {
			return
		}
		var targetID [20]byte
		copy(targetID[:], targetStr)

		d.noteQuerySender(senderID, addr)

		closerNodes := d.closestHeardNodes(targetID, 8, addr)
		d.sendResponse(t, map[string]interface{}{
			"id":    string(d.nodeID[:]),
			"nodes": compactNodes(closerNodes),
		}, addr)

	case "get_peers":
		infoHashStr, _ := a["info_hash"].(string)
		if len(infoHashStr) != 20 {
			return
		}
		var infoHash [20]byte
		copy(infoHash[:], infoHashStr)

		d.noteQuerySender(senderID, addr)

		token := d.generateToken(addr)
		peers := d.getPeersForInfoHash(infoHash, addr)
		if len(peers) > 0 {
			d.sendResponse(t, map[string]interface{}{
				"id":     string(d.nodeID[:]),
				"token":  token,
				"values": peers,
			}, addr)
		} else {
			closerNodes := d.closestHeardNodes(infoHash, 8, addr)
			d.sendResponse(t, map[string]interface{}{
				"id":    string(d.nodeID[:]),
				"token": token,
				"nodes": compactNodes(closerNodes),
			}, addr)
		}

	case "announce_peer":
		infoHashStr, _ := a["info_hash"].(string)
		if len(infoHashStr) != 20 {
			return
		}
		var infoHash [20]byte
		copy(infoHash[:], infoHashStr)

		tokenStr, _ := a["token"].(string)
		if !d.validateToken(addr, tokenStr) {
			return
		}

		impliedPortVal, ok := a["implied_port"].(int64)
		actualPort := uint16(0)
		if ok && impliedPortVal != 0 {
			if addr.Port <= 0 || addr.Port > 65535 {
				return
			}
			actualPort = uint16(addr.Port)
		} else {
			portVal, ok := a["port"].(int64)
			if !ok || portVal <= 0 || portVal > 65535 {
				return
			}
			actualPort = uint16(portVal)
		}
		if actualPort == 0 {
			return
		}

		d.noteQuerySender(senderID, addr)
		d.registerPeer(infoHash, addr.IP, actualPort)

		d.sendResponse(t, map[string]interface{}{
			"id": string(d.nodeID[:]),
		}, addr)
	}
}

func (d *DHT) sendResponse(t string, r map[string]interface{}, addr *net.UDPAddr) {
	msg := map[string]interface{}{
		"t": t,
		"y": "r",
		"r": r,
	}
	payload, err := bencode.Marshal(msg)
	if err != nil || !d.respBudget.take(len(payload), int64(time.Since(d.started))) {
		return
	}
	_, _ = d.conn.WriteToUDP(payload, addr)
}

// nextTransactionID returns an unpredictable transaction ID for an outgoing
// query. A sequential counter would let an off-path attacker who can spoof the
// queried node's UDP source address forge responses (injecting bogus peers/nodes)
// by guessing the next ID, so we draw it from crypto/rand instead. 32 random bits
// also make accidental collisions among concurrently outstanding transactions
// negligible. The counter is retained only as a fallback for the astronomically
// unlikely RNG read failure.
func (d *DHT) nextTransactionID() string {
	var buf [4]byte
	if _, err := io.ReadFull(rand.Reader, buf[:]); err != nil {
		d.txMu.Lock()
		d.txCounter++
		v := d.txCounter
		d.txMu.Unlock()
		return fmt.Sprintf("c%07x", v)
	}
	return fmt.Sprintf("%x", buf)
}

func (d *DHT) registerTransaction(t string, addr *net.UDPAddr, ch chan interface{}) {
	d.txMu.Lock()
	defer d.txMu.Unlock()
	d.transactions[t] = transaction{ch: ch, addr: addr}
}

func (d *DHT) unregisterTransaction(t string) {
	d.txMu.Lock()
	defer d.txMu.Unlock()
	delete(d.transactions, t)
}

func (d *DHT) handleResponse(t string, r map[string]interface{}, addr *net.UDPAddr) {
	d.txMu.Lock()
	tx, ok := d.transactions[t]
	d.txMu.Unlock()
	if ok && sameUDPAddr(tx.addr, addr) {
		select {
		case tx.ch <- r:
		default:
		}
	}
}

func (d *DHT) generateToken(addr *net.UDPAddr) string {
	d.mu.Lock()
	if time.Since(d.tokenCreated) > 10*time.Minute {
		d.tokenSecrets[1] = d.tokenSecrets[0]
		_, _ = io.ReadFull(rand.Reader, d.tokenSecrets[0][:])
		d.tokenCreated = time.Now()
	}
	secret := d.tokenSecrets[0]
	d.mu.Unlock()
	return d.tokenForSecret(addr, secret)
}

func (d *DHT) validateToken(addr *net.UDPAddr, token string) bool {
	d.mu.Lock()
	if time.Since(d.tokenCreated) > 10*time.Minute {
		d.tokenSecrets[1] = d.tokenSecrets[0]
		_, _ = io.ReadFull(rand.Reader, d.tokenSecrets[0][:])
		d.tokenCreated = time.Now()
	}
	current := d.tokenSecrets[0]
	previous := d.tokenSecrets[1]
	d.mu.Unlock()

	return token != "" && (token == d.tokenForSecret(addr, current) || token == d.tokenForSecret(addr, previous))
}

func (d *DHT) tokenForSecret(addr *net.UDPAddr, secret [20]byte) string {
	h := sha1.New()
	_, _ = h.Write([]byte(addr.IP.String()))
	_, _ = h.Write(secret[:])
	return string(h.Sum(nil)[:8])
}

func sameUDPAddr(a, b *net.UDPAddr) bool {
	if a == nil || b == nil {
		return false
	}
	return a.Port == b.Port && a.IP.Equal(b.IP)
}

// getPeersForInfoHash returns the peers announced to us for infoHash that
// asker may be told about; a nil asker gets them all.
func (d *DHT) getPeersForInfoHash(infoHash [20]byte, asker *net.UDPAddr) []interface{} {
	d.peersMu.Lock()
	defer d.peersMu.Unlock()
	return d.peers.get(infoHash, time.Now(), asker)
}

func (d *DHT) registerPeer(infoHash [20]byte, ip net.IP, port uint16) {
	ip4 := ip.To4()
	if ip4 == nil {
		return
	}
	var key [4]byte
	copy(key[:], ip4)

	d.peersMu.Lock()
	defer d.peersMu.Unlock()
	d.peers.announce(infoHash, key, port, time.Now())
}

// getCloserNodes returns up to count nodes from the routing table closest to
// target, ordered nearest-first, for seeding our own lookups.
func (d *DHT) getCloserNodes(target [20]byte, count int) []Node {
	return d.closestNodes(target, count, false, nil)
}

// closestHeardNodes is getCloserNodes for the find_node and get_peers answers
// we give asker. It leaves out contacts loaded from disk that have not been
// heard from this run: the file may predate one-contact-per-IP admission or
// have been planted, so a saved contact is only handed to the rest of the DHT
// once it has answered us again. Our own lookups may still start from it. It
// also leaves out contacts asker may not be told about under netpolicy's
// scope rule, so a public asker never learns our loopback, LAN or link-local
// contacts (it could not reach them, and they map our network for it).
func (d *DHT) closestHeardNodes(target [20]byte, count int, asker *net.UDPAddr) []Node {
	return d.closestNodes(target, count, true, asker)
}

// closestNodes returns up to count contacts closest to target, nearest first,
// skipping never-heard (zero LastSeen) contacts when heardOnly is set, and,
// when asker is set, contacts asker may not be told about; the scan goes on
// past them, so up to count eligible contacts are still returned. Rather than
// copying every node out of the table and sorting the copy (which recomputes
// each XOR distance on every comparison), it keeps a small sorted candidate
// slice of size <= count and inserts each node into it in place, computing its
// distance exactly once.
func (d *DHT) closestNodes(target [20]byte, count int, heardOnly bool, asker *net.UDPAddr) []Node {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if count <= 0 {
		return nil
	}

	type candidate struct {
		node Node
		dist [20]byte
	}

	best := make([]candidate, 0, count)

	for _, b := range d.buckets {
		if b == nil {
			continue
		}
		for _, n := range b.nodes {
			if heardOnly && n.LastSeen.IsZero() {
				continue
			}
			dist := xorDistance(n.ID, target)
			if len(best) == count && !lessXor(dist, best[count-1].dist) {
				continue
			}
			// Only a contact that would make the cut pays for the scope check.
			if asker != nil {
				if k, ok := nodeAddrKeyOf(n.Addr); !ok || !endpointAllowed(k, asker) {
					continue
				}
			}

			if len(best) < count {
				best = append(best, candidate{node: n, dist: dist})
				for i := len(best) - 1; i > 0 && lessXor(best[i].dist, best[i-1].dist); i-- {
					best[i], best[i-1] = best[i-1], best[i]
				}
				continue
			}

			best[count-1] = candidate{node: n, dist: dist}
			for i := count - 1; i > 0 && lessXor(best[i].dist, best[i-1].dist); i-- {
				best[i], best[i-1] = best[i-1], best[i]
			}
		}
	}

	result := make([]Node, len(best))
	for i, c := range best {
		result[i] = c.node
	}
	return result
}

// lessXor reports whether XOR distance a is closer (smaller) than b.
func lessXor(a, b [20]byte) bool {
	for i := 0; i < 20; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// nodeAddrKey identifies a routing-table endpoint.
type nodeAddrKey struct {
	ip   [4]byte
	port uint16
}

// endpointKey normalises an IPv4 endpoint, reporting false for anything the
// IPv4-only routing table cannot hold.
func endpointKey(ip net.IP, port int) (nodeAddrKey, bool) {
	ip4 := ip.To4()
	if ip4 == nil || port <= 0 || port > 65535 {
		return nodeAddrKey{}, false
	}
	k := nodeAddrKey{port: uint16(port)}
	copy(k.ip[:], ip4)
	return k, true
}

func nodeAddrKeyOf(addr *net.UDPAddr) (nodeAddrKey, bool) {
	if addr == nil {
		return nodeAddrKey{}, false
	}
	return endpointKey(addr.IP, addr.Port)
}

// udpAddr returns a fresh address for k, so a stored contact never aliases a
// caller's buffer.
func (k nodeAddrKey) udpAddr() *net.UDPAddr {
	return &net.UDPAddr{IP: net.IP(append([]byte(nil), k.ip[:]...)), Port: int(k.port)}
}

// onePerIP reports whether ip is held to a single routing-table contact, like
// libtorrent's dht_restrict_routing_ips, so one host cannot fill buckets by
// claiming many IDs on many ports. Loopback is exempt: it can only be learned
// from this host, and local test networks run many nodes on 127.0.0.1.
func onePerIP(ip [4]byte) bool {
	return ip[0] != 127
}

// diverse reports whether ip is held to one contact per /24 in each bucket
// and one candidate per /24 in each lookup, like libtorrent's
// dht_restrict_routing_ips and dht_restrict_search_ips, so a host controlling
// a whole /24 still cannot fill the buckets or steer the lookups near a chosen
// ID. Loopback, link-local and private (LAN) addresses are exempt, so local and
// test DHTs, which often share one subnet, keep working.
func diverse(ip [4]byte) bool {
	return !netpolicy.IsLocal(netip.AddrFrom4(ip))
}

// sameSlash24 reports whether a and b are in the same IPv4 /24.
func sameSlash24(a, b [4]byte) bool {
	return a[0] == b[0] && a[1] == b[1] && a[2] == b[2]
}

// slash24Taken reports whether b bars a contact at k for subnet diversity: k
// is a diverse address and b already holds a contact on its /24, other than
// one stored at k itself or under skip (the contact k would replace). The
// bucket holds at most bucketSize contacts, so the scan is short. Callers
// hold d.mu.
func slash24Taken(b *bucket, k nodeAddrKey, skip *[20]byte) bool {
	if b == nil || !diverse(k.ip) {
		return false
	}
	for i := range b.nodes {
		n := &b.nodes[i]
		if skip != nil && n.ID == *skip {
			continue
		}
		nk, ok := nodeAddrKeyOf(n.Addr)
		if ok && nk != k && sameSlash24(nk.ip, k.ip) {
			return true
		}
	}
	return false
}

// endpointAllowed reports whether we may contact k after source told us about
// it: per netpolicy, a remote node may only point us at addresses no more local
// than itself, and never at one that cannot be a unicast peer.
func endpointAllowed(k nodeAddrKey, source *net.UDPAddr) bool {
	var src netip.Addr
	if source != nil {
		if s4 := source.IP.To4(); s4 != nil {
			src = netip.AddrFrom4([4]byte(s4))
		}
	}
	return netpolicy.PeerAllowed(netip.AddrPortFrom(netip.AddrFrom4(k.ip), k.port), src)
}

// questionable reports whether BEP 5 would ping n before letting it keep its
// slot: it has not been heard from for nodeQuestionableAfter. Contacts loaded
// from disk carry a zero LastSeen, so they start questionable.
func questionable(n Node, now time.Time) bool {
	return now.Sub(n.LastSeen) >= nodeQuestionableAfter
}

func (b *bucket) indexOf(id [20]byte) int {
	for i := range b.nodes {
		if b.nodes[i].ID == id {
			return i
		}
	}
	return -1
}

// oldest returns b's least recently seen contact, the one BEP 5 checks first
// when a newcomer wants a slot. Buckets are kept roughly in that order, but a
// scan of K entries makes it exact.
func (b *bucket) oldest() Node {
	o := 0
	for i := 1; i < len(b.nodes); i++ {
		if b.nodes[i].LastSeen.Before(b.nodes[o].LastSeen) {
			o = i
		}
	}
	return b.nodes[o]
}

// appendNodeLocked adds n at the tail of b and indexes it. Callers hold d.mu
// and have already checked admission.
func (d *DHT) appendNodeLocked(b *bucket, n Node) {
	k, ok := nodeAddrKeyOf(n.Addr)
	if !ok {
		return
	}
	b.nodes = append(b.nodes, n)
	d.nodeAddrs[k] = n.ID
	d.nodeIPs[k.ip]++
}

// removeNodeAtLocked removes b.nodes[i] and its index entries. Callers hold d.mu.
func (d *DHT) removeNodeAtLocked(b *bucket, i int) {
	n := b.nodes[i]
	b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
	d.unindexNodeLocked(n)
}

func (d *DHT) unindexNodeLocked(n Node) {
	k, ok := nodeAddrKeyOf(n.Addr)
	if !ok {
		return
	}
	if d.nodeAddrs[k] == n.ID {
		delete(d.nodeAddrs, k)
	}
	if d.nodeIPs[k.ip] <= 1 {
		delete(d.nodeIPs, k.ip)
	} else {
		d.nodeIPs[k.ip]--
	}
}

// removeNodeByIDLocked removes the contact stored under id at k, if any.
func (d *DHT) removeNodeByIDLocked(id [20]byte, k nodeAddrKey) {
	b := d.buckets[bucketIndex(d.nodeID, id)]
	if b == nil {
		return
	}
	if i := b.indexOf(id); i >= 0 {
		if ik, ok := nodeAddrKeyOf(b.nodes[i].Addr); ok && ik == k {
			d.removeNodeAtLocked(b, i)
		}
	}
}

// touchLocked records a sighting of b.nodes[i] and moves it to the tail, the
// most-recently-seen end of the bucket.
func touchLocked(b *bucket, i int, seen time.Time) {
	n := b.nodes[i]
	if seen.After(n.LastSeen) {
		n.LastSeen = seen
	}
	b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
	b.nodes = append(b.nodes, n)
}

// addNode admits or refreshes a contact that answered one of our own queries
// from addr with id. That answer, matched to a random transaction ID and to
// the address we sent to, is the only evidence strong enough to admit a new
// contact; unsolicited query senders go through noteQuerySender instead.
func (d *DHT) addNode(id [20]byte, addr *net.UDPAddr) {
	d.addNodeSeen(id, addr, time.Now())
}

func (d *DHT) addNodeSeen(id [20]byte, addr *net.UDPAddr, seen time.Time) {
	if id == d.nodeID {
		return
	}
	k, ok := nodeAddrKeyOf(addr)
	if !ok {
		return
	}

	idx := bucketIndex(d.nodeID, id)
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.buckets[idx] == nil {
		d.buckets[idx] = &bucket{}
	}
	b := d.buckets[idx]

	if i := b.indexOf(id); i >= 0 {
		if sameUDPAddr(b.nodes[i].Addr, addr) {
			touchLocked(b, i, seen)
			return
		}
		// A claimed address change is never trusted on sight; it is only a candidate.
		d.considerAddressChange(id, b.nodes[i].Addr, addr)
		return
	}

	// This endpoint answered as id, so another ID stored there is stale.
	if old, held := d.nodeAddrs[k]; held {
		d.removeNodeByIDLocked(old, k)
	}
	if onePerIP(k.ip) && d.nodeIPs[k.ip] > 0 {
		return
	}

	if len(b.nodes) < bucketSize {
		if slash24Taken(b, k, nil) {
			return
		}
		d.appendNodeLocked(b, Node{ID: id, Addr: k.udpAddr(), LastSeen: seen})
		return
	}
	// BEP 5: a bucket full of good contacts simply discards the newcomer; only
	// a questionable contact is challenged, one challenge per bucket at a time.
	stale := b.oldest()
	if b.pingInProgress || !questionable(stale, time.Now()) {
		return
	}
	// The challenged contact may be what holds the newcomer's /24; if it is
	// replaced, that /24 is free again.
	if slash24Taken(b, k, &stale.ID) {
		return
	}
	n := Node{ID: id, Addr: k.udpAddr(), LastSeen: seen}
	b.pingInProgress = true
	d.goTracked(func() {
		d.challenge(idx, stale, n)
	})
}

// challenge pings a full bucket's questionable contact on behalf of a
// newcomer. The contact keeps its slot if it answers with its own ID. It is
// replaced after maxNodePingFailures consecutive misses, or at once if its
// address answers as a different node. d.mu is never held across a ping.
func (d *DHT) challenge(idx int, stale, newcomer Node) {
	defer func() {
		d.mu.Lock()
		if b := d.buckets[idx]; b != nil {
			b.pingInProgress = false
		}
		d.mu.Unlock()
	}()

	failures := 0
	for failures < maxNodePingFailures {
		ctx, cancel := context.WithTimeout(d.ctx, nodePingTimeout)
		gotID, err := d.queryNodeID(ctx, stale.Addr)
		cancel()
		if d.ctx.Err() != nil {
			return
		}
		switch {
		case err == nil && gotID == stale.ID:
			d.refreshNode(stale.ID, stale.Addr)
			return
		case err == nil:
			// Something else owns that address now: the contact is proven wrong.
			failures = maxNodePingFailures
		default:
			failures++
		}
	}
	d.replaceNode(idx, stale, newcomer)
}

// replaceNode evicts old from bucket idx, if it is still stored there and still
// questionable, and admits newcomer if the table still has room for it.
func (d *DHT) replaceNode(idx int, old, newcomer Node) {
	k, ok := nodeAddrKeyOf(newcomer.Addr)
	if !ok {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	b := d.buckets[idx]
	if b == nil {
		return
	}
	if i := b.indexOf(old.ID); i >= 0 && sameUDPAddr(b.nodes[i].Addr, old.Addr) {
		if !questionable(b.nodes[i], time.Now()) {
			// Heard from while we pinged, so it keeps its slot.
			return
		}
		d.removeNodeAtLocked(b, i)
	}
	// The table may have changed during the pings, so re-check admission.
	if len(b.nodes) >= bucketSize || b.indexOf(newcomer.ID) >= 0 {
		return
	}
	if _, held := d.nodeAddrs[k]; held {
		return
	}
	if onePerIP(k.ip) && d.nodeIPs[k.ip] > 0 {
		return
	}
	if slash24Taken(b, k, nil) {
		return
	}
	d.appendNodeLocked(b, newcomer)
}

// noteQuerySender handles the sender of a well-formed inbound query. Its
// source address is unverified, since UDP is trivially spoofed, so a query
// never refreshes a contact: LastSeen only moves on an answer to one of our
// own queries, matched to a random transaction ID and the address we sent to.
// Otherwise a spoofer replaying queries in a dead contact's name could keep it
// looking live and pin it in its bucket. A saved contact not yet heard from,
// and an unknown sender, are pinged through the bounded probe path, and
// admitted or refreshed by what answers there. The table is only read here.
func (d *DHT) noteQuerySender(id [20]byte, addr *net.UDPAddr) {
	if id == d.nodeID {
		return
	}
	k, ok := nodeAddrKeyOf(addr)
	if !ok {
		return
	}
	idx := bucketIndex(d.nodeID, id)

	d.mu.RLock()
	probe := false
	if b := d.buckets[idx]; b != nil {
		if i := b.indexOf(id); i >= 0 {
			switch {
			case !sameUDPAddr(b.nodes[i].Addr, addr):
				// A claimed address change is never trusted on sight; it is only a candidate.
				d.considerAddressChange(id, b.nodes[i].Addr, addr)
			case b.nodes[i].LastSeen.IsZero():
				// A spoofed query naming a saved contact must not make it
				// look live, or get it handed out, without it answering us.
				probe = true
			}
			d.mu.RUnlock()
			if probe {
				d.probeNode(k, true)
			}
			return
		}
	}
	probe = d.mightAdmitLocked(idx, k)
	d.mu.RUnlock()

	if probe {
		d.probeNode(k, true)
	}
}

// mightAdmitLocked reports whether a verified contact at k with an ID in bucket
// idx could be admitted, so a probe is only spent when it could matter. An
// endpoint held by another ID is still probed: its answer settles which ID
// lives there now. It mirrors addNodeSeen's admission rules. Callers hold d.mu,
// read or write.
func (d *DHT) mightAdmitLocked(idx int, k nodeAddrKey) bool {
	if _, held := d.nodeAddrs[k]; !held && onePerIP(k.ip) && d.nodeIPs[k.ip] > 0 {
		return false
	}
	b := d.buckets[idx]
	if b == nil {
		return true
	}
	if len(b.nodes) < bucketSize {
		return !slash24Taken(b, k, nil)
	}
	if b.pingInProgress {
		return false
	}
	stale := b.oldest()
	return questionable(stale, time.Now()) && !slash24Taken(b, k, &stale.ID)
}

// considerAddressChange queues a candidate address for a node ID already in the
// routing table. Called with d.mu held (a read lock suffices); it only reserves
// a slot and hands the network work to a tracked goroutine.
func (d *DHT) considerAddressChange(id [20]byte, oldAddr, newAddr *net.UDPAddr) {
	if oldAddr == nil || newAddr == nil {
		return
	}
	// Admission is decided straight from the stored addresses; only the
	// goroutine that outlives d.mu needs copies, so a rejected sighting costs
	// no allocation on the receive path.
	if !d.beginAddressVerification(id, newAddr) {
		return
	}
	from := cloneUDPAddr(oldAddr)
	to := cloneUDPAddr(newAddr)
	d.goTracked(func() {
		outcome := d.verifyAddressChange(id, from, to)
		d.endAddressVerification(id, to, outcome)
	})
}

// beginAddressVerification reserves the single verification slot for id,
// reporting false when this exact candidate is still in its long cooldown, the
// ID is still in its short cooldown, a verification is already in flight for it,
// or the in-flight set is full.
func (d *DHT) beginAddressVerification(id [20]byte, candidate *net.UDPAddr) bool {
	now := time.Now()
	candKey := addrChangeKey{id: id, addr: udpAddrKey(candidate)}
	idKey := addrChangeKey{id: id}

	d.addrMu.Lock()
	defer d.addrMu.Unlock()

	if d.addrChanges == nil {
		d.addrChanges = make(map[[20]byte]struct{})
	}
	if d.addrCooldowns == nil {
		d.addrCooldowns = make(map[addrChangeKey]time.Time)
	}

	// This exact claim was already checked and rejected recently.
	if until, ok := d.addrCooldowns[candKey]; ok && now.Before(until) {
		return false
	}
	// The short per-ID window bounds how often any burst of sightings, spoofed
	// or not, can make us probe this node's stored address.
	if until, ok := d.addrCooldowns[idKey]; ok && now.Before(until) {
		return false
	}
	if _, ok := d.addrChanges[id]; ok {
		return false
	}
	if len(d.addrChanges) >= maxPendingAddrChanges {
		return false
	}
	d.addrChanges[id] = struct{}{}
	return true
}

// endAddressVerification releases the in-flight slot for id. A verified
// adoption clears every cooldown held for that ID, so a node that legitimately
// moves again straight away - even back to an address that once failed - is not
// ignored; every other outcome arms both cooldowns.
func (d *DHT) endAddressVerification(id [20]byte, candidate *net.UDPAddr, outcome addrChangeOutcome) {
	candKey := addrChangeKey{id: id, addr: udpAddrKey(candidate)}
	idKey := addrChangeKey{id: id}

	d.addrMu.Lock()
	defer d.addrMu.Unlock()

	delete(d.addrChanges, id)
	if d.addrCooldowns == nil {
		d.addrCooldowns = make(map[addrChangeKey]time.Time)
	}
	if outcome == addrChangeAdopted {
		for k := range d.addrCooldowns {
			if k.id == id {
				delete(d.addrCooldowns, k)
			}
		}
		return
	}

	wanted := 0
	if _, ok := d.addrCooldowns[idKey]; !ok {
		wanted++
	}
	if _, ok := d.addrCooldowns[candKey]; !ok {
		wanted++
	}
	d.makeAddrCooldownRoomLocked(wanted)

	now := time.Now()
	d.addrCooldowns[idKey] = now.Add(addrChangeIDCooldown)
	d.addrCooldowns[candKey] = now.Add(addrChangeCooldown)
}

// makeAddrCooldownRoomLocked frees room for wanted new entries, dropping
// expired cooldowns first. Both tiers of one attempt are recorded together, so
// room is made for them in one pass; otherwise recording the long entry could
// evict the short one that was just armed beside it. Callers must hold addrMu.
func (d *DHT) makeAddrCooldownRoomLocked(wanted int) {
	if len(d.addrCooldowns)+wanted <= maxAddrChangeCooldowns {
		return
	}

	now := time.Now()
	for k, until := range d.addrCooldowns {
		if !now.Before(until) {
			delete(d.addrCooldowns, k)
		}
	}

	for len(d.addrCooldowns)+wanted > maxAddrChangeCooldowns {
		var victim addrChangeKey
		var victimUntil time.Time
		found := false
		for k, until := range d.addrCooldowns {
			if !found || evictBefore(k, until, victim, victimUntil) {
				victim, victimUntil, found = k, until, true
			}
		}
		if !found {
			return
		}
		delete(d.addrCooldowns, victim)
	}
}

// evictBefore reports whether cooldown a should be dropped ahead of b. The long
// per-candidate tier goes first because the short per-ID tier is what bounds how
// often we probe a stored address; within a tier the entry nearest to expiring
// loses the least protection.
func evictBefore(a addrChangeKey, aUntil time.Time, b addrChangeKey, bUntil time.Time) bool {
	if (a.addr != "") != (b.addr != "") {
		return a.addr != ""
	}
	return aUntil.Before(bUntil)
}

// udpAddrKey renders an address as a map key using the same identity
// sameUDPAddr compares on: IP and port, ignoring the zone.
func udpAddrKey(a *net.UDPAddr) string {
	if a == nil {
		return ""
	}
	return net.JoinHostPort(a.IP.String(), strconv.Itoa(a.Port))
}

// verifyAddressChange adopts newAddr for id only when oldAddr stops answering
// pings and newAddr answers one with that same node ID. It runs off the read
// loop and never holds d.mu across a network wait.
func (d *DHT) verifyAddressChange(id [20]byte, oldAddr, newAddr *net.UDPAddr) addrChangeOutcome {
	select {
	case <-d.ctx.Done():
		return addrChangeFailed
	default:
	}

	ctx, cancel := context.WithTimeout(d.ctx, nodePingTimeout)
	oldID, err := d.queryNodeID(ctx, oldAddr)
	cancel()
	if err == nil {
		// Any answer from the stored address discards the candidate unprobed.
		if oldID == id {
			d.refreshNode(id, oldAddr)
		} else {
			// Something else owns that address now, so the stored contact is
			// proven wrong and must not linger in the bucket.
			d.dropNode(id, oldAddr)
		}
		return addrChangeIncumbentAnswered
	}

	ctx, cancel = context.WithTimeout(d.ctx, nodePingTimeout)
	newID, err := d.queryNodeID(ctx, newAddr)
	cancel()
	if err != nil || newID != id {
		return addrChangeFailed
	}

	if !d.repointNode(id, oldAddr, newAddr) {
		return addrChangeFailed
	}
	return addrChangeAdopted
}

// refreshNode marks a node live and moves it to the tail of its bucket, but only
// if it is still stored at addr.
func (d *DHT) refreshNode(id [20]byte, addr *net.UDPAddr) {
	idx := bucketIndex(d.nodeID, id)

	d.mu.Lock()
	defer d.mu.Unlock()

	b := d.buckets[idx]
	if b == nil {
		return
	}
	for i, n := range b.nodes {
		if n.ID == id && sameUDPAddr(n.Addr, addr) {
			n.LastSeen = time.Now()
			b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
			b.nodes = append(b.nodes, n)
			return
		}
	}
}

// dropNode removes a node that has been proven wrong, but only if it is still
// stored at addr, so a concurrent re-point is never undone.
func (d *DHT) dropNode(id [20]byte, addr *net.UDPAddr) {
	idx := bucketIndex(d.nodeID, id)

	d.mu.Lock()
	defer d.mu.Unlock()

	b := d.buckets[idx]
	if b == nil {
		return
	}
	if i := b.indexOf(id); i >= 0 && sameUDPAddr(b.nodes[i].Addr, addr) {
		d.removeNodeAtLocked(b, i)
	}
}

// repointNode moves a verified node from oldAddr to newAddr, leaving the entry
// alone if it no longer points at oldAddr, or if newAddr is already taken by
// another contact, would give its IP a second one, or would give its bucket a
// second contact on a diverse /24. It reports whether the move applied.
func (d *DHT) repointNode(id [20]byte, oldAddr, newAddr *net.UDPAddr) bool {
	nk, ok := nodeAddrKeyOf(newAddr)
	if !ok {
		return false
	}
	idx := bucketIndex(d.nodeID, id)

	d.mu.Lock()
	defer d.mu.Unlock()

	b := d.buckets[idx]
	if b == nil {
		return false
	}
	i := b.indexOf(id)
	if i < 0 || !sameUDPAddr(b.nodes[i].Addr, oldAddr) {
		return false
	}
	if _, held := d.nodeAddrs[nk]; held {
		return false
	}
	others := d.nodeIPs[nk.ip]
	if cur, valid := nodeAddrKeyOf(b.nodes[i].Addr); valid && cur.ip == nk.ip {
		others-- // the node itself, moving to another port on the same IP
	}
	if onePerIP(nk.ip) && others > 0 {
		return false
	}
	if slash24Taken(b, nk, &id) {
		return false
	}

	n := b.nodes[i]
	d.removeNodeAtLocked(b, i)
	n.Addr = nk.udpAddr()
	n.LastSeen = time.Now()
	d.appendNodeLocked(b, n)
	return true
}

func cloneUDPAddr(a *net.UDPAddr) *net.UDPAddr {
	if a == nil {
		return nil
	}
	return &net.UDPAddr{IP: append(net.IP(nil), a.IP...), Port: a.Port, Zone: a.Zone}
}

// HasNodeAddress returns true if a node with the given IP and UDP port exists in the routing table.
func (d *DHT) HasNodeAddress(ip net.IP, port uint16) bool {
	if d == nil {
		return false
	}
	k, ok := endpointKey(ip, int(port))
	if !ok {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, held := d.nodeAddrs[k]
	return held
}

// AddNode ingests a DHT node advertised by a BitTorrent peer via the BEP 5 PORT
// message. That message carries only the node's IP and UDP port — not its
// Kademlia node ID — so we ping the endpoint to learn its ID and, on a valid
// reply, insert it into the routing table. Live peers are one of the richest
// sources of fresh DHT nodes, so this grows the table beyond bootstrap nodes and
// lookups. The probe runs on a tracked goroutine so the caller (the peer message
// loop) never blocks on the network.
// Note: Since the DHT socket is IPv4-bound, IPv6 addresses are silently ignored.
func (d *DHT) AddNode(ip net.IP, port uint16) {
	if d == nil {
		return
	}
	k, ok := endpointKey(ip, int(port))
	if !ok {
		// Silently ignore IPv6 addresses as the DHT UDP socket is IPv4-bound.
		return
	}
	if netpolicy.Classify(netip.AddrFrom4(k.ip)) == netpolicy.ScopeInvalid {
		return
	}
	d.probeUnknown(k)
}

// probeUnknown probes k unless the routing table already holds that endpoint
// or another contact on its IP. Both checks are O(1) map lookups, so a peer
// repeating PORT messages costs no table scan on its message loop.
func (d *DHT) probeUnknown(k nodeAddrKey) {
	d.mu.RLock()
	_, held := d.nodeAddrs[k]
	ipTaken := onePerIP(k.ip) && d.nodeIPs[k.ip] > 0
	d.mu.RUnlock()
	if held || ipTaken {
		return
	}
	d.probeNode(k, false)
}

// probeKey is the in-flight identity of a probe to k: the IP alone where only
// one contact per IP can be admitted, so a flood rotating ports on one address
// still costs at most one probe at a time.
func probeKey(k nodeAddrKey) nodeAddrKey {
	if onePerIP(k.ip) {
		k.port = 0
	}
	return k
}

// probeNode pings k and admits whatever node ID answers from it. Probes are
// deduplicated per probeKey, capped at maxInFlightProbes (maxQuerySenderProbes
// for those fromQuery), and run on tracked goroutines so callers never block
// on the network.
func (d *DHT) probeNode(k nodeAddrKey, fromQuery bool) {
	pk := probeKey(k)
	d.txMu.Lock()
	if d.inFlightProbes == nil {
		d.inFlightProbes = make(map[nodeAddrKey]struct{})
	}
	// Deduplicate: don't spawn multiple queries to the same address concurrently
	if _, active := d.inFlightProbes[pk]; active {
		d.txMu.Unlock()
		return
	}
	if len(d.inFlightProbes) >= maxInFlightProbes || (fromQuery && d.querySenderProbes >= maxQuerySenderProbes) {
		d.txMu.Unlock()
		return
	}
	d.inFlightProbes[pk] = struct{}{}
	if fromQuery {
		d.querySenderProbes++
	}
	d.txMu.Unlock()

	addr := k.udpAddr()
	d.goTracked(func() {
		defer func() {
			d.txMu.Lock()
			delete(d.inFlightProbes, pk)
			if fromQuery {
				d.querySenderProbes--
			}
			d.txMu.Unlock()
		}()

		select {
		case <-d.ctx.Done():
			return
		default:
		}
		ctx, cancel := context.WithTimeout(d.ctx, nodeProbeTimeout)
		defer cancel()
		id, err := d.queryNodeID(ctx, addr)
		if err != nil {
			return
		}
		d.addNode(id, addr)
	})
}

// queryNodeID pings addr and returns the responder's 20-byte Kademlia node ID.
func (d *DHT) queryNodeID(ctx context.Context, addr *net.UDPAddr) ([20]byte, error) {
	var id [20]byte
	t := d.nextTransactionID()
	query := map[string]interface{}{
		"t": t,
		"y": "q",
		"q": "ping",
		"a": map[string]interface{}{
			"id": string(d.nodeID[:]),
		},
	}

	payload, err := bencode.Marshal(query)
	if err != nil {
		return id, err
	}

	ch := make(chan interface{}, 1)
	d.registerTransaction(t, addr, ch)
	defer d.unregisterTransaction(t)

	_, err = d.conn.WriteToUDP(payload, addr)
	if err != nil {
		return id, err
	}

	select {
	case resp := <-ch:
		rDict, ok := resp.(map[string]interface{})
		if !ok {
			return id, errors.New("invalid response")
		}
		idStr, _ := rDict["id"].(string)
		if len(idStr) != 20 {
			return id, errors.New("invalid responder id")
		}
		copy(id[:], idStr)
		return id, nil
	case <-ctx.Done():
		return id, ctx.Err()
	}
}

func (d *DHT) findNode(ctx context.Context, target [20]byte, addr *net.UDPAddr) ([]Node, error) {
	t := d.nextTransactionID()
	query := map[string]interface{}{
		"t": t,
		"y": "q",
		"q": "find_node",
		"a": map[string]interface{}{
			"id":     string(d.nodeID[:]),
			"target": string(target[:]),
		},
	}

	payload, err := bencode.Marshal(query)
	if err != nil {
		return nil, err
	}

	ch := make(chan interface{}, 1)
	d.registerTransaction(t, addr, ch)
	defer d.unregisterTransaction(t)

	_, err = d.conn.WriteToUDP(payload, addr)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		rDict, ok := resp.(map[string]interface{})
		if !ok {
			return nil, errors.New("invalid response")
		}
		if idStr, _ := rDict["id"].(string); len(idStr) != 20 {
			return nil, errors.New("invalid responder id")
		}
		nodes, ok := nodesField(rDict)
		if !ok {
			return nil, errMalformedNodes
		}
		return nodes, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetPeersResult contains token, closer nodes, or discovered peers.
type GetPeersResult struct {
	// ID is the node ID the responder reported for itself.
	ID    [20]byte
	Token string
	Peers []string
	Nodes []Node
}

func (d *DHT) getPeersQuery(ctx context.Context, infoHash [20]byte, addr *net.UDPAddr) (*GetPeersResult, error) {
	t := d.nextTransactionID()
	query := map[string]interface{}{
		"t": t,
		"y": "q",
		"q": "get_peers",
		"a": map[string]interface{}{
			"id":        string(d.nodeID[:]),
			"info_hash": string(infoHash[:]),
		},
	}

	payload, err := bencode.Marshal(query)
	if err != nil {
		return nil, err
	}

	ch := make(chan interface{}, 1)
	d.registerTransaction(t, addr, ch)
	defer d.unregisterTransaction(t)

	_, err = d.conn.WriteToUDP(payload, addr)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		rDict, ok := resp.(map[string]interface{})
		if !ok {
			return nil, errors.New("invalid response")
		}
		idStr, _ := rDict["id"].(string)
		if len(idStr) != 20 {
			return nil, errors.New("invalid responder id")
		}
		token, _ := rDict["token"].(string)
		if len(token) > dhtMaxTokenLen {
			// We echo the token in announce_peer; an oversized one would make
			// us send the responder a large packet of its choosing. Dropping
			// it means we never announce there (rakshasa/libtorrent@fa9812b).
			token = ""
		}
		res := &GetPeersResult{Token: token}
		copy(res.ID[:], idStr)

		if val, exists := rDict["values"]; exists {
			list, ok := val.([]interface{})
			if ok {
				for _, item := range list {
					if len(res.Peers) == dhtMaxValuesPerResponse {
						break
					}
					s, ok := item.(string)
					if ok && len(s) == 6 {
						res.Peers = append(res.Peers, s)
					}
				}
			}
		}
		// A malformed node list fails the whole answer, so the lookup marks
		// the responder failed and uses neither its token nor its values.
		nodes, ok := nodesField(rDict)
		if !ok {
			return nil, errMalformedNodes
		}
		res.Nodes = nodes
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *DHT) announcePeerQuery(ctx context.Context, infoHash [20]byte, port uint16, token string, addr *net.UDPAddr) error {
	t := d.nextTransactionID()
	query := map[string]interface{}{
		"t": t,
		"y": "q",
		"q": "announce_peer",
		"a": map[string]interface{}{
			"id":        string(d.nodeID[:]),
			"info_hash": string(infoHash[:]),
			"port":      int(port),
			"token":     token,
		},
	}

	payload, err := bencode.Marshal(query)
	if err != nil {
		return err
	}

	ch := make(chan interface{}, 1)
	d.registerTransaction(t, addr, ch)
	defer d.unregisterTransaction(t)

	_, err = d.conn.WriteToUDP(payload, addr)
	if err != nil {
		return err
	}

	select {
	case resp := <-ch:
		_, ok := resp.(map[string]interface{})
		if !ok {
			return errors.New("invalid response")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DefaultBootstrapHosts is the list of public DHT bootstrap routers.
// Modified in unit tests to prevent hitting the real network.
var DefaultBootstrapHosts = []string{
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.transmissionbt.com:6881",
}

func (d *DHT) bootstrap() {
	var resolver net.Resolver
	for _, host := range DefaultBootstrapHosts {
		hostName, portStr, err := net.SplitHostPort(host)
		if err != nil {
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			continue
		}
		// Cancellable, IPv4-only DNS: a hung resolver must not keep this bootstrap goroutine
		// — and therefore DHT.Close, which waits on it — blocked past context cancellation,
		// and the DHT socket is IPv4-bound, so an IPv6 result would be unusable.
		ips, err := resolver.LookupIP(d.ctx, "ip4", hostName)
		if err != nil || len(ips) == 0 {
			continue
		}
		targetAddr := &net.UDPAddr{IP: ips[0], Port: port}
		d.goTracked(func() {
			select {
			case <-d.ctx.Done():
				return
			default:
			}
			ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
			defer cancel()
			nodes, err := d.findNode(ctx, d.nodeID, targetAddr)
			if err == nil {
				// Referrals are only candidates: each is admitted once it
				// answers a ping of our own, under the ID it answers with.
				for _, node := range nodes {
					if k, ok := nodeAddrKeyOf(node.Addr); ok && endpointAllowed(k, targetAddr) {
						d.probeUnknown(k)
					}
				}
			}
		})
	}
}

const (
	dhtLookupStartNodes  = 32
	dhtLookupQueryLimit  = 128
	dhtLookupParallelism = 8
	// dhtLookupK is how many closest responders a lookup converges on and
	// announces to (BEP 5's K).
	dhtLookupK = 8
	// dhtLookupCandidates bounds a lookup's candidate set. Only the closest are
	// kept, so a responder flooding referrals cannot grow it or push the
	// search away from the target.
	dhtLookupCandidates = 64
	// dhtLookupQueryTimeout bounds each get_peers query of a lookup.
	dhtLookupQueryTimeout = 3 * time.Second
	// dhtBootstrapWait bounds how long a lookup that found the table empty
	// waits for bootstrap referrals to answer their probes;
	// dhtBootstrapPoll is how often it checks.
	dhtBootstrapWait = 5 * time.Second
	dhtBootstrapPoll = 50 * time.Millisecond
	// dhtMaxValuesPerResponse caps the peers taken from one get_peers answer.
	// Honest nodes return at most this many (libtorrent's dht_max_peers_reply;
	// we return 50), so one response cannot fill our dial slots with hundreds.
	dhtMaxValuesPerResponse = 100
	// dhtMaxTokenLen caps the get_peers token we are willing to echo back in
	// announce_peer. Common implementations use 4 to 20 bytes (ours are 8).
	dhtMaxTokenLen = 64
)

type candidateState uint8

const (
	candidateFresh candidateState = iota
	candidateQueried
	candidateResponded
	candidateFailed
)

type lookupCandidate struct {
	id    [20]byte
	key   nodeAddrKey
	dist  [20]byte
	state candidateState
	token string
}

// lookupSet is a Kademlia traversal's candidate set: at most
// dhtLookupCandidates nodes ordered by XOR distance to the target, with at
// most one per /24 for public addresses and one per IP for local ones
// (libtorrent's dht_restrict_search_ips), so one host, or one subnet, cannot
// steer the query budget with many ports, IDs or addresses.
type lookupSet struct {
	target [20]byte
	self   [20]byte
	list   []lookupCandidate
	seen   map[nodeAddrKey]struct{} // lookupKey of every candidate ever admitted
}

func newLookupSet(target, self [20]byte) *lookupSet {
	return &lookupSet{
		target: target,
		self:   self,
		list:   make([]lookupCandidate, 0, dhtLookupCandidates),
		seen:   make(map[nodeAddrKey]struct{}),
	}
}

// lookupKey is a candidate's identity in a lookup: its /24 for a diverse
// address, its probeKey (the IP, or the endpoint on loopback) otherwise.
func lookupKey(k nodeAddrKey) nodeAddrKey {
	if diverse(k.ip) {
		return nodeAddrKey{ip: [4]byte{k.ip[0], k.ip[1], k.ip[2], 0}}
	}
	return probeKey(k)
}

// add offers a node as a candidate. It is dropped if its /24 (or, for a local
// address, its IP) was already seen, or if the set is full and it is no closer
// than the farthest candidate.
func (l *lookupSet) add(id [20]byte, k nodeAddrKey) {
	if id == l.self {
		return
	}
	sk := lookupKey(k)
	if _, dup := l.seen[sk]; dup {
		return
	}
	dist := xorDistance(id, l.target)
	if len(l.list) == dhtLookupCandidates {
		if !lessXor(dist, l.list[len(l.list)-1].dist) {
			return
		}
		l.list = l.list[:len(l.list)-1]
	}
	l.seen[sk] = struct{}{}
	i := len(l.list)
	for i > 0 && lessXor(dist, l.list[i-1].dist) {
		i--
	}
	l.list = append(l.list, lookupCandidate{})
	copy(l.list[i+1:], l.list[i:])
	l.list[i] = lookupCandidate{id: id, key: k, dist: dist}
}

// addReferrals offers the nodes one responder referred us to. A responder may
// only point us at addresses no more local than itself, so a public node
// cannot aim our queries at loopback or LAN services. Only the dhtLookupK
// closest are taken: a BEP 5 answer carries at most K nodes, so this never
// trims an honest one, but a single responder can no longer fill the whole
// candidate set with close-sounding junk on many IPs and push out everything
// other responders told us about.
func (l *lookupSet) addReferrals(nodes []Node, responder *net.UDPAddr) {
	refs := make([]lookupCandidate, 0, len(nodes))
	for _, n := range nodes {
		if k, ok := nodeAddrKeyOf(n.Addr); ok && endpointAllowed(k, responder) {
			refs = append(refs, lookupCandidate{id: n.ID, key: k, dist: xorDistance(n.ID, l.target)})
		}
	}
	if len(refs) > dhtLookupK {
		sort.Slice(refs, func(i, j int) bool { return lessXor(refs[i].dist, refs[j].dist) })
		refs = refs[:dhtLookupK]
	}
	for _, r := range refs {
		l.add(r.id, r.key)
	}
}

// next marks up to max of the closest unqueried candidates as queried and
// returns them. Only the dhtLookupK closest candidates that have not failed
// are eligible, so an empty result means the lookup has converged: the K
// closest live nodes have all answered.
func (l *lookupSet) next(max int) []lookupCandidate {
	var batch []lookupCandidate
	live := 0
	for i := range l.list {
		c := &l.list[i]
		if c.state == candidateFailed {
			continue
		}
		if live == dhtLookupK || len(batch) == max {
			break
		}
		live++
		if c.state == candidateFresh {
			c.state = candidateQueried
			batch = append(batch, *c)
		}
	}
	return batch
}

// find returns the candidate at k, or nil if it has since been dropped.
func (l *lookupSet) find(k nodeAddrKey) *lookupCandidate {
	for i := range l.list {
		if l.list[i].key == k {
			return &l.list[i]
		}
	}
	return nil
}

// LookupOptions controls how a DHT lookup behaves.
type LookupOptions struct {
	// Announce publishes our peer port with announce_peer responses. Callers that
	// have not confirmed a torrent is public should leave this false.
	Announce bool
}

// Lookup queries the DHT swarm for a given torrent's info-hash and announces
// peerPort to nodes that return valid tokens. The lookup is queued; Lookup
// returns immediately.
func (d *DHT) Lookup(infoHash [20]byte, peerPort uint16) {
	d.LookupWithOptions(infoHash, peerPort, LookupOptions{Announce: true})
}

// LookupWithOptions queries the DHT swarm for a given torrent's info-hash. The
// lookup is queued; LookupWithOptions returns immediately. At most
// dhtMaxConcurrentLookups run at once and starts are spaced by
// dhtLookupStartInterval, with an info-hash's first lookup since the DHT
// started ahead of repeats. A request for an info-hash already queued is
// merged into it, and one for an info-hash whose lookup is still running is
// dropped: callers look up again on their own cadence.
func (d *DHT) LookupWithOptions(infoHash [20]byte, peerPort uint16, opts LookupOptions) {
	d.queueLookup(infoHash, peerPort, opts)
}

// lookup runs one get_peers lookup to completion on the calling goroutine. It
// is an iterative Kademlia traversal: each round queries up to
// dhtLookupParallelism of the closest unqueried candidates, and the lookup
// ends once the dhtLookupK closest live candidates have all answered, or after
// dhtLookupQueryLimit queries. It then announces to the closest responders
// that returned a token.
func (d *DHT) lookup(infoHash [20]byte, peerPort uint16, opts LookupOptions) {
	logEnabled := logging.Enabled()
	infoHashHex := ""
	if logEnabled {
		infoHashHex = fmt.Sprintf("%x", infoHash)
		logging.Debug("dht_lookup_started",
			logging.String("info_hash", infoHashHex),
			logging.Uint16("peer_port", peerPort),
			logging.Bool("announce", opts.Announce),
		)
	}
	queriesCount := 0
	discoveredPeers := 0
	defer func(start time.Time) {
		if logging.Enabled() {
			logging.Debug("dht_lookup_finished",
				logging.String("info_hash", infoHashHex),
				logging.Int("queries", queriesCount),
				logging.Int("peers", discoveredPeers),
				logging.Duration("duration", time.Since(start)),
			)
		}
	}(time.Now())

	startNodes := d.getCloserNodes(infoHash, dhtLookupStartNodes)
	if len(startNodes) == 0 {
		d.bootstrap()
		startNodes = d.awaitStartNodes(infoHash)
		if len(startNodes) == 0 {
			return
		}
	}

	set := newLookupSet(infoHash, d.nodeID)
	for _, n := range startNodes {
		if k, ok := nodeAddrKeyOf(n.Addr); ok {
			set.add(n.ID, k)
		}
	}

	// seenPeers dedups values across the lookup's responses, so the same peer
	// handed out by several nodes is published once.
	seenPeers := make(map[[6]byte]struct{})

	type lookupResult struct {
		key nodeAddrKey
		res *GetPeersResult
		err error
	}
	for queriesCount < dhtLookupQueryLimit {
		select {
		case <-d.ctx.Done():
			return
		default:
		}

		batch := set.next(min(dhtLookupParallelism, dhtLookupQueryLimit-queriesCount))
		if len(batch) == 0 {
			break
		}
		queriesCount += len(batch)

		results := make(chan lookupResult, len(batch))
		for _, c := range batch {
			k := c.key
			go func() {
				ctx, cancel := context.WithTimeout(d.ctx, dhtLookupQueryTimeout)
				res, err := d.getPeersQuery(ctx, infoHash, k.udpAddr())
				cancel()
				results <- lookupResult{key: k, res: res, err: err}
			}()
		}

		for range batch {
			result := <-results
			c := set.find(result.key)
			if result.err != nil {
				if c != nil {
					c.state = candidateFailed
				}
				continue
			}
			if c != nil {
				// A responder whose ID differs from the one it was referred
				// under sits at a bogus distance, so it does not count toward
				// convergence or receive our announce.
				if result.res.ID == c.id {
					c.state = candidateResponded
					c.token = result.res.Token
				} else {
					c.state = candidateFailed
				}
			}
			responder := result.key.udpAddr()

			// Store the responder under the ID it reported itself, never
			// the one a referrer claimed for its address.
			d.addNode(result.res.ID, responder)

			for _, cp := range result.res.Peers {
				var value [6]byte
				copy(value[:], cp)
				if _, dup := seenPeers[value]; dup {
					continue
				}
				seenPeers[value] = struct{}{}
				pk := nodeAddrKey{ip: [4]byte(value[:4]), port: binary.BigEndian.Uint16(value[4:])}
				// Some buggy DHT implementation hands out peers on port 1,
				// where nothing listens, so a dial would only waste an
				// outbound slot. Transmission drops these too (remove_bad_pex;
				// transmission issues #527 and #5218).
				if pk.port == 1 {
					continue
				}
				// We dial these, so a responder may only hand out peers no
				// more local than itself: a public node cannot aim our
				// connections at loopback or LAN services.
				if !endpointAllowed(pk, responder) {
					continue
				}
				ip := net.IP(append([]byte(nil), pk.ip[:]...))
				port := pk.port

				select {
				case d.peerChan <- DiscoveredPeer{
					InfoHash: infoHash,
					IP:       ip,
					Port:     port,
				}:
					discoveredPeers++
					if logging.Enabled() {
						logging.Debug("dht_peer_discovered",
							logging.String("info_hash", infoHashHex),
							logging.String("peer", net.JoinHostPort(ip.String(), strconv.Itoa(int(port)))),
						)
					}
				case <-d.ctx.Done():
					return
				default:
				}
			}

			set.addReferrals(result.res.Nodes, responder)
		}
	}

	if !opts.Announce || peerPort == 0 {
		return
	}
	// BEP 5: announce to the K closest nodes that returned a token, not to
	// every node the lookup happened to query.
	announced := 0
	for _, c := range set.list {
		if announced == dhtLookupK {
			break
		}
		if c.state != candidateResponded || c.token == "" {
			continue
		}
		announced++
		addr := c.key.udpAddr()
		token := c.token
		d.goTracked(func() {
			select {
			case <-d.ctx.Done():
				return
			default:
			}
			ctxAnn, cancelAnn := context.WithTimeout(d.ctx, dhtLookupQueryTimeout)
			defer cancelAnn()
			if err := d.announcePeerQuery(ctxAnn, infoHash, peerPort, token, addr); err != nil {
				if logging.Enabled() {
					logging.Debug("dht_announce_peer_failed",
						logging.String("info_hash", infoHashHex),
						logging.String("node", addr.String()),
						logging.Err(err),
					)
				}
			}
		})
	}
}

// awaitStartNodes waits for bootstrap to put a first contact in an empty table
// and returns the closest to target. A referral now costs a ping before it is
// admitted, a round trip more than before, so instead of checking once after a
// fixed second the lookup starts as soon as any contact is in, and gives slow
// links up to dhtBootstrapWait.
func (d *DHT) awaitStartNodes(target [20]byte) []Node {
	deadline := time.NewTimer(dhtBootstrapWait)
	defer deadline.Stop()
	poll := time.NewTicker(dhtBootstrapPoll)
	defer poll.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return nil
		case <-deadline.C:
			return d.getCloserNodes(target, dhtLookupStartNodes)
		case <-poll.C:
			if nodes := d.getCloserNodes(target, dhtLookupStartNodes); len(nodes) > 0 {
				return nodes
			}
		}
	}
}

func (d *DHT) generateNodeID() [20]byte {
	var id [20]byte
	_, _ = io.ReadFull(rand.Reader, id[:])
	return id
}

// nodesFileName is the routing-table snapshot kept in the download directory.
const nodesFileName = ".dht_nodes"

// maxNodesFileSize bounds how much of a nodes file is read. A full routing
// table encodes to well under 100 KiB.
const maxNodesFileSize = 1 << 20

var errNodesFileNotRegular = errors.New("dht nodes file is not a regular file")

// saveNodes snapshots the routing table's contacts to the nodes file.
func (d *DHT) saveNodes() {
	if d.downloadDir == "" {
		return
	}
	path := filepath.Join(d.downloadDir, nodesFileName)
	d.mu.RLock()
	var nodesList []interface{}
	for _, b := range d.buckets {
		if b != nil {
			for _, n := range b.nodes {
				nodesList = append(nodesList, map[string]interface{}{
					"id":   string(n.ID[:]),
					"addr": n.Addr.String(),
				})
			}
		}
	}
	d.mu.RUnlock()

	saveDict := map[string]interface{}{
		"nodes": nodesList,
	}

	data, err := bencode.Marshal(saveDict)
	if err != nil {
		return
	}

	_ = writeNodesFile(path, data)
}

// writeNodesFile replaces path atomically with a private (0600) file. The data
// goes to a new, randomly named file in the same directory that is renamed
// over path, so a reader never sees a partial file and nothing is ever
// written through an existing name: a symlink planted at path is replaced,
// not followed.
func writeNodesFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), nodesFileName+"-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_, err = tmp.Write(data)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpName, path)
	}
	if err != nil {
		_ = os.Remove(tmpName)
	}
	return err
}

// readNodesFile reads path only if it is a regular file of sane size, so a
// symlink, FIFO or device planted in a shared download directory is ignored
// rather than followed or waited on.
func readNodesFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxNodesFileSize {
		return nil, errNodesFileNotRegular
	}
	// The name may be swapped between Lstat and the open: openNodesFile
	// neither follows a symlink nor blocks on a FIFO, and what it opened must
	// be the regular file Lstat saw.
	f, err := openNodesFile(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() > maxNodesFileSize {
		return nil, errNodesFileNotRegular
	}
	return io.ReadAll(io.LimitReader(f, maxNodesFileSize))
}

// loadNodes seeds the routing table from contacts saved by an earlier run.
// d.nodeID must already be set; a node_id saved by older versions is ignored.
func (d *DHT) loadNodes() {
	if d.downloadDir == "" {
		return
	}
	data, err := readNodesFile(filepath.Join(d.downloadDir, nodesFileName))
	if err != nil {
		return
	}

	parsed, err := bencode.Unmarshal(data)
	if err != nil {
		return
	}

	dict, ok := parsed.(map[string]interface{})
	if !ok {
		return
	}

	nodesVal, exists := dict["nodes"]
	if !exists {
		return
	}

	list, ok := nodesVal.([]interface{})
	if !ok {
		return
	}

	for _, item := range list {
		dict, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		nodeIDStr, _ := dict["id"].(string)
		addrStr, _ := dict["addr"].(string)
		if len(nodeIDStr) != 20 || addrStr == "" {
			continue
		}

		var id [20]byte
		copy(id[:], nodeIDStr)

		// Parse without resolving: the file holds literal addresses, and a
		// hostname in it must not make startup wait on DNS.
		ap, err := netip.ParseAddrPort(addrStr)
		if err != nil || !ap.Addr().Unmap().Is4() || netpolicy.Classify(ap.Addr()) == netpolicy.ScopeInvalid {
			continue
		}
		addr := &net.UDPAddr{IP: ap.Addr().Unmap().AsSlice(), Port: int(ap.Port())}
		// A zero LastSeen leaves saved contacts questionable, so the first
		// newcomer to their bucket re-checks them before they can keep it,
		// and keeps them out of our answers until they are heard from.
		d.addNodeSeen(id, addr, time.Time{})
	}
}

func xorDistance(id1, id2 [20]byte) [20]byte {
	var dist [20]byte
	for i := 0; i < 20; i++ {
		dist[i] = id1[i] ^ id2[i]
	}
	return dist
}

func bucketIndex(id1, id2 [20]byte) int {
	for i := 0; i < 20; i++ {
		x := id1[i] ^ id2[i]
		if x != 0 {
			return i*8 + bits.LeadingZeros8(x)
		}
	}
	return 159
}

func compactNodes(nodes []Node) string {
	var buf bytes.Buffer
	for _, n := range nodes {
		ip4 := n.Addr.IP.To4()
		if ip4 == nil || n.Addr.Port <= 0 || n.Addr.Port > 65535 {
			continue
		}
		buf.Write(n.ID[:])
		buf.Write(ip4)
		var pBytes [2]byte
		binary.BigEndian.PutUint16(pBytes[:], uint16(n.Addr.Port))
		buf.Write(pBytes[:])
	}
	return buf.String()
}

// errMalformedNodes reports an answer whose "nodes" is not a compact node list.
var errMalformedNodes = errors.New("malformed nodes")

// nodesField parses the optional "nodes" entry of a response. It reports false
// when the entry is present but is not a well-formed compact node list.
func nodesField(r map[string]interface{}) ([]Node, bool) {
	v, exists := r["nodes"]
	if !exists {
		return nil, true
	}
	str, ok := v.(string)
	if !ok {
		return nil, false
	}
	return parseCompactNodes(str)
}

// parseCompactNodes decodes a BEP 5 compact node list: 26 bytes per node, a
// 20-byte ID then an IPv4 address and port. It reports false, and returns no
// nodes, when the length is not a multiple of 26: like jech/dht and rqbit we
// treat a ragged list as a broken answer rather than guess where it went
// wrong. Entries with an all-zero ID are skipped, as jech/dht does; no real
// node draws that ID.
func parseCompactNodes(s string) ([]Node, bool) {
	if len(s)%26 != 0 {
		return nil, false
	}
	data := []byte(s)
	var nodes []Node
	for len(data) >= 26 {
		var id [20]byte
		copy(id[:], data[0:20])
		if id != ([20]byte{}) {
			nodes = append(nodes, Node{
				ID: id,
				Addr: &net.UDPAddr{
					IP:   net.IP(data[20:24]),
					Port: int(binary.BigEndian.Uint16(data[24:26])),
				},
				LastSeen: time.Now(),
			})
		}
		data = data[26:]
	}
	return nodes, true
}
