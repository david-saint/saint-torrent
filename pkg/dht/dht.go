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
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/logging"
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
	peersMu      sync.Mutex                        // guards peersMap; separate from mu so announce_peer eviction never blocks getCloserNodes/generateToken/addNode
	peersMap     map[[20]byte]map[string]time.Time // infoHash -> peerAddr -> lastSeen
	tokenSecrets [2][20]byte
	tokenCreated time.Time
	peerChan     chan DiscoveredPeer
	transactions map[string]transaction
	txMu         sync.Mutex
	txCounter    uint32

	inFlightProbes map[string]struct{} // Track in-flight AddNode queries to endpoints

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
		peersMap:       make(map[[20]byte]map[string]time.Time),
		peerChan:       make(chan DiscoveredPeer, 256),
		transactions:   make(map[string]transaction),
		inFlightProbes: make(map[string]struct{}),
		ctx:            ctx,
		cancel:         cancel,
		downloadDir:    downloadDir,
	}
	_, _ = io.ReadFull(rand.Reader, d.tokenSecrets[0][:])
	_, _ = io.ReadFull(rand.Reader, d.tokenSecrets[1][:])
	d.tokenCreated = time.Now()

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

// Close stops the DHT listener and saves the routing table.
func (d *DHT) Close() {
	d.closeOnce.Do(func() {
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
		qStr, _ := dict["q"].(string)
		aDict, _ := dict["a"].(map[string]interface{})
		d.handleQuery(tStr, qStr, aDict, addr)
	case "r":
		rDict, _ := dict["r"].(map[string]interface{})
		d.handleResponse(tStr, rDict, addr)
	}
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

	d.addNode(senderID, addr)

	switch q {
	case "ping":
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

		closerNodes := d.getCloserNodes(targetID, 8)
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

		token := d.generateToken(addr)
		peers := d.getPeersForInfoHash(infoHash)
		if len(peers) > 0 {
			d.sendResponse(t, map[string]interface{}{
				"id":     string(d.nodeID[:]),
				"token":  token,
				"values": peers,
			}, addr)
		} else {
			closerNodes := d.getCloserNodes(infoHash, 8)
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
	if err == nil {
		_, _ = d.conn.WriteToUDP(payload, addr)
	}
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

func (d *DHT) getPeersForInfoHash(infoHash [20]byte) []interface{} {
	d.peersMu.Lock()
	defer d.peersMu.Unlock()

	peers, ok := d.peersMap[infoHash]
	if !ok {
		return nil
	}

	// Prune expired entries (TTL of 30 minutes)
	now := time.Now()
	for addrStr, lastSeen := range peers {
		if now.Sub(lastSeen) > 30*time.Minute {
			delete(peers, addrStr)
		}
	}

	if len(peers) == 0 {
		delete(d.peersMap, infoHash)
		return nil
	}

	var list []interface{}
	for addrStr := range peers {
		if len(list) >= 50 {
			break
		}
		host, portStr, err := net.SplitHostPort(addrStr)
		if err != nil {
			continue
		}
		ip := net.ParseIP(host).To4()
		port, err := net.LookupPort("tcp", portStr)
		if ip != nil && err == nil && port > 0 && port <= 65535 {
			var comp [6]byte
			copy(comp[0:4], ip)
			binary.BigEndian.PutUint16(comp[4:6], uint16(port))
			list = append(list, string(comp[:]))
		}
	}
	return list
}

func (d *DHT) registerPeer(infoHash [20]byte, ip net.IP, port uint16) {
	ip4 := ip.To4()
	if ip4 == nil {
		return
	}

	d.peersMu.Lock()
	defer d.peersMu.Unlock()

	now := time.Now()

	// Prune expired entries from the current infoHash
	if peers, exists := d.peersMap[infoHash]; exists {
		for addr, lastSeen := range peers {
			if now.Sub(lastSeen) > 30*time.Minute {
				delete(peers, addr)
			}
		}
		if len(peers) == 0 {
			delete(d.peersMap, infoHash)
		}
	}

	// Limit total info-hashes stored to 500 (evicting an expired/empty or random one when space is needed)
	if d.peersMap[infoHash] == nil {
		if len(d.peersMap) >= 500 {
			var evictedHash [20]byte
			foundEvictable := false
			for h, peers := range d.peersMap {
				for addr, lastSeen := range peers {
					if now.Sub(lastSeen) > 30*time.Minute {
						delete(peers, addr)
					}
				}
				if len(peers) == 0 {
					evictedHash = h
					foundEvictable = true
					break
				}
			}
			if !foundEvictable {
				for h := range d.peersMap {
					evictedHash = h
					foundEvictable = true
					break
				}
			}
			if foundEvictable {
				delete(d.peersMap, evictedHash)
			}
		}

		d.peersMap[infoHash] = make(map[string]time.Time)
	}

	addrStr := net.JoinHostPort(ip.String(), fmt.Sprintf("%d", port))

	// Limit peers stored per info-hash to 50 (evicting the oldest peer when full)
	peers := d.peersMap[infoHash]
	_, exists := peers[addrStr]
	if !exists && len(peers) >= 50 {
		var oldestAddr string
		var oldestTime time.Time
		first := true
		for addr, lastSeen := range peers {
			if first || lastSeen.Before(oldestTime) {
				oldestAddr = addr
				oldestTime = lastSeen
				first = false
			}
		}
		if oldestAddr != "" {
			delete(peers, oldestAddr)
		}
	}

	d.peersMap[infoHash][addrStr] = now
}

// getCloserNodes returns up to count nodes from the routing table closest to
// target, ordered nearest-first. Rather than copying every node out of the
// table and sorting the copy (which recomputes each XOR distance on every
// comparison), it keeps a small sorted candidate slice of size <= count and
// inserts each node into it in place, computing its distance exactly once.
func (d *DHT) getCloserNodes(target [20]byte, count int) []Node {
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
			dist := xorDistance(n.ID, target)

			if len(best) < count {
				best = append(best, candidate{node: n, dist: dist})
				for i := len(best) - 1; i > 0 && lessXor(best[i].dist, best[i-1].dist); i-- {
					best[i], best[i-1] = best[i-1], best[i]
				}
				continue
			}

			if lessXor(dist, best[count-1].dist) {
				best[count-1] = candidate{node: n, dist: dist}
				for i := count - 1; i > 0 && lessXor(best[i].dist, best[i-1].dist); i-- {
					best[i], best[i-1] = best[i-1], best[i]
				}
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

func (d *DHT) addNode(id [20]byte, addr *net.UDPAddr) {
	if id == d.nodeID {
		return
	}
	if addr.IP.To4() == nil {
		return
	}

	idx := bucketIndex(d.nodeID, id)
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.buckets[idx] == nil {
		d.buckets[idx] = &bucket{}
	}

	b := d.buckets[idx]
	foundIdx := -1
	for i, n := range b.nodes {
		if n.ID == id {
			foundIdx = i
			break
		}
	}

	if foundIdx != -1 {
		n := b.nodes[foundIdx]
		n.Addr = addr
		n.LastSeen = time.Now()
		b.nodes = append(b.nodes[:foundIdx], b.nodes[foundIdx+1:]...)
		b.nodes = append(b.nodes, n)
	} else {
		if len(b.nodes) < 8 {
			b.nodes = append(b.nodes, Node{
				ID:       id,
				Addr:     addr,
				LastSeen: time.Now(),
			})
		} else {
			if b.pingInProgress {
				return
			}
			b.pingInProgress = true
			// Asynchronously ping the head node and replace if offline
			head := b.nodes[0]
			d.goTracked(func() {
				defer func() {
					d.mu.Lock()
					b2 := d.buckets[idx]
					if b2 != nil {
						b2.pingInProgress = false
					}
					d.mu.Unlock()
				}()
				headNode := head
				newID := id
				newAddr := addr
				ctx, cancel := context.WithTimeout(d.ctx, 2*time.Second)
				defer cancel()
				err := d.pingNode(ctx, headNode.Addr)
				d.mu.Lock()
				defer d.mu.Unlock()
				b2 := d.buckets[idx]
				if b2 == nil {
					return
				}
				if err != nil {
					// Evict head and add new
					for i, n := range b2.nodes {
						if n.ID == headNode.ID {
							b2.nodes = append(b2.nodes[:i], b2.nodes[i+1:]...)
							b2.nodes = append(b2.nodes, Node{
								ID:       newID,
								Addr:     newAddr,
								LastSeen: time.Now(),
							})
							break
						}
					}
				} else {
					// Respond, update mtime
					for i, n := range b2.nodes {
						if n.ID == headNode.ID {
							b2.nodes = append(b2.nodes[:i], b2.nodes[i+1:]...)
							headNode.LastSeen = time.Now()
							b2.nodes = append(b2.nodes, headNode)
							break
						}
					}
				}
			})
		}
	}
}

// HasNodeAddress returns true if a node with the given IP and UDP port exists in the routing table.
func (d *DHT) HasNodeAddress(ip net.IP, port uint16) bool {
	if d == nil {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, b := range d.buckets {
		if b == nil {
			continue
		}
		for _, n := range b.nodes {
			if n.Addr != nil && n.Addr.IP.Equal(ip) && n.Addr.Port == int(port) {
				return true
			}
		}
	}
	return false
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
	ip4 := ip.To4()
	if ip4 == nil {
		// Silently ignore IPv6 addresses as the DHT UDP socket is IPv4-bound.
		return
	}
	if port == 0 {
		return
	}

	addr := &net.UDPAddr{IP: append(net.IP(nil), ip4...), Port: int(port)}

	// Skip if the node is already in our routing table
	if d.HasNodeAddress(ip4, port) {
		return
	}

	addrStr := addr.String()
	d.txMu.Lock()
	if d.inFlightProbes == nil {
		d.inFlightProbes = make(map[string]struct{})
	}
	// Deduplicate: don't spawn multiple queries to the same address concurrently
	if _, active := d.inFlightProbes[addrStr]; active {
		d.txMu.Unlock()
		return
	}
	// Rate-limit: bound maximum concurrent unsolicited AddNode probes to 100
	if len(d.inFlightProbes) >= 100 {
		d.txMu.Unlock()
		return
	}
	d.inFlightProbes[addrStr] = struct{}{}
	d.txMu.Unlock()

	d.goTracked(func() {
		defer func() {
			d.txMu.Lock()
			delete(d.inFlightProbes, addrStr)
			d.txMu.Unlock()
		}()

		select {
		case <-d.ctx.Done():
			return
		default:
		}
		ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
		defer cancel()
		id, err := d.queryNodeID(ctx, addr)
		if err != nil {
			return
		}
		d.addNode(id, addr)
	})
}

func (d *DHT) pingNode(ctx context.Context, addr *net.UDPAddr) error {
	_, err := d.queryNodeID(ctx, addr)
	return err
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
		nodesStr, _ := rDict["nodes"].(string)
		return parseCompactNodes(nodesStr), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetPeersResult contains token, closer nodes, or discovered peers.
type GetPeersResult struct {
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
		token, _ := rDict["token"].(string)
		res := &GetPeersResult{Token: token}

		if val, exists := rDict["values"]; exists {
			list, ok := val.([]interface{})
			if ok {
				for _, item := range list {
					s, ok := item.(string)
					if ok {
						res.Peers = append(res.Peers, s)
					}
				}
			}
		}
		if nodesVal, exists := rDict["nodes"]; exists {
			nodesStr, _ := nodesVal.(string)
			res.Nodes = parseCompactNodes(nodesStr)
		}
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
				for _, node := range nodes {
					d.addNode(node.ID, node.Addr)
				}
			}
		})
	}
}

const (
	dhtLookupStartNodes  = 32
	dhtLookupQueryLimit  = 128
	dhtLookupParallelism = 8
)

// LookupOptions controls how a DHT lookup behaves.
type LookupOptions struct {
	// Announce publishes our peer port with announce_peer responses. Callers that
	// have not confirmed a torrent is public should leave this false.
	Announce bool
}

// Lookup queries the DHT swarm for a given torrent's info-hash and announces
// peerPort to nodes that return valid tokens.
func (d *DHT) Lookup(infoHash [20]byte, peerPort uint16) {
	d.LookupWithOptions(infoHash, peerPort, LookupOptions{Announce: true})
}

// LookupWithOptions queries the DHT swarm for a given torrent's info-hash.
func (d *DHT) LookupWithOptions(infoHash [20]byte, peerPort uint16, opts LookupOptions) {
	d.goTracked(func() {
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
			select {
			case <-time.After(1 * time.Second):
			case <-d.ctx.Done():
				return
			}
			startNodes = d.getCloserNodes(infoHash, dhtLookupStartNodes)
			if len(startNodes) == 0 {
				return
			}
		}

		visited := make(map[string]bool)
		queue := append([]Node{}, startNodes...)

		for len(queue) > 0 && queriesCount < dhtLookupQueryLimit {
			select {
			case <-d.ctx.Done():
				return
			default:
			}

			batch := make([]Node, 0, dhtLookupParallelism)
			for len(queue) > 0 && len(batch) < dhtLookupParallelism && queriesCount < dhtLookupQueryLimit {
				curr := queue[0]
				queue = queue[1:]

				addrStr := curr.Addr.String()
				if visited[addrStr] {
					continue
				}
				visited[addrStr] = true
				queriesCount++
				batch = append(batch, curr)
			}
			if len(batch) == 0 {
				continue
			}

			type lookupResult struct {
				node Node
				res  *GetPeersResult
				err  error
			}
			results := make(chan lookupResult, len(batch))
			for _, node := range batch {
				n := node
				go func() {
					ctx, cancel := context.WithTimeout(d.ctx, 3*time.Second)
					res, err := d.getPeersQuery(ctx, infoHash, n.Addr)
					cancel()
					results <- lookupResult{node: n, res: res, err: err}
				}()
			}

			for range batch {
				result := <-results
				if result.err != nil {
					continue
				}

				d.addNode(result.node.ID, result.node.Addr)

				for _, cp := range result.res.Peers {
					if len(cp) != 6 {
						continue
					}
					ip := net.IP([]byte(cp[0:4]))
					port := binary.BigEndian.Uint16([]byte(cp[4:6]))
					if port == 0 {
						continue
					}

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

				if opts.Announce && result.res.Token != "" && peerPort != 0 {
					n := result.node
					token := result.res.Token
					d.goTracked(func() {
						select {
						case <-d.ctx.Done():
							return
						default:
						}
						ctxAnn, cancelAnn := context.WithTimeout(d.ctx, 3*time.Second)
						defer cancelAnn()
						if err := d.announcePeerQuery(ctxAnn, infoHash, peerPort, token, n.Addr); err != nil {
							if logging.Enabled() {
								logging.Debug("dht_announce_peer_failed",
									logging.String("info_hash", infoHashHex),
									logging.String("node", n.Addr.String()),
									logging.Err(err),
								)
							}
						}
					})
				}

				for _, n := range result.res.Nodes {
					if !visited[n.Addr.String()] {
						queue = append(queue, n)
					}
				}
			}
		}
	})
}

func (d *DHT) generateNodeID() [20]byte {
	var id [20]byte
	_, _ = io.ReadFull(rand.Reader, id[:])
	return id
}

func (d *DHT) saveNodes() {
	if d.downloadDir == "" {
		return
	}
	path := filepath.Join(d.downloadDir, ".dht_nodes")
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
		"node_id": string(d.nodeID[:]),
		"nodes":   nodesList,
	}

	data, err := bencode.Marshal(saveDict)
	if err != nil {
		return
	}

	_ = os.WriteFile(path, data, 0644)
}

func (d *DHT) loadNodes() {
	if d.downloadDir == "" {
		d.nodeID = d.generateNodeID()
		return
	}
	path := filepath.Join(d.downloadDir, ".dht_nodes")
	data, err := os.ReadFile(path)
	if err != nil {
		d.nodeID = d.generateNodeID()
		return
	}

	parsed, err := bencode.Unmarshal(data)
	if err != nil {
		d.nodeID = d.generateNodeID()
		return
	}

	dict, ok := parsed.(map[string]interface{})
	if !ok {
		d.nodeID = d.generateNodeID()
		return
	}

	idStr, ok := dict["node_id"].(string)
	if ok && len(idStr) == 20 {
		copy(d.nodeID[:], idStr)
	} else {
		d.nodeID = d.generateNodeID()
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

		addr, err := net.ResolveUDPAddr("udp", addrStr)
		if err == nil {
			d.addNode(id, addr)
		}
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

func parseCompactNodes(s string) []Node {
	data := []byte(s)
	var nodes []Node
	for len(data) >= 26 {
		var id [20]byte
		copy(id[:], data[0:20])
		ip := net.IP(data[20:24])
		port := binary.BigEndian.Uint16(data[24:26])
		nodes = append(nodes, Node{
			ID: id,
			Addr: &net.UDPAddr{
				IP:   ip,
				Port: int(port),
			},
			LastSeen: time.Now(),
		})
		data = data[26:]
	}
	return nodes
}
