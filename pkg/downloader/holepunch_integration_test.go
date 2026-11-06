package downloader

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// TestHolepunchConnectsTwoNATedPeersThroughRelay runs BEP 55 end to end with
// three real managers on loopback. R is reachable; A and B each sit behind an
// emulated port-restricted cone NAT (utp.Socket.EmulateNAT) with inbound TCP
// blocked, so neither can dial the other directly. Both connect out to R. R
// lists B to A over PEX (B connected in, so it is listed at the listen port
// its extension handshake announced), A's direct dial to B fails, A sends R a
// rendezvous, R sends both a connect, and their simultaneous uTP dials open
// each NAT to the other. B seeds the torrent and R wants none of it, so A can
// only complete over that holepunched A<->B connection.
func TestHolepunchConnectsTwoNATedPeersThroughRelay(t *testing.T) {
	oldPEX, oldDial, oldRetry, oldTCPDial := pexInterval, peerDialTimeout, holepunchRetryInterval, peerTCPDial
	pexInterval = 200 * time.Millisecond
	peerDialTimeout = time.Second
	holepunchRetryInterval = 500 * time.Millisecond
	t.Cleanup(func() {
		pexInterval, peerDialTimeout, holepunchRetryInterval, peerTCPDial = oldPEX, oldDial, oldRetry, oldTCPDial
	})

	// Inbound TCP to a NATed peer never arrives.
	var natMu sync.Mutex
	natPorts := map[string]bool{}
	peerTCPDial = func(ctx context.Context, addr string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(addr)
		natMu.Lock()
		blocked := natPorts[port]
		natMu.Unlock()
		if blocked {
			<-ctx.Done()
			return nil, errors.New("tcp dial into NAT dropped")
		}
		return oldTCPDial(ctx, addr)
	}

	const pieceLen = 16 * 1024
	const numPieces = 4
	data := make([]byte, pieceLen*numPieces)
	for i := range data {
		data[i] = byte(i*31 + i/pieceLen)
	}
	hashes := make([][20]byte, numPieces)
	for i := range hashes {
		hashes[i] = sha1.Sum(data[i*pieceLen : (i+1)*pieceLen])
	}
	newTorrent := func() *torrent.Torrent {
		return &torrent.Torrent{
			Name:        "holepunch.bin",
			InfoHash:    sha1.Sum([]byte("holepunch integration")),
			PieceLength: pieceLen,
			PieceHashes: hashes,
			Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"holepunch.bin"}}},
		}
	}

	type node struct {
		mgr  *TorrentManager
		sess *Session
		addr string // listen endpoint, TCP and uTP alike
	}
	newNode := func(name string, id byte, nat bool) *node {
		t.Helper()
		mgr := NewTorrentManager()
		if err := mgr.StartPeerListener(0); err != nil {
			t.Fatalf("%s: peer listener: %v", name, err)
		}
		if err := mgr.StartDHT(t.TempDir(), int(mgr.PeerListenPort())); err != nil {
			mgr.Close()
			t.Fatalf("%s: uTP/DHT socket: %v", name, err)
		}
		t.Cleanup(mgr.Close)
		port := strconv.Itoa(int(mgr.PeerListenPort()))
		if nat {
			mgr.utpSocket.EmulateNAT()
			natMu.Lock()
			natPorts[port] = true
			natMu.Unlock()
		}
		tor := newTorrent()
		st, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{{Path: "holepunch.bin", Length: int64(len(data))}}, pieceLen)
		if err != nil {
			t.Fatalf("%s: storage: %v", name, err)
		}
		var peerID [20]byte
		for i := range peerID {
			peerID[i] = id
		}
		sess, err := NewSession(tor, st, peerID, 0, t.TempDir())
		if err != nil {
			t.Fatalf("%s: session: %v", name, err)
		}
		if err := mgr.AddSession(fmt.Sprintf("%x", tor.InfoHash), sess); err != nil {
			t.Fatalf("%s: add session: %v", name, err)
		}
		return &node{mgr: mgr, sess: sess, addr: net.JoinHostPort("127.0.0.1", port)}
	}

	relay := newNode("relay", 'R', false)
	seeder := newNode("seeder", 'B', true)
	leecher := newNode("leecher", 'A', true)

	for i := 0; i < numPieces; i++ {
		if err := seeder.sess.Storage.WriteBlock(int64(i), 0, data[i*pieceLen:(i+1)*pieceLen]); err != nil {
			t.Fatalf("seed piece %d: %v", i, err)
		}
	}
	seeder.sess.mu.Lock()
	for i := 0; i < numPieces; i++ {
		seeder.sess.setPieceStateLocked(i, PieceCompleted)
	}
	seeder.sess.mu.Unlock()
	// The relay downloads nothing, so it has nothing to pass on to A.
	relay.sess.SetFilePriority(0, PrioritySkip)

	for _, n := range []*node{relay, seeder, leecher} {
		n.sess.Start()
	}
	// Both NATed peers know the relay (from a tracker, say) and connect out.
	seeder.sess.addPeer(relay.addr, false)
	leecher.sess.addPeer(relay.addr, false)

	connectedTo := func(n *node, addr string) bool {
		n.sess.mu.RLock()
		defer n.sess.mu.RUnlock()
		lc := n.sess.liveConns[addr]
		return lc != nil && lc.utp && n.sess.activePeers[addr] == lc.client
	}
	deadline := time.Now().Add(30 * time.Second)
	for leecher.sess.PercentComplete() != 100 || !connectedTo(leecher, seeder.addr) || !connectedTo(seeder, leecher.addr) {
		if time.Now().After(deadline) {
			t.Fatalf("no holepunched transfer: leecher %.0f%% complete, leecher->seeder uTP %v, seeder->leecher uTP %v",
				leecher.sess.PercentComplete(), connectedTo(leecher, seeder.addr), connectedTo(seeder, leecher.addr))
		}
		time.Sleep(20 * time.Millisecond)
	}

	got := make([]byte, len(data))
	for i := 0; i < numPieces; i++ {
		if _, err := leecher.sess.Storage.ReadBlock(int64(i), 0, got[i*pieceLen:(i+1)*pieceLen]); err != nil {
			t.Fatalf("read piece %d: %v", i, err)
		}
	}
	if string(got) != string(data) {
		t.Fatal("leecher's data does not match the seeder's")
	}
	relay.sess.mu.RLock()
	defer relay.sess.mu.RUnlock()
	for i, st := range relay.sess.PieceStates {
		if st == PieceCompleted {
			t.Fatalf("relay has piece %d; it must not be a source", i)
		}
	}
}
