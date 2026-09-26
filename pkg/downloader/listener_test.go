package downloader

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/utp"
)

func TestManagerSharedListenerRoutesByInfoHash(t *testing.T) {
	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start shared peer listener: %v", err)
	}
	defer mgr.Close()

	newManagedSession := func(name string) *Session {
		infoHash := sha1.Sum([]byte(name))
		tor := &torrent.Torrent{
			Name:        name,
			InfoHash:    infoHash,
			PieceLength: 1,
			PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
			Files:       []torrent.File{{Length: 1, Path: []string{name}}},
		}
		sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		mgr.AddSession(fmt.Sprintf("%x", infoHash), sess)
		sess.Start()
		return sess
	}

	first := newManagedSession("first")
	second := newManagedSession("second")
	if first.Port == 0 || first.Port != second.Port || first.Port != mgr.PeerListenPort() {
		t.Fatalf("expected sessions to share port %d, got %d and %d",
			mgr.PeerListenPort(), first.Port, second.Port)
	}
	if first.listener != nil || second.listener != nil {
		t.Fatal("managed sessions should not own individual listeners")
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", mgr.PeerListenPort()))
	if err != nil {
		t.Fatalf("failed to dial shared listener: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	handshake := &peer.Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: second.Torrent.InfoHash,
		PeerID:   [20]byte{9, 9, 9},
	}
	if _, err := conn.Write(handshake.Serialize()); err != nil {
		t.Fatalf("failed to write handshake: %v", err)
	}
	response, err := peer.ParseHandshake(conn)
	if err != nil {
		t.Fatalf("failed to read handshake response: %v", err)
	}
	if response.InfoHash != second.Torrent.InfoHash {
		t.Fatalf("expected response for second torrent, got %x", response.InfoHash)
	}

	deadline := time.Now().Add(time.Second)
	for len(second.GetActivePeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared listener did not route connection to the matching session")
		}
		time.Sleep(time.Millisecond)
	}
	if len(first.GetActivePeers()) != 0 {
		t.Fatal("shared listener routed connection to the wrong session")
	}
}

func TestManagerSharedUTPListenerRoutesByInfoHash(t *testing.T) {
	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start shared peer listener: %v", err)
	}
	if err := mgr.StartDHT(t.TempDir(), int(mgr.PeerListenPort())); err != nil {
		t.Fatalf("failed to start shared UDP/DHT listener: %v", err)
	}
	defer mgr.Close()

	newManagedSession := func(name string) *Session {
		infoHash := sha1.Sum([]byte(name))
		tor := &torrent.Torrent{
			Name:        name,
			InfoHash:    infoHash,
			PieceLength: 1,
			PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
			Files:       []torrent.File{{Length: 1, Path: []string{name}}},
		}
		sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		mgr.AddSession(fmt.Sprintf("%x", infoHash), sess)
		sess.Start()
		return sess
	}

	first := newManagedSession("first-utp")
	second := newManagedSession("second-utp")
	if mgr.DHTListenPort() == 0 || mgr.DHTListenPort() != mgr.PeerListenPort() {
		t.Fatalf("expected DHT/uTP UDP port to match TCP listen port %d, got %d",
			mgr.PeerListenPort(), mgr.DHTListenPort())
	}
	if first.utpSocket == nil || second.utpSocket == nil {
		t.Fatal("managed sessions did not receive the shared uTP socket")
	}

	clientSocket, err := utp.NewSocket(0)
	if err != nil {
		t.Fatalf("failed to create client uTP socket: %v", err)
	}
	defer clientSocket.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := clientSocket.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", mgr.DHTListenPort()))
	if err != nil {
		t.Fatalf("failed to dial shared uTP listener: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	handshake := &peer.Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: second.Torrent.InfoHash,
		PeerID:   [20]byte{9, 9, 9},
	}
	if _, err := conn.Write(handshake.Serialize()); err != nil {
		t.Fatalf("failed to write handshake: %v", err)
	}
	response, err := peer.ParseHandshake(conn)
	if err != nil {
		t.Fatalf("failed to read handshake response: %v", err)
	}
	if response.InfoHash != second.Torrent.InfoHash {
		t.Fatalf("expected response for second torrent, got %x", response.InfoHash)
	}

	deadline := time.Now().Add(time.Second)
	for len(second.GetActivePeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared uTP listener did not route connection to the matching session")
		}
		time.Sleep(time.Millisecond)
	}
	if len(first.GetActivePeers()) != 0 {
		t.Fatal("shared uTP listener routed connection to the wrong session")
	}
}

// TestManagerSharedUTPListenerRoutesEncryptedConnection runs a required-MSE
// handshake over uTP end to end: the dialer's handshake ACK promotes the
// half-open conn, the receiver waits for Ya before answering, and both sides'
// coalesced flights cross a transport whose writes wait for acks.
func TestManagerSharedUTPListenerRoutesEncryptedConnection(t *testing.T) {
	mgr := NewTorrentManager()
	mgr.SetEncryptionPolicy(mse.PolicyRequire)
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start shared peer listener: %v", err)
	}
	if err := mgr.StartDHT(t.TempDir(), int(mgr.PeerListenPort())); err != nil {
		t.Fatalf("failed to start shared UDP/DHT listener: %v", err)
	}
	defer mgr.Close()
	sess := newEncryptionTestManagedSession(t, mgr, "encrypted-utp")

	clientSocket, err := utp.NewSocket(0)
	if err != nil {
		t.Fatalf("failed to create client uTP socket: %v", err)
	}
	defer clientSocket.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := clientSocket.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", mgr.DHTListenPort()))
	if err != nil {
		t.Fatalf("failed to dial shared uTP listener: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	wrapped, _, err := mse.Initiate(conn, sess.Torrent.InfoHash[:], nil, mse.CryptoMethodRC4)
	if err != nil {
		t.Fatalf("MSE initiate over uTP failed: %v", err)
	}
	client := peer.NewClient(wrapped, sess.Torrent.InfoHash, [20]byte{9, 9, 9})
	response, err := client.Handshake()
	if err != nil {
		t.Fatalf("encrypted peer handshake over uTP failed: %v", err)
	}
	if response.InfoHash != sess.Torrent.InfoHash {
		t.Fatalf("expected response for %x, got %x", sess.Torrent.InfoHash, response.InfoHash)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(sess.GetActivePeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared uTP listener did not route the encrypted connection")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestManagerSecretKeysSnapshotTracksSessionLifecycle(t *testing.T) {
	mgr := NewTorrentManager()

	first := newEncryptionTestSession(t, "secret-cache-first")
	second := newEncryptionTestSession(t, "secret-cache-second")
	firstHex := fmt.Sprintf("%x", first.Torrent.InfoHash)
	secondHex := fmt.Sprintf("%x", second.Torrent.InfoHash)

	mgr.AddSession(firstHex, first)
	before := mgr.secretKeySnapshot()
	mgr.AddSession(secondHex, second)

	secrets := mgr.secretKeySnapshot()
	if len(secrets) != 2 || !hasSecret(secrets, first.Torrent.InfoHash) || !hasSecret(secrets, second.Torrent.InfoHash) {
		t.Fatalf("unexpected cached secrets after add: %v", secrets)
	}
	// A snapshot taken earlier is never modified, so it can be read
	// without the manager lock.
	if len(before) != 1 || !hasSecret(before, first.Torrent.InfoHash) {
		t.Fatalf("earlier snapshot changed: %v", before)
	}

	if err := mgr.RemoveSession(firstHex, false); err != nil {
		t.Fatalf("remove first session: %v", err)
	}
	secrets = mgr.secretKeySnapshot()
	if len(secrets) != 1 || hasSecret(secrets, first.Torrent.InfoHash) || !hasSecret(secrets, second.Torrent.InfoHash) {
		t.Fatalf("unexpected cached secrets after remove: %v", secrets)
	}

	mgr.Close()
	if secrets := mgr.secretKeySnapshot(); len(secrets) != 0 {
		t.Fatalf("expected cached secrets to clear on close, got %v", secrets)
	}
}

// TestSecretKeySetLookupAndRefcount checks the obfuscated-hash index: a
// lookup by HASH('req2', infohash) finds the info hash, and an info hash
// added twice stays until removed twice, as the old list of keys did.
func TestSecretKeySetLookupAndRefcount(t *testing.T) {
	a := sha1.Sum([]byte("a"))
	b := sha1.Sum([]byte("b"))
	var set secretKeySet
	set = set.with(a).with(b).with(a)
	if got, ok := set.lookup(mse.ObfuscatedHash(a[:])); !ok || !bytes.Equal(got, a[:]) {
		t.Fatalf("lookup(a) = %x, %v", got, ok)
	}
	if _, ok := set.lookup(sha1.Sum(a[:])); ok {
		t.Fatal("lookup by the plain info hash must not match")
	}
	set = set.without(a)
	if _, ok := set.lookup(mse.ObfuscatedHash(a[:])); !ok {
		t.Fatal("info hash added twice was dropped by a single removal")
	}
	set = set.without(a).without(sha1.Sum([]byte("never added")))
	if _, ok := set.lookup(mse.ObfuscatedHash(a[:])); ok || len(set) != 1 {
		t.Fatalf("after removals: len=%d, a still present=%v", len(set), ok)
	}
}

func (m *TorrentManager) secretKeySnapshot() secretKeySet {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.secretKeys
}

func hasSecret(secrets secretKeySet, want [20]byte) bool {
	got, ok := secrets.lookup(mse.ObfuscatedHash(want[:]))
	return ok && bytes.Equal(got, want[:])
}
