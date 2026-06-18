package downloader

import (
	"crypto/sha1"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/tracker"
)

func TestManagerSharedListenerRoutesEncryptedConnectionByInfoHash(t *testing.T) {
	mgr := NewTorrentManager()
	mgr.SetEncryptionPolicy(mse.PolicyRequire)
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start shared peer listener: %v", err)
	}
	defer mgr.Close()

	first := newEncryptionTestManagedSession(t, mgr, "encrypted-first")
	second := newEncryptionTestManagedSession(t, mgr, "encrypted-second")

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", mgr.PeerListenPort()))
	if err != nil {
		t.Fatalf("failed to dial shared listener: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	wrapped, _, err := mse.Initiate(conn, second.Torrent.InfoHash[:], nil, mse.CryptoMethodRC4)
	if err != nil {
		t.Fatalf("MSE initiate failed: %v", err)
	}
	client := peer.NewClient(wrapped, second.Torrent.InfoHash, [20]byte{9, 9, 9})
	response, err := client.Handshake()
	if err != nil {
		t.Fatalf("encrypted peer handshake failed: %v", err)
	}
	if response.InfoHash != second.Torrent.InfoHash {
		t.Fatalf("expected response for second torrent, got %x", response.InfoHash)
	}

	deadline := time.Now().Add(time.Second)
	for len(second.GetActivePeers()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("shared listener did not route encrypted connection to the matching session")
		}
		time.Sleep(time.Millisecond)
	}
	if len(first.GetActivePeers()) != 0 {
		t.Fatal("shared listener routed encrypted connection to the wrong session")
	}
}

func TestConnectToPeerWithRequiredEncryption(t *testing.T) {
	sess := newEncryptionTestSession(t, "required-outgoing")
	sess.EncryptionPolicy = mse.PolicyRequire

	ln, port := listenEncryptionTestPeer(t)
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

		wrapped, _, err := mse.Receive(conn, secretKeyIter(sess.Torrent.InfoHash), mse.SelectRC4)
		if err != nil {
			serverDone <- fmt.Errorf("receive MSE: %w", err)
			return
		}
		handshake, err := peer.ParseHandshake(wrapped)
		if err != nil {
			serverDone <- fmt.Errorf("parse handshake: %w", err)
			return
		}
		if handshake.InfoHash != sess.Torrent.InfoHash {
			serverDone <- fmt.Errorf("info hash = %x, want %x", handshake.InfoHash, sess.Torrent.InfoHash)
			return
		}
		resp := &peer.Handshake{
			Pstr:     "BitTorrent protocol",
			InfoHash: sess.Torrent.InfoHash,
			PeerID:   [20]byte{7, 7, 7},
		}
		if _, err := wrapped.Write(resp.Serialize()); err != nil {
			serverDone <- fmt.Errorf("write response: %w", err)
			return
		}
		msg, err := peer.ParseMessage(wrapped)
		if err != nil {
			serverDone <- fmt.Errorf("read initial message: %w", err)
			return
		}
		if msg == nil || msg.ID != peer.MsgInterested {
			serverDone <- fmt.Errorf("initial message = %#v, want interested", msg)
			return
		}
		serverDone <- nil
	}()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		sess.connectToPeer(tracker.Peer{IP: net.ParseIP("127.0.0.1"), Port: uint16(port)})
	}()

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("encrypted peer did not complete handshake")
	}
	select {
	case <-clientDone:
	case <-time.After(3 * time.Second):
		t.Fatal("connectToPeer did not exit after encrypted peer closed")
	}
}

func TestConnectToPeerPreferFallsBackToPlaintext(t *testing.T) {
	sess := newEncryptionTestSession(t, "prefer-fallback")
	sess.EncryptionPolicy = mse.PolicyPrefer

	ln, port := listenEncryptionTestPeer(t)
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		first, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		_ = first.SetDeadline(time.Now().Add(2 * time.Second))
		prefix := make([]byte, mse.PlaintextHandshakePrefixLen())
		if _, err := io.ReadFull(first, prefix); err != nil {
			_ = first.Close()
			serverDone <- fmt.Errorf("read first attempt prefix: %w", err)
			return
		}
		_ = first.Close()
		if mse.LooksLikePlaintextHandshake(prefix) {
			serverDone <- fmt.Errorf("first prefer attempt was plaintext")
			return
		}

		second, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer second.Close()
		_ = second.SetDeadline(time.Now().Add(2 * time.Second))
		handshake, err := peer.ParseHandshake(second)
		if err != nil {
			serverDone <- fmt.Errorf("parse fallback handshake: %w", err)
			return
		}
		if handshake.InfoHash != sess.Torrent.InfoHash {
			serverDone <- fmt.Errorf("fallback info hash = %x, want %x", handshake.InfoHash, sess.Torrent.InfoHash)
			return
		}
		resp := &peer.Handshake{
			Pstr:     "BitTorrent protocol",
			InfoHash: sess.Torrent.InfoHash,
			PeerID:   [20]byte{8, 8, 8},
		}
		if _, err := second.Write(resp.Serialize()); err != nil {
			serverDone <- fmt.Errorf("write fallback response: %w", err)
			return
		}
		msg, err := peer.ParseMessage(second)
		if err != nil {
			serverDone <- fmt.Errorf("read fallback initial message: %w", err)
			return
		}
		if msg == nil || msg.ID != peer.MsgInterested {
			serverDone <- fmt.Errorf("fallback initial message = %#v, want interested", msg)
			return
		}
		serverDone <- nil
	}()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		sess.connectToPeer(tracker.Peer{IP: net.ParseIP("127.0.0.1"), Port: uint16(port)})
	}()

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("plaintext fallback did not complete")
	}
	select {
	case <-clientDone:
	case <-time.After(3 * time.Second):
		t.Fatal("connectToPeer did not exit after fallback peer closed")
	}
}

func TestUnderlyingTCPConnUnwrapsBufferedConnection(t *testing.T) {
	ln, port := listenEncryptionTestPeer(t)
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
		defer serverConn.Close()
	case err := <-acceptErr:
		t.Fatalf("accept: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for accept")
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("dial returned %T, want *net.TCPConn", conn)
	}
	if got := underlyingTCPConn(newBufferedConn(conn)); got != tcpConn {
		t.Fatalf("underlyingTCPConn(newBufferedConn(conn)) = %p, want %p", got, tcpConn)
	}
}

func newEncryptionTestManagedSession(t *testing.T, mgr *TorrentManager, name string) *Session {
	t.Helper()
	sess := newEncryptionTestSession(t, name)
	mgr.AddSession(fmt.Sprintf("%x", sess.Torrent.InfoHash), sess)
	sess.Start()
	return sess
}

func newEncryptionTestSession(t *testing.T, name string) *Session {
	t.Helper()
	infoHash := sha1.Sum([]byte(name))
	tor := &torrent.Torrent{
		Name:        name,
		InfoHash:    infoHash,
		PieceLength: 1,
		PieceHashes: [][20]byte{sha1.Sum([]byte("x"))},
		Files:       []torrent.File{{Length: 1, Path: []string{name}}},
	}
	sess, err := NewSession(tor, nil, [20]byte{1, 2, 3}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	return sess
}

func listenEncryptionTestPeer(t *testing.T) (net.Listener, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portText, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		ln.Close()
		t.Fatalf("split listen addr: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		ln.Close()
		t.Fatalf("parse listen port: %v", err)
	}
	return ln, port
}
