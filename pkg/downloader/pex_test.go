package downloader

import (
	"crypto/sha1"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestExtensionHandshakeMapAdvertisesPEXUnlessPrivate(t *testing.T) {
	sess := &Session{Torrent: &torrent.Torrent{}}
	sess.mu.Lock()
	extensions := sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()

	if extensions[peer.ExtNameMetadata] != peer.LocalMetadataExtID {
		t.Fatalf("ut_metadata = %d, want %d", extensions[peer.ExtNameMetadata], peer.LocalMetadataExtID)
	}
	if extensions[peer.ExtNamePEX] != peer.LocalPEXExtID {
		t.Fatalf("ut_pex = %d, want %d", extensions[peer.ExtNamePEX], peer.LocalPEXExtID)
	}

	sess.Torrent.Private = true
	sess.mu.Lock()
	extensions = sess.extensionHandshakeMapLocked()
	sess.mu.Unlock()
	if _, ok := extensions[peer.ExtNamePEX]; ok {
		t.Fatal("private torrent advertised ut_pex")
	}
	if extensions[peer.ExtNameMetadata] != peer.LocalMetadataExtID {
		t.Fatalf("private torrent did not keep ut_metadata")
	}
}

func TestBuildPEXDeltaAddsAndDrops(t *testing.T) {
	sess := &Session{
		Torrent: &torrent.Torrent{},
		Peers: map[string]*PeerState{
			net.JoinHostPort("127.0.0.1", "1001"): {
				IP:       "127.0.0.1",
				Port:     1001,
				Active:   true,
				Dialable: true,
			},
			net.JoinHostPort("127.0.0.1", "1002"): {
				IP:       "127.0.0.1",
				Port:     1002,
				Active:   true,
				Dialable: true,
			},
			net.JoinHostPort("127.0.0.1", "1004"): {
				IP:       "127.0.0.1",
				Port:     1004,
				Active:   true,
				Dialable: false,
			},
		},
	}
	advertised := map[string]struct{}{
		net.JoinHostPort("127.0.0.1", "1001"): {},
		net.JoinHostPort("127.0.0.1", "1003"): {},
	}

	msg, next, ok := sess.buildPEXDelta("", advertised)
	if !ok {
		t.Fatal("expected PEX delta")
	}
	if len(msg.Added) != 1 || msg.Added[0].Port != 1002 {
		t.Fatalf("added = %+v, want only port 1002", msg.Added)
	}
	if len(msg.Dropped) != 1 || msg.Dropped[0].Port != 1003 {
		t.Fatalf("dropped = %+v, want only port 1003", msg.Dropped)
	}
	if _, ok := next[net.JoinHostPort("127.0.0.1", "1001")]; !ok {
		t.Fatal("next advertised set lost unchanged peer")
	}
	if _, ok := next[net.JoinHostPort("127.0.0.1", "1002")]; !ok {
		t.Fatal("next advertised set did not include added peer")
	}
	if _, ok := next[net.JoinHostPort("127.0.0.1", "1003")]; ok {
		t.Fatal("next advertised set retained dropped peer")
	}
}

func TestBuildPEXDeltaHonorsPrivateTorrent(t *testing.T) {
	sess := &Session{
		Torrent: &torrent.Torrent{Private: true},
		Peers: map[string]*PeerState{
			net.JoinHostPort("127.0.0.1", "1001"): {
				IP:       "127.0.0.1",
				Port:     1001,
				Active:   true,
				Dialable: true,
			},
		},
	}
	advertised := map[string]struct{}{net.JoinHostPort("127.0.0.1", "1001"): {}}
	if msg, next, ok := sess.buildPEXDelta("", advertised); ok || msg != nil || len(next) != 1 {
		t.Fatal("private torrent produced a PEX delta")
	}
}

func TestPEXDiscoversAndConnectsThirdPeer(t *testing.T) {
	tor := &torrent.Torrent{
		Name:        "pex.txt",
		InfoHash:    sha1.Sum([]byte("pex-integration")),
		PieceLength: 32,
		PieceHashes: [][20]byte{sha1.Sum([]byte("01234567890123456789012345678901"))},
		Files:       []torrent.File{{Length: 32, Path: []string{"pex.txt"}}},
	}
	tempDir := t.TempDir()
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{Path: filepath.Join(tor.Files[0].Path...), Length: 32}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{1}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	sess.Start()
	defer sess.Close()

	thirdConnected := make(chan struct{}, 1)
	thirdListener, thirdPort := startPEXHandshakePeer(t, thirdConnected, false, nil)
	defer thirdListener.Close()
	thirdAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(thirdPort))

	pexListener, pexPort := startPEXHandshakePeer(t, nil, true, func(conn net.Conn) {
		hsPayload, err := peer.SerializeExtensionHandshakeWithExtensions(map[string]int{
			peer.ExtNameMetadata: peer.LocalMetadataExtID,
			peer.ExtNamePEX:      7,
		}, 0)
		if err != nil {
			t.Errorf("failed to serialize extension handshake: %v", err)
			return
		}
		writeExtendedTestMessage(t, conn, peer.ExtHandshake, hsPayload)

		pexPayload, err := peer.SerializePEXMessage(&peer.PEXMessage{
			Added: []peer.PEXPeer{{IP: net.ParseIP("127.0.0.1"), Port: uint16(thirdPort)}},
		})
		if err != nil {
			t.Errorf("failed to serialize PEX: %v", err)
			return
		}
		writeExtendedTestMessage(t, conn, peer.LocalPEXExtID, pexPayload)
	})
	defer pexListener.Close()

	sess.AddPeerFromDiscovery(net.JoinHostPort("127.0.0.1", strconv.Itoa(pexPort)))

	select {
	case <-thirdConnected:
	case <-time.After(3 * time.Second):
		t.Fatalf("third peer %s was not dialed after PEX", thirdAddr)
	}

	sess.mu.RLock()
	ps, ok := sess.Peers[thirdAddr]
	sess.mu.RUnlock()
	if !ok {
		t.Fatalf("third peer %s was not added to known peers", thirdAddr)
	}
	if !ps.Dialable {
		t.Fatalf("third peer %s was not marked dialable", thirdAddr)
	}
}

func startPEXHandshakePeer(t *testing.T, connected chan<- struct{}, supportsExtensions bool, afterHandshake func(net.Conn)) (net.Listener, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to parse listener addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("failed to parse listener port: %v", err)
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}
		resp := &peer.Handshake{
			Pstr:     "BitTorrent protocol",
			InfoHash: hs.InfoHash,
			PeerID:   [20]byte{9},
		}
		if supportsExtensions {
			resp.Reserved[5] = 0x10
		}
		if _, err := conn.Write(resp.Serialize()); err != nil {
			return
		}
		if connected != nil {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
		if afterHandshake != nil {
			afterHandshake(conn)
		}
		<-time.After(250 * time.Millisecond)
	}()

	return ln, port
}

func writeExtendedTestMessage(t *testing.T, conn net.Conn, extID byte, payload []byte) {
	t.Helper()
	msgPayload := make([]byte, 1+len(payload))
	msgPayload[0] = extID
	copy(msgPayload[1:], payload)
	if _, err := conn.Write((&peer.Message{ID: peer.MsgExtended, Payload: msgPayload}).Serialize()); err != nil {
		t.Errorf("failed to write extended message: %v", err)
	}
}
