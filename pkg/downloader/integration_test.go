package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/utp"
)

func TestEndToEndDownload(t *testing.T) {
	// 1. Prepare Mock Torrent Data
	// Total data: 64 bytes, 2 pieces of 32 bytes each
	mockData := []byte("hello world! this is a complete integration test for saintTorrent") // exactly 65 bytes, let's pad to 64
	mockData = mockData[:64]
	pieceLength := int64(32)

	p0 := mockData[0:32]
	p1 := mockData[32:64]

	h0 := sha1.Sum(p0)
	h1 := sha1.Sum(p1)

	tor := &torrent.Torrent{
		Name:        "integration_test.txt",
		PieceLength: pieceLength,
		PieceHashes: [][20]byte{h0, h1},
		Files: []torrent.File{
			{Length: 64, Path: []string{"integration_test.txt"}},
		},
	}

	// 2. Start Mock TCP Peer Server
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock TCP peer listener: %v", err)
	}
	defer tcpListener.Close()
	_, peerPortStr, _ := net.SplitHostPort(tcpListener.Addr().String())
	peerPort, _ := strconv.Atoi(peerPortStr)

	go func() {
		conn, err := tcpListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// A. Receive handshake
		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}

		// B. Send handshake response
		peerID := [20]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
		respHs := &peer.Handshake{
			Pstr:     "BitTorrent protocol",
			InfoHash: hs.InfoHash,
			PeerID:   peerID,
		}
		_, _ = conn.Write(respHs.Serialize())

		// C. Send Bitfield (representing we have all pieces: piece 0 and 1)
		// 2 pieces fit in 1 byte, top 2 bits set: 11000000 -> 0xC0
		bfMsg := &peer.Message{
			ID:      peer.MsgBitfield,
			Payload: []byte{0xC0},
		}
		_, _ = conn.Write(bfMsg.Serialize())

		// D. Read loop
		for {
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				break
			}
			if msg == nil {
				continue
			}

			switch msg.ID {
			case peer.MsgInterested:
				// Send unchoke
				unchokeMsg := &peer.Message{ID: peer.MsgUnchoke}
				_, _ = conn.Write(unchokeMsg.Serialize())

			case peer.MsgRequest:
				if len(msg.Payload) == 12 {
					index := binary.BigEndian.Uint32(msg.Payload[0:4])
					begin := binary.BigEndian.Uint32(msg.Payload[4:8])
					length := binary.BigEndian.Uint32(msg.Payload[8:12])

					var blockData []byte
					if index == 0 {
						blockData = p0[begin : begin+length]
					} else if index == 1 {
						blockData = p1[begin : begin+length]
					}

					// Send piece block
					respPayload := make([]byte, 8+len(blockData))
					binary.BigEndian.PutUint32(respPayload[0:4], index)
					binary.BigEndian.PutUint32(respPayload[4:8], begin)
					copy(respPayload[8:], blockData)

					respMsg := &peer.Message{
						ID:      peer.MsgPiece,
						Payload: respPayload,
					}
					_, _ = conn.Write(respMsg.Serialize())
				}
			}
		}
	}()

	// 3. Start Mock HTTP Tracker
	trackerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Respond with bencoded compact peers representation containing mock peer IP/Port
		// Peer IP: 127.0.0.1, Port: peerPort
		ipBytes := net.ParseIP("127.0.0.1").To4()
		portBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(portBytes, uint16(peerPort))

		peerComp := append(ipBytes, portBytes...)

		respDict := map[string]interface{}{
			"interval": int64(1800),
			"peers":    string(peerComp),
		}

		bencoded, err := bencode.Marshal(respDict)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(bencoded)
	}))
	defer trackerServer.Close()

	// Update Torrent with mock tracker URL
	tor.Announce = trackerServer.URL
	tor.Trackers = []string{trackerServer.URL}

	// 4. Initialize Local Temporary Storage
	tempDir, err := os.MkdirTemp("", "sainttorrent_integration_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewMemStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	// 5. Initialize Downloader Session
	clientPeerID := [20]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	sess, err := NewSession(tor, st, clientPeerID, 6882, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// 6. Run Session & Wait for Download to Complete
	sess.Start()
	defer sess.Close()

	// Wait up to 5 seconds for completion
	completed := false
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		if sess.PercentComplete() == 100.0 {
			completed = true
			break
		}
	}

	if !completed {
		t.Fatalf("download did not complete in time, percentage: %f", sess.PercentComplete())
	}

	// 7. Verify downloaded content from the in-memory backend.
	resBytes := make([]byte, len(mockData))
	_, err = st.ReadBlock(0, 0, resBytes[:32])
	if err != nil {
		t.Fatalf("failed to read downloaded piece 0: %v", err)
	}
	_, err = st.ReadBlock(1, 0, resBytes[32:])
	if err != nil {
		t.Fatalf("failed to read downloaded piece 1: %v", err)
	}

	if string(resBytes) != string(mockData) {
		t.Errorf("downloaded data mismatch:\nexpected: %q\ngot: %q", mockData, resBytes)
	}
}

func TestEndToEndDownloadFallsBackToUTP(t *testing.T) {
	mockData := bytesRepeat([]byte("utp download path "), 256)
	pieceLength := int64(len(mockData))
	pieceHash := sha1.Sum(mockData)
	tor := &torrent.Torrent{
		Name:        "utp_integration_test.txt",
		PieceLength: pieceLength,
		PieceHashes: [][20]byte{pieceHash},
		Files: []torrent.File{
			{Length: int64(len(mockData)), Path: []string{"utp_integration_test.txt"}},
		},
	}

	peerSocket := newUTPSocketWithClosedTCPPort(t)
	defer peerSocket.Close()
	peerListener := peerSocket.Listen()
	defer peerListener.Close()
	peerPort := peerSocket.Port()

	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		conn, err := peerListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}
		respHs := &peer.Handshake{
			Pstr:     "BitTorrent protocol",
			InfoHash: hs.InfoHash,
			PeerID:   [20]byte{7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7},
		}
		_, _ = conn.Write(respHs.Serialize())
		_, _ = conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x80}}).Serialize())

		for {
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			switch msg.ID {
			case peer.MsgInterested:
				_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
			case peer.MsgRequest:
				if len(msg.Payload) != 12 {
					continue
				}
				index := binary.BigEndian.Uint32(msg.Payload[0:4])
				begin := binary.BigEndian.Uint32(msg.Payload[4:8])
				length := binary.BigEndian.Uint32(msg.Payload[8:12])
				if index != 0 || int(begin+length) > len(mockData) {
					return
				}
				respPayload := make([]byte, 8+length)
				binary.BigEndian.PutUint32(respPayload[0:4], index)
				binary.BigEndian.PutUint32(respPayload[4:8], begin)
				copy(respPayload[8:], mockData[begin:begin+length])
				_, _ = conn.Write((&peer.Message{ID: peer.MsgPiece, Payload: respPayload}).Serialize())
			}
		}
	}()

	trackerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ipBytes := net.ParseIP("127.0.0.1").To4()
		portBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(portBytes, peerPort)
		respDict := map[string]interface{}{
			"interval": int64(1800),
			"peers":    string(append(ipBytes, portBytes...)),
		}
		bencoded, err := bencode.Marshal(respDict)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(bencoded)
	}))
	defer trackerServer.Close()
	tor.Announce = trackerServer.URL
	tor.Trackers = []string{trackerServer.URL}

	tempDir := t.TempDir()
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{
		Path:   filepath.Join(tor.Files[0].Path...),
		Length: tor.Files[0].Length,
	}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	clientSocket, err := utp.NewSocket(0)
	if err != nil {
		t.Fatalf("failed to create client uTP socket: %v", err)
	}
	defer clientSocket.Close()

	clientPeerID := [20]byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	sess, err := NewSession(tor, st, clientPeerID, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	sess.utpSocket = clientSocket
	sess.Start()
	defer sess.Close()

	deadline := time.Now().Add(5 * time.Second)
	for sess.PercentComplete() != 100.0 {
		if time.Now().After(deadline) {
			t.Fatalf("uTP download did not complete in time, percentage: %f", sess.PercentComplete())
		}
		time.Sleep(50 * time.Millisecond)
	}

	resultPath := filepath.Join(tempDir, "utp_integration_test.txt")
	resBytes, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if string(resBytes) != string(mockData) {
		t.Errorf("downloaded data mismatch:\nexpected: %q\ngot: %q", mockData, resBytes)
	}

	sess.Close()
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("uTP mock peer did not exit")
	}
}

func newUTPSocketWithClosedTCPPort(t *testing.T) *utp.Socket {
	t.Helper()
	for i := 0; i < 10; i++ {
		socket, err := utp.NewSocket(0)
		if err != nil {
			t.Fatalf("failed to create peer uTP socket: %v", err)
		}
		addr := fmt.Sprintf("127.0.0.1:%d", socket.Port())
		conn, err := net.DialTimeout("tcp", addr, 25*time.Millisecond)
		if err != nil {
			return socket
		}
		_ = conn.Close()
		_ = socket.Close()
	}
	t.Fatal("could not find a uTP port without a TCP listener")
	return nil
}

func bytesRepeat(chunk []byte, count int) []byte {
	out := make([]byte, 0, len(chunk)*count)
	for i := 0; i < count; i++ {
		out = append(out, chunk...)
	}
	return out
}

func TestInboundPeerConnection(t *testing.T) {
	// Prepare Torrent Data
	tor := &torrent.Torrent{
		Name:        "inbound_test.txt",
		PieceLength: 32,
		PieceHashes: [][20]byte{sha1.Sum([]byte("12345678901234567890123456789012"))},
		Files: []torrent.File{
			{Length: 32, Path: []string{"inbound_test.txt"}},
		},
	}

	tempDir, err := os.MkdirTemp("", "sainttorrent_inbound_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	files := []storage.FileInfo{
		{Path: filepath.Join(tor.Files[0].Path...), Length: tor.Files[0].Length},
	}
	st, err := storage.NewStorage(tempDir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	clientPeerID := [20]byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	// Bind to port 0 (random port)
	sess, err := NewSession(tor, st, clientPeerID, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.Start()
	defer sess.Close()

	// Wait for listener to bind and update port
	var boundPort uint16
	for i := 0; i < 50; i++ {
		sess.mu.RLock()
		port := sess.Port
		sess.mu.RUnlock()
		if port > 0 {
			boundPort = port
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if boundPort == 0 {
		t.Fatal("session did not bind to a port in time")
	}

	// Connect to the session's listener
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", boundPort))
	if err != nil {
		t.Fatalf("failed to connect to inbound listener: %v", err)
	}
	defer conn.Close()

	// Write handshake
	peerID := [20]byte{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}
	hs := &peer.Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: tor.InfoHash,
		PeerID:   peerID,
	}
	_, err = conn.Write(hs.Serialize())
	if err != nil {
		t.Fatalf("failed to write handshake: %v", err)
	}

	// Parse handshake response
	respHs, err := peer.ParseHandshake(conn)
	if err != nil {
		t.Fatalf("failed to parse handshake response: %v", err)
	}

	if respHs.InfoHash != tor.InfoHash {
		t.Errorf("expected info hash %x, got %x", tor.InfoHash, respHs.InfoHash)
	}

	// Wait briefly and verify that the connection has been recorded as active
	var found bool
	for i := 0; i < 50; i++ {
		active := sess.GetActivePeers()
		if len(active) > 0 {
			found = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !found {
		t.Fatal("peer was not registered as active in the session")
	}
}
