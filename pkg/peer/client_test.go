package peer

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestMockPeerExchange(t *testing.T) {
	infoHash := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	peerID := [20]byte{21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40}
	remotePeerID := [20]byte{41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Channels to synchronize results or errors
	errChan := make(chan error, 2)

	// Mock peer (serverConn side)
	go func() {
		// Set a deadline just in case the test hangs
		serverConn.SetDeadline(time.Now().Add(2 * time.Second))

		// 1. Read handshake from client
		h, err := ParseHandshake(serverConn)
		if err != nil {
			errChan <- err
			return
		}
		if h.InfoHash != infoHash {
			t.Errorf("Server got wrong info hash: %v", h.InfoHash)
		}
		if !SupportsFastExtension(h.Reserved) {
			t.Error("client handshake did not advertise fast extension")
		}

		// 2. Send handshake back
		respHandshake := &Handshake{
			Pstr:     "BitTorrent protocol",
			InfoHash: infoHash,
			PeerID:   remotePeerID,
		}
		_, err = serverConn.Write(respHandshake.Serialize())
		if err != nil {
			errChan <- err
			return
		}

		// 3. Read interested message
		msg, err := ParseMessage(serverConn)
		if err != nil {
			errChan <- err
			return
		}
		if msg == nil || msg.ID != MsgInterested {
			t.Errorf("Server expected MsgInterested, got %v", msg)
		}

		// 4. Send unchoke message
		unchoke := &Message{ID: MsgUnchoke}
		_, err = serverConn.Write(unchoke.Serialize())
		if err != nil {
			errChan <- err
			return
		}

		// 5. Read request message
		msg, err = ParseMessage(serverConn)
		if err != nil {
			errChan <- err
			return
		}
		if msg == nil || msg.ID != MsgRequest {
			t.Errorf("Server expected MsgRequest, got %v", msg)
		}

		// Parse request fields
		index := binary.BigEndian.Uint32(msg.Payload[0:4])
		begin := binary.BigEndian.Uint32(msg.Payload[4:8])
		length := binary.BigEndian.Uint32(msg.Payload[8:12])
		if index != 1 || begin != 0 || length != 10 {
			t.Errorf("Server got unexpected request: index=%d, begin=%d, length=%d", index, begin, length)
		}

		// 6. Send piece message
		// Piece payload: index (4 bytes), begin (4 bytes), block (X bytes)
		piecePayload := make([]byte, 8+length)
		binary.BigEndian.PutUint32(piecePayload[0:4], index)
		binary.BigEndian.PutUint32(piecePayload[4:8], begin)
		copy(piecePayload[8:], []byte("abcdefghij"))

		pieceMsg := &Message{ID: MsgPiece, Payload: piecePayload}
		_, err = serverConn.Write(pieceMsg.Serialize())
		if err != nil {
			errChan <- err
			return
		}

		errChan <- nil
	}()

	// Client side
	go func() {
		clientConn.SetDeadline(time.Now().Add(2 * time.Second))

		client := NewClient(clientConn, infoHash, peerID)

		// 1. Handshake
		serverHandshake, err := client.Handshake()
		if err != nil {
			errChan <- err
			return
		}
		if serverHandshake.PeerID != remotePeerID {
			t.Errorf("Client got wrong peer ID: %v", serverHandshake.PeerID)
		}

		// 2. Send Interested
		err = client.SendInterested()
		if err != nil {
			errChan <- err
			return
		}

		// 3. Receive Unchoke
		msg, err := client.ReadMessage()
		if err != nil {
			errChan <- err
			return
		}
		if msg == nil || msg.ID != MsgUnchoke {
			t.Errorf("Client expected MsgUnchoke, got %v", msg)
		}

		// 4. Send Request
		err = client.WriteRequest(1, 0, 10)
		if err == nil {
			err = client.Flush()
		}
		if err != nil {
			errChan <- err
			return
		}

		// 5. Receive Piece
		msg, err = client.ReadMessage()
		if err != nil {
			errChan <- err
			return
		}
		if msg == nil || msg.ID != MsgPiece {
			t.Errorf("Client expected MsgPiece, got %v", msg)
		}

		// Verify piece
		pieceIndex := binary.BigEndian.Uint32(msg.Payload[0:4])
		begin := binary.BigEndian.Uint32(msg.Payload[4:8])
		block := msg.Payload[8:]

		if pieceIndex != 1 {
			t.Errorf("Client expected piece index 1, got %d", pieceIndex)
		}
		if begin != 0 {
			t.Errorf("Client expected begin 0, got %d", begin)
		}
		if !bytes.Equal(block, []byte("abcdefghij")) {
			t.Errorf("Client expected block %q, got %q", "abcdefghij", block)
		}

		errChan <- nil
	}()

	// Wait for both sides to finish
	for i := 0; i < 2; i++ {
		err := <-errChan
		if err != nil {
			t.Fatalf("Error during exchange: %v", err)
		}
	}
}

func TestFastMessageSendHelpers(t *testing.T) {
	infoHash := [20]byte{1}
	peerID := [20]byte{2}
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	client := NewClient(clientConn, infoHash, peerID)

	errCh := make(chan error, 1)
	go func() {
		if err := client.SendHaveAll(); err != nil {
			errCh <- err
			return
		}
		if err := client.SendHaveNone(); err != nil {
			errCh <- err
			return
		}
		if err := client.SendSuggestPiece(7); err != nil {
			errCh <- err
			return
		}
		if err := client.SendRejectRequest(1, 2, 3); err != nil {
			errCh <- err
			return
		}
		if err := client.SendAllowedFast(9); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	serverConn.SetDeadline(time.Now().Add(2 * time.Second))
	expected := []struct {
		id      MessageID
		payload []byte
	}{
		{id: MsgHaveAll},
		{id: MsgHaveNone},
		{id: MsgSuggestPiece, payload: []byte{0, 0, 0, 7}},
		{id: MsgRejectRequest, payload: []byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3}},
		{id: MsgAllowedFast, payload: []byte{0, 0, 0, 9}},
	}
	for _, want := range expected {
		msg, err := ParseMessage(serverConn)
		if err != nil {
			t.Fatalf("ParseMessage failed: %v", err)
		}
		if msg == nil || msg.ID != want.id || !bytes.Equal(msg.Payload, want.payload) {
			t.Fatalf("got message %#v, want id=%d payload=%v", msg, want.id, want.payload)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("send helper failed: %v", err)
	}
}
