// Package peer implements the BitTorrent peer wire protocol.
package peer

import (
	"encoding/binary"
	"net"
	"sync"
)

// Client represents a connection to a BitTorrent peer.
type Client struct {
	Conn     net.Conn
	InfoHash [20]byte
	PeerID   [20]byte
	writeMu  sync.Mutex // protects concurrent writes to Conn
}

// NewClient initializes a new peer wire client.
func NewClient(conn net.Conn, infoHash, peerID [20]byte) *Client {
	return &Client{
		Conn:     conn,
		InfoHash: infoHash,
		PeerID:   peerID,
	}
}

// Handshake performs the BitTorrent protocol handshake.
// It writes our handshake, then reads and parses the peer's handshake response.
func (c *Client) Handshake() (*Handshake, error) {
	reqHandshake := &Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: c.InfoHash,
		PeerID:   c.PeerID,
	}
	reqHandshake.Reserved[5] = 0x10 // Support extension protocol (BEP 10)

	c.writeMu.Lock()
	_, err := c.Conn.Write(reqHandshake.Serialize())
	c.writeMu.Unlock()
	if err != nil {
		return nil, err
	}

	return ParseHandshake(c.Conn)
}

// SendMessage serializes and writes a message to the peer connection in a thread-safe manner.
func (c *Client) SendMessage(msg *Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// SendKeepAlive sends a keep-alive message (zero-length prefix).
func (c *Client) SendKeepAlive() error {
	return c.SendMessage(nil)
}

// SendChoke sends a choke message to the peer.
func (c *Client) SendChoke() error {
	return c.SendMessage(&Message{ID: MsgChoke})
}

// SendUnchoke sends an unchoke message to the peer.
func (c *Client) SendUnchoke() error {
	return c.SendMessage(&Message{ID: MsgUnchoke})
}

// SendInterested sends an interested message to the peer.
func (c *Client) SendInterested() error {
	return c.SendMessage(&Message{ID: MsgInterested})
}

// SendNotInterested sends a not-interested message to the peer.
func (c *Client) SendNotInterested() error {
	return c.SendMessage(&Message{ID: MsgNotInterested})
}

// SendHave sends a have message notifying the peer that we downloaded a piece.
func (c *Client) SendHave(index uint32) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, index)
	return c.SendMessage(&Message{ID: MsgHave, Payload: payload})
}

// SendBitfield sends our bitfield representation of possessed pieces to the peer.
func (c *Client) SendBitfield(bitfield []byte) error {
	return c.SendMessage(&Message{ID: MsgBitfield, Payload: bitfield})
}

// SendRequest sends a request message for a block of a piece.
func (c *Client) SendRequest(index, begin, length uint32) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:12], length)
	return c.SendMessage(&Message{ID: MsgRequest, Payload: payload})
}

// SendPiece sends a piece block message to the peer.
func (c *Client) SendPiece(index, begin uint32, block []byte) error {
	payload := make([]byte, 8+len(block))
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	copy(payload[8:], block)
	return c.SendMessage(&Message{ID: MsgPiece, Payload: payload})
}

// SendCancel sends a cancel message to withdraw a request for a block.
func (c *Client) SendCancel(index, begin, length uint32) error {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:12], length)
	return c.SendMessage(&Message{ID: MsgCancel, Payload: payload})
}

// ReadMessage reads a message from the peer connection.
func (c *Client) ReadMessage() (*Message, error) {
	return ParseMessage(c.Conn)
}
