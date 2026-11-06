package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
)

// BEP 55 (Holepunch extension). Two peers that both sit behind NAT cannot open
// a connection to each other directly, but a third peer connected to both can
// tell each to connect to the other at the same moment: each side's outgoing
// uTP SYN opens its own NAT to the other, so the other side's SYN gets through.
const (
	ExtNameHolepunch = "ut_holepunch"

	// LocalHolepunchExtID is the ID we advertise for ut_holepunch in our BEP 10
	// handshake.
	LocalHolepunchExtID = 3

	// PEXFlagHolepunch marks a PEX peer that supports ut_holepunch (BEP 11/55).
	PEXFlagHolepunch byte = 1 << 3
	// PEXFlagOutgoing marks a PEX peer the sender connected to, so it is known
	// to accept incoming connections (BEP 11).
	PEXFlagOutgoing byte = 1 << 4
)

// HolepunchMsgType is a ut_holepunch message's msg_type.
type HolepunchMsgType byte

const (
	// HolepunchRendezvous asks the receiver (the relay) to introduce the
	// sender to the endpoint in the message.
	HolepunchRendezvous HolepunchMsgType = 0
	// HolepunchConnect tells the receiver to connect to the endpoint in the
	// message now, which is being told the same about the receiver.
	HolepunchConnect HolepunchMsgType = 1
	// HolepunchError tells the initiator of a rendezvous that the relay could
	// not introduce it to the endpoint in the message.
	HolepunchError HolepunchMsgType = 2
)

// HolepunchErrCode is the err_code of a ut_holepunch error message.
type HolepunchErrCode uint32

const (
	// HolepunchErrNoSuchPeer: the target endpoint is invalid.
	HolepunchErrNoSuchPeer HolepunchErrCode = 1
	// HolepunchErrNotConnected: the relay is not connected to the target.
	HolepunchErrNotConnected HolepunchErrCode = 2
	// HolepunchErrNoSupport: the target does not support ut_holepunch.
	HolepunchErrNoSupport HolepunchErrCode = 3
	// HolepunchErrNoSelf: the target endpoint is the relay itself.
	HolepunchErrNoSelf HolepunchErrCode = 4
)

func (c HolepunchErrCode) String() string {
	switch c {
	case HolepunchErrNoSuchPeer:
		return "no_such_peer"
	case HolepunchErrNotConnected:
		return "not_connected"
	case HolepunchErrNoSupport:
		return "no_support"
	case HolepunchErrNoSelf:
		return "no_self"
	default:
		return fmt.Sprintf("unknown_%d", uint32(c))
	}
}

const (
	holepunchAddrIPv4 = 0
	holepunchAddrIPv6 = 1

	// MaxHolepunchMessageSize is the size of the largest ut_holepunch payload:
	// msg_type, addr_type, a 16-byte IPv6 address, port and err_code.
	MaxHolepunchMessageSize = 1 + 1 + 16 + 2 + 4
)

// HolepunchMessage is one ut_holepunch message. Unlike other BEP 10 messages
// it is binary, not bencoded:
//
//	msg_type  uint8
//	addr_type uint8   (0 = IPv4, 1 = IPv6)
//	addr      4 or 16 bytes
//	port      uint16  big-endian
//	err_code  uint32  big-endian (0 unless msg_type is error)
type HolepunchMessage struct {
	Type    HolepunchMsgType
	Addr    netip.AddrPort
	ErrCode HolepunchErrCode
}

// ParseHolepunchMessage decodes a ut_holepunch payload (the bytes after the
// extended message ID). IPv4-mapped IPv6 addresses are unmapped, so the result
// compares equal to the same peer reached over IPv4.
func ParseHolepunchMessage(data []byte) (*HolepunchMessage, error) {
	if len(data) < 2 {
		return nil, errors.New("holepunch message too short")
	}
	msgType := HolepunchMsgType(data[0])
	if msgType > HolepunchError {
		return nil, fmt.Errorf("unknown holepunch msg_type %d", data[0])
	}
	var addrLen int
	switch data[1] {
	case holepunchAddrIPv4:
		addrLen = 4
	case holepunchAddrIPv6:
		addrLen = 16
	default:
		return nil, fmt.Errorf("unknown holepunch addr_type %d", data[1])
	}
	if want := 2 + addrLen + 2 + 4; len(data) != want {
		return nil, fmt.Errorf("holepunch message length %d, want %d", len(data), want)
	}
	ip, _ := netip.AddrFromSlice(data[2 : 2+addrLen])
	port := binary.BigEndian.Uint16(data[2+addrLen:])
	var errCode HolepunchErrCode
	if msgType == HolepunchError {
		// Only meaningful in an error message; BEP 55 has it 0 otherwise.
		errCode = HolepunchErrCode(binary.BigEndian.Uint32(data[2+addrLen+2:]))
	}
	return &HolepunchMessage{
		Type:    msgType,
		Addr:    netip.AddrPortFrom(ip.Unmap(), port),
		ErrCode: errCode,
	}, nil
}

// SerializeHolepunchMessage encodes msg as a ut_holepunch payload. An IPv4 (or
// IPv4-mapped) address is sent as IPv4.
func SerializeHolepunchMessage(msg *HolepunchMessage) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil holepunch message")
	}
	if msg.Type > HolepunchError {
		return nil, fmt.Errorf("unknown holepunch msg_type %d", msg.Type)
	}
	ip := msg.Addr.Addr().Unmap()
	if !ip.IsValid() {
		return nil, errors.New("holepunch message has no address")
	}
	buf := make([]byte, 0, MaxHolepunchMessageSize)
	buf = append(buf, byte(msg.Type))
	if ip.Is4() {
		a := ip.As4()
		buf = append(buf, holepunchAddrIPv4)
		buf = append(buf, a[:]...)
	} else {
		a := ip.As16()
		buf = append(buf, holepunchAddrIPv6)
		buf = append(buf, a[:]...)
	}
	buf = binary.BigEndian.AppendUint16(buf, msg.Addr.Port())
	var errCode uint32
	if msg.Type == HolepunchError {
		errCode = uint32(msg.ErrCode)
	}
	buf = binary.BigEndian.AppendUint32(buf, errCode)
	return buf, nil
}

// SendHolepunch sends a ut_holepunch message using the peer's advertised
// extension message ID.
func (c *Client) SendHolepunch(extMsgID byte, msg *HolepunchMessage) error {
	payload, err := SerializeHolepunchMessage(msg)
	if err != nil {
		return err
	}
	return c.sendExtendedPayload(extMsgID, payload)
}
