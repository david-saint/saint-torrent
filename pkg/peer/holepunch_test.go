package peer

import (
	"bytes"
	"net"
	"net/netip"
	"testing"
)

func TestHolepunchMessageRoundTrip(t *testing.T) {
	cases := []HolepunchMessage{
		{Type: HolepunchRendezvous, Addr: netip.MustParseAddrPort("203.0.113.7:6881")},
		{Type: HolepunchConnect, Addr: netip.MustParseAddrPort("[2001:db8::1]:51413")},
		{Type: HolepunchError, Addr: netip.MustParseAddrPort("198.51.100.2:1"), ErrCode: HolepunchErrNoSupport},
	}
	for _, want := range cases {
		data, err := SerializeHolepunchMessage(&want)
		if err != nil {
			t.Fatalf("serialize %+v: %v", want, err)
		}
		got, err := ParseHolepunchMessage(data)
		if err != nil {
			t.Fatalf("parse %x: %v", data, err)
		}
		if *got != want {
			t.Fatalf("round trip = %+v, want %+v", *got, want)
		}
	}
}

// TestHolepunchMessageWireFormat pins the BEP 55 layout byte for byte.
func TestHolepunchMessageWireFormat(t *testing.T) {
	data, err := SerializeHolepunchMessage(&HolepunchMessage{
		Type:    HolepunchError,
		Addr:    netip.MustParseAddrPort("1.2.3.4:258"),
		ErrCode: HolepunchErrNotConnected,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{2, 0, 1, 2, 3, 4, 0x01, 0x02, 0, 0, 0, 2}
	if !bytes.Equal(data, want) {
		t.Fatalf("error message = %x, want %x", data, want)
	}

	data, err = SerializeHolepunchMessage(&HolepunchMessage{
		Type: HolepunchConnect,
		Addr: netip.MustParseAddrPort("[::1]:6881"),
		// err_code is sent only with an error message.
		ErrCode: HolepunchErrNoSelf,
	})
	if err != nil {
		t.Fatal(err)
	}
	want = append([]byte{1, 1}, net.IPv6loopback...)
	want = append(want, 0x1a, 0xe1, 0, 0, 0, 0)
	if !bytes.Equal(data, want) {
		t.Fatalf("connect message = %x, want %x", data, want)
	}
}

// TestHolepunchMessageUnmapsIPv4 checks that an IPv4-mapped address goes out
// as IPv4 and comes back unmapped, so it compares equal to the peer's key.
func TestHolepunchMessageUnmapsIPv4(t *testing.T) {
	mapped := netip.AddrPortFrom(netip.AddrFrom16(netip.MustParseAddr("10.0.0.9").As16()), 7000)
	data, err := SerializeHolepunchMessage(&HolepunchMessage{Type: HolepunchRendezvous, Addr: mapped})
	if err != nil {
		t.Fatal(err)
	}
	if data[1] != holepunchAddrIPv4 || len(data) != 12 {
		t.Fatalf("mapped address encoded as %x, want IPv4", data)
	}

	v6mapped := append([]byte{0, 1}, net.IPv4(10, 0, 0, 9).To16()...)
	v6mapped = append(v6mapped, 0x1b, 0x58, 0, 0, 0, 0)
	got, err := ParseHolepunchMessage(v6mapped)
	if err != nil {
		t.Fatal(err)
	}
	if want := netip.MustParseAddrPort("10.0.0.9:7000"); got.Addr != want {
		t.Fatalf("parsed %v, want %v", got.Addr, want)
	}
}

func TestParseHolepunchMessageRejectsMalformed(t *testing.T) {
	valid4 := []byte{0, 0, 1, 2, 3, 4, 0, 80, 0, 0, 0, 0}
	cases := map[string][]byte{
		"empty":            nil,
		"one byte":         {0},
		"unknown msg_type": append([]byte{3}, valid4[1:]...),
		"unknown addr_type": func() []byte {
			b := append([]byte(nil), valid4...)
			b[1] = 2
			return b
		}(),
		"truncated":         valid4[:len(valid4)-1],
		"trailing data":     append(append([]byte(nil), valid4...), 0),
		"ipv6 short":        append([]byte{0, 1}, make([]byte, 16)...),
		"ipv4 as ipv6 size": append([]byte{0, 0}, make([]byte, 22)...),
	}
	for name, data := range cases {
		if _, err := ParseHolepunchMessage(data); err == nil {
			t.Errorf("%s: parsed %x without error", name, data)
		}
	}
}

func TestSerializeHolepunchMessageRejectsInvalid(t *testing.T) {
	if _, err := SerializeHolepunchMessage(nil); err == nil {
		t.Error("nil message serialized")
	}
	if _, err := SerializeHolepunchMessage(&HolepunchMessage{Type: HolepunchConnect}); err == nil {
		t.Error("message without an address serialized")
	}
	if _, err := SerializeHolepunchMessage(&HolepunchMessage{Type: 7, Addr: netip.MustParseAddrPort("1.2.3.4:5")}); err == nil {
		t.Error("unknown msg_type serialized")
	}
}

// TestSendHolepunchFramesExtendedMessage checks the BEP 10 framing: the
// extended message carries the peer's ut_holepunch ID, then the payload.
func TestSendHolepunchFramesExtendedMessage(t *testing.T) {
	ours, theirs := net.Pipe()
	defer ours.Close()
	defer theirs.Close()
	client := NewClient(ours, [20]byte{}, [20]byte{})
	msg := &HolepunchMessage{Type: HolepunchRendezvous, Addr: netip.MustParseAddrPort("192.0.2.1:6881")}
	go func() { _ = client.SendHolepunch(9, msg) }()

	got, err := NewClient(theirs, [20]byte{}, [20]byte{}).ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != MsgExtended || len(got.Payload) < 1 || got.Payload[0] != 9 {
		t.Fatalf("got message %d payload %x, want extended message 9", got.ID, got.Payload)
	}
	parsed, err := ParseHolepunchMessage(got.Payload[1:])
	if err != nil {
		t.Fatal(err)
	}
	if *parsed != *msg {
		t.Fatalf("sent %+v, received %+v", *msg, *parsed)
	}
}

func FuzzParseHolepunchMessage(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 4, 0, 80, 0, 0, 0, 0})
	f.Add(append(append([]byte{2, 1}, make([]byte, 16)...), 0, 1, 0, 0, 0, 4))
	f.Fuzz(func(t *testing.T, data []byte) {
		msg, err := ParseHolepunchMessage(data)
		if err != nil {
			return
		}
		out, err := SerializeHolepunchMessage(msg)
		if err != nil {
			t.Fatalf("parsed %x but cannot serialize %+v: %v", data, msg, err)
		}
		again, err := ParseHolepunchMessage(out)
		if err != nil || *again != *msg {
			t.Fatalf("re-parse of %x = %+v, %v; want %+v", out, again, err, msg)
		}
	})
}
