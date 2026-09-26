package tracker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"sainttorrent/pkg/torrent"
)

func bencodeString(s string) string { return strconv.Itoa(len(s)) + ":" + s }

func TestParseTrackerResponseCapsPeers(t *testing.T) {
	// A hostile tracker ignores numwant and sends ~5000 IPv4 + 100 IPv6 peers.
	var v4, v6 bytes.Buffer
	for i := 0; i < 5000; i++ {
		v4.Write([]byte{10, byte(i >> 16), byte(i >> 8), byte(i), 0x1a, 0xe1})
	}
	for i := 0; i < 100; i++ {
		v6.Write(net.ParseIP(fmt.Sprintf("2001:db8::%x", i+1)))
		v6.Write([]byte{0x1a, 0xe1})
	}
	body := "d8:intervali1800e5:peers" + bencodeString(v4.String()) + "6:peers6" + bencodeString(v6.String()) + "e"
	resp, err := ParseTrackerResponse([]byte(body))
	if err != nil {
		t.Fatalf("ParseTrackerResponse: %v", err)
	}
	if len(resp.Peers) != MaxResponsePeers {
		t.Fatalf("parsed %d peers, want cap %d", len(resp.Peers), MaxResponsePeers)
	}
	// Each IP is its own small slice, not a window onto the response body.
	for _, p := range resp.Peers[:3] {
		if cap(p.IP) != net.IPv4len {
			t.Fatalf("peer IP cap = %d, want %d (detached from the body)", cap(p.IP), net.IPv4len)
		}
	}
	if got, want := peerString(resp.Peers[1]), "10.0.0.1:6881"; got != want {
		t.Fatalf("peer[1] = %s, want %s", got, want)
	}

	// With room left under the cap, IPv6 peers are kept after the IPv4 ones.
	body = "d8:intervali1800e5:peers" + bencodeString(string(v4.Bytes()[:6*10])) + "6:peers6" + bencodeString(v6.String()) + "e"
	resp, err = ParseTrackerResponse([]byte(body))
	if err != nil {
		t.Fatalf("ParseTrackerResponse: %v", err)
	}
	if len(resp.Peers) != 110 {
		t.Fatalf("parsed %d peers, want 110", len(resp.Peers))
	}
	if got := resp.Peers[10].IP.String(); got != "2001:db8::1" {
		t.Fatalf("first IPv6 peer = %s", got)
	}
	if cap(resp.Peers[10].IP) != net.IPv6len {
		t.Fatalf("IPv6 peer IP cap = %d", cap(resp.Peers[10].IP))
	}
}

func peerString(p Peer) string {
	return net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
}

func TestParseTrackerResponseClampsIntegers(t *testing.T) {
	cases := []struct {
		raw  string
		want int
	}{
		{"1800", 1800},
		{"9223372037", math.MaxInt32}, // * time.Second would overflow Duration
		{"4294967297", math.MaxInt32}, // 2^32+1 truncated to 1 on 386 before
		{"-5", 0},
	}
	for _, c := range cases {
		body := "d8:intervali" + c.raw + "e12:min intervali" + c.raw + "e8:completei" + c.raw + "ee"
		resp, err := ParseTrackerResponse([]byte(body))
		if err != nil {
			t.Fatalf("interval %s: %v", c.raw, err)
		}
		if resp.Interval != c.want || resp.MinInterval != c.want || resp.Complete != c.want {
			t.Fatalf("interval %s parsed as %d/%d/%d, want %d", c.raw, resp.Interval, resp.MinInterval, resp.Complete, c.want)
		}
	}
}

func TestTrackerMessagesAreBoundedValidUTF8(t *testing.T) {
	huge := strings.Repeat("A", 1<<20)
	_, err := ParseTrackerResponse([]byte("d14:failure reason" + bencodeString(huge) + "e"))
	if err == nil {
		t.Fatal("failure reason did not produce an error")
	}
	if len(err.Error()) > len("tracker error: ")+maxTrackerMessage {
		t.Fatalf("failure reason error is %d bytes", len(err.Error()))
	}

	// Invalid UTF-8, and a multi-byte rune straddling the cut.
	bad := "\xff\xfe" + strings.Repeat("é", 200)
	resp, err := ParseTrackerResponse([]byte("d8:intervali60e15:warning message" + bencodeString(bad) + "e"))
	if err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(resp.Warning) || len(resp.Warning) > maxTrackerMessage {
		t.Fatalf("warning = %q (%d bytes), want bounded valid UTF-8", resp.Warning, len(resp.Warning))
	}
	_, err = ParseScrapeResponse([]byte("d14:failure reason" + bencodeString(huge) + "e"))
	if err == nil || len(err.Error()) > len("tracker error: ")+maxTrackerMessage {
		t.Fatalf("scrape failure reason not bounded: %d bytes", len(err.Error()))
	}
}

func TestUDPErrorTextIsBounded(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	go func() {
		buf := make([]byte, 2048)
		n, addr, err := pc.ReadFrom(buf)
		if err != nil || n < 16 {
			return
		}
		resp := make([]byte, 8, 4000)
		binary.BigEndian.PutUint32(resp[0:4], actionError)
		copy(resp[4:8], buf[12:16])
		resp = append(resp, bytes.Repeat([]byte{0xff}, 3900)...)
		_, _ = pc.WriteTo(resp, addr)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", [20]byte{}, [20]byte{}, 6881, 0, 0, 0, "")
	if err == nil {
		t.Fatal("expected tracker error")
	}
	msg := err.Error()
	if !utf8.ValidString(msg) || len(msg) > 64+maxTrackerMessage {
		t.Fatalf("UDP error text not bounded valid UTF-8: %d bytes", len(msg))
	}
}

func TestParseUDPAnnounceResponseIPv6(t *testing.T) {
	data := make([]byte, 20, 20+18)
	binary.BigEndian.PutUint32(data[0:4], actionAnnounce)
	binary.BigEndian.PutUint32(data[8:12], 1800)
	data = append(data, net.ParseIP("2001:db8:0:1:2:3:4:5")...)
	data = binary.BigEndian.AppendUint16(data, 6881)

	resp, err := parseUDPAnnounceResponse(data, true)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(resp.Peers) != 1 || peerString(resp.Peers[0]) != "[2001:db8:0:1:2:3:4:5]:6881" {
		t.Fatalf("peers = %v, want [2001:db8:0:1:2:3:4:5]:6881", resp.Peers)
	}
	// Read as IPv4 the same bytes become three bogus endpoints; the family
	// must come from the socket, not the length.
	if resp, err := parseUDPAnnounceResponse(data, false); err != nil || len(resp.Peers) != 3 {
		t.Fatalf("IPv4 parse of an 18-byte entry: %v, %d peers", err, len(resp.Peers))
	}
}

func TestUDPAnnounceOverIPv6UsesEighteenByteEntries(t *testing.T) {
	pc, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer pc.Close()
	peer := net.ParseIP("2001:db8::7")
	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 16 {
				continue
			}
			txn := buf[12:16]
			switch binary.BigEndian.Uint32(buf[8:12]) {
			case actionConnect:
				resp := make([]byte, 16)
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				copy(resp[4:8], txn)
				_, _ = pc.WriteTo(resp, addr)
			case actionAnnounce:
				resp := make([]byte, 20)
				binary.BigEndian.PutUint32(resp[0:4], actionAnnounce)
				copy(resp[4:8], txn)
				binary.BigEndian.PutUint32(resp[8:12], 1800)
				resp = append(resp, peer...)
				resp = binary.BigEndian.AppendUint16(resp, 51413)
				_, _ = pc.WriteTo(resp, addr)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", [20]byte{}, [20]byte{}, 6881, 0, 0, 0, "")
	if err != nil {
		t.Fatalf("UDPAnnounce over IPv6: %v", err)
	}
	if len(resp.Peers) != 1 || peerString(resp.Peers[0]) != "[2001:db8::7]:51413" {
		t.Fatalf("peers = %v", resp.Peers)
	}
}

func TestUDPTrackerDestinationPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := UDPAnnounce(ctx, "udp://169.254.169.254:6969/announce", [20]byte{}, [20]byte{}, 6881, 0, 0, 0, "")
	if !errors.Is(err, ErrDestinationRefused) {
		t.Fatalf("link-local UDP tracker: err = %v, want ErrDestinationRefused", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("refusal should happen before any packet is sent")
	}
	if _, err := UDPScrape(ctx, "udp://224.0.0.1:6969/announce", [20]byte{}); !errors.Is(err, ErrDestinationRefused) {
		t.Fatalf("multicast UDP scrape: err = %v, want ErrDestinationRefused", err)
	}
}

func TestScrapeDispatchesOnParsedScheme(t *testing.T) {
	var hash [20]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("d5:filesd20:" + string(hash[:]) + "d8:completei3eeee"))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	upper := "HTTP://" + strings.TrimPrefix(srv.URL, "http://") + "/announce"
	stats, err := Scrape(ctx, upper, hash)
	if err != nil {
		t.Fatalf("Scrape(%q): %v", upper, err)
	}
	if stats[hash].Complete != 3 {
		t.Fatalf("stats = %+v", stats[hash])
	}
	for _, bad := range []string{"httpx://" + strings.TrimPrefix(srv.URL, "http://") + "/announce", "udpfoo://t.io:1/announce"} {
		if _, err := Scrape(ctx, bad, hash); err == nil || !strings.Contains(err.Error(), "unsupported tracker scheme") {
			t.Fatalf("Scrape(%q) err = %v, want unsupported scheme", bad, err)
		}
	}
}

// padURL pads prefix with 'a' to exactly n bytes.
func padURL(prefix string, n int) string {
	return prefix + strings.Repeat("a", n-len(prefix))
}

// TestParseAnnounceURLCapsLength (CVE-2008-4434 family): the session's
// tracker list runs every URL through ParseAnnounceURL, trackers restored
// from saved state or added later included, so an over-long one is dropped
// there instead of being sent with every announce.
func TestParseAnnounceURLCapsLength(t *testing.T) {
	for _, prefix := range []string{"http://tracker.example/announce?passkey=", "udp://tracker.example:6969/announce?"} {
		if u, err := ParseAnnounceURL(padURL(prefix, MaxAnnounceURLLength)); err != nil || u.Hostname() != "tracker.example" {
			t.Fatalf("ParseAnnounceURL(%d-byte %s URL) = %v, want success", MaxAnnounceURLLength, prefix[:3], err)
		}
		if _, err := ParseAnnounceURL(padURL(prefix, MaxAnnounceURLLength+1)); !errors.Is(err, ErrURLTooLong) {
			t.Fatalf("ParseAnnounceURL(%d-byte %s URL) = %v, want ErrURLTooLong", MaxAnnounceURLLength+1, prefix[:3], err)
		}
	}
	if _, err := Scrape(context.Background(), padURL("http://tracker.example/announce?k=", 1<<20)); !errors.Is(err, ErrURLTooLong) {
		t.Fatalf("Scrape(1 MiB URL) = %v, want ErrURLTooLong", err)
	}

	// The limit is on the configured URL, not on the announce request built
	// from it, which adds the info-hash, peer ID and counters.
	base := padURL("http://tracker.example/announce?passkey=", MaxAnnounceURLLength)
	reqURL, err := BuildTrackerURL(base, [20]byte{0xff}, [20]byte{0xff}, 6881, 1<<40, 1<<40, 1<<40, true, "started")
	if err != nil {
		t.Fatal(err)
	}
	if len(reqURL) <= MaxAnnounceURLLength {
		t.Fatalf("announce request is %d bytes, want it past the %d-byte limit for this test", len(reqURL), MaxAnnounceURLLength)
	}
	if _, err := NewRequest(context.Background(), PurposeAnnounce, reqURL); err != nil {
		t.Fatalf("NewRequest(%d-byte announce request) = %v, want success", len(reqURL), err)
	}
}

// TestNewRequestCapsWebseedURLLength: a webseed request URL (the url-list
// entry plus the file's path) longer than MaxWebseedURLLength is refused
// before anything is sent.
func TestNewRequestCapsWebseedURLLength(t *testing.T) {
	if _, err := NewRequest(context.Background(), PurposeWebseed, padURL("https://cdn.example/files/", MaxWebseedURLLength)); err != nil {
		t.Fatalf("NewRequest(%d-byte webseed URL) = %v, want success", MaxWebseedURLLength, err)
	}
	for _, n := range []int{MaxWebseedURLLength + 1, 1 << 20} {
		if _, err := NewRequest(context.Background(), PurposeWebseed, padURL("https://cdn.example/files/", n)); !errors.Is(err, ErrURLTooLong) {
			t.Fatalf("NewRequest(%d-byte webseed URL) = %v, want ErrURLTooLong", n, err)
		}
	}

	// The longest url-list entry a torrent may carry still reaches a file
	// whose path is 1,320 bytes of CJK text, 3,960 once escaped: the cap is
	// on the request, so it must leave room for the path beyond the entry.
	entry := padURL("https://cdn.example/", torrent.MaxWebSeedURLLength-1) + "/"
	path := strings.Repeat(url.PathEscape("日本語の長いファイル名"), 40)
	if _, err := NewRequest(context.Background(), PurposeWebseed, entry+path); err != nil {
		t.Fatalf("NewRequest(%d-byte entry + %d-byte escaped path) = %v, want success", len(entry), len(path), err)
	}
}

func TestHTTPScrapeCapsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("d5:filesd"))
		_, _ = w.Write(bytes.Repeat([]byte("0:le"), (maxScrapeResponse/4)+1))
		_, _ = w.Write([]byte("ee"))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := HTTPScrape(ctx, srv.URL+"/announce", [20]byte{}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized scrape body: err = %v", err)
	}
}
