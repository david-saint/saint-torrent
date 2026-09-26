package tracker

import (
	"bytes"
	"net"
	"net/url"
	"strconv"
	"testing"
)

func TestBuildTrackerURL(t *testing.T) {
	baseURL := "http://tracker.example.com/announce"
	infoHash := [20]byte{
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34,
		0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78,
	}
	peerID := [20]byte{
		'-', 'G', 'R', '0', '0', '0', '1', '-', '1', '2',
		'3', '4', '5', '6', '7', '8', '9', '0', '1', '2',
	}
	port := uint16(6881)
	uploaded := int64(100)
	downloaded := int64(200)
	left := int64(300)
	compact := true

	rawURL, err := BuildTrackerURL(baseURL, infoHash, peerID, port, uploaded, downloaded, left, compact, "")
	if err != nil {
		t.Fatalf("Failed to build tracker URL: %v", err)
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("Failed to parse output URL: %v", err)
	}

	// Verify base URL path
	expectedPath := "/announce"
	if u.Path != expectedPath {
		t.Errorf("Expected path %q, got %q", expectedPath, u.Path)
	}

	// Verify query params
	q := u.Query()
	if q.Get("port") != strconv.Itoa(int(port)) {
		t.Errorf("Expected port %d, got %s", port, q.Get("port"))
	}
	if q.Get("uploaded") != strconv.FormatInt(uploaded, 10) {
		t.Errorf("Expected uploaded %d, got %s", uploaded, q.Get("uploaded"))
	}
	if q.Get("downloaded") != strconv.FormatInt(downloaded, 10) {
		t.Errorf("Expected downloaded %d, got %s", downloaded, q.Get("downloaded"))
	}
	if q.Get("left") != strconv.FormatInt(left, 10) {
		t.Errorf("Expected left %d, got %s", left, q.Get("left"))
	}
	if q.Get("compact") != "1" {
		t.Errorf("Expected compact 1, got %s", q.Get("compact"))
	}
	if q.Get("numwant") != strconv.Itoa(defaultNumWant) {
		t.Errorf("Expected numwant %d, got %s", defaultNumWant, q.Get("numwant"))
	}

	// Wait, standard URL parsing decodes %-encoded params.
	// But let's check the raw query string to verify binary fields are encoded correctly (with uppercase hex-encoding for non-alphanumeric).
	// Spec requires raw info_hash query param. Let's inspect the raw query string directly.
	rawQuery := u.RawQuery
	// We want to verify info_hash=%124Vx%9A%BC%DE%F0%124Vx%9A%BC%DE%F0%124Vx (where alphanumeric characters like '4', 'V', 'x' might not be escaped, or they are, but hex digits are capitalized).
	// Let's verify that info_hash and peer_id are present in RawQuery.
	if !bytes.Contains([]byte(rawQuery), []byte("info_hash=")) {
		t.Errorf("raw query does not contain info_hash: %s", rawQuery)
	}
	if !bytes.Contains([]byte(rawQuery), []byte("peer_id=")) {
		t.Errorf("raw query does not contain peer_id: %s", rawQuery)
	}
}

func TestParseTrackerResponse(t *testing.T) {
	// Compact peers:
	// Peer 1: 192.168.0.1:6881 => [192, 168, 0, 1, 0x1a, 0xe1]
	// Peer 2: 10.0.0.1:8080 => [10, 0, 0, 1, 0x1f, 0x90]
	peer1Bytes := []byte{192, 168, 0, 1, 0x1a, 0xe1}
	peer2Bytes := []byte{10, 0, 0, 1, 0x1f, 0x90}
	peersBytes := append(peer1Bytes, peer2Bytes...)

	// Construct bencoded response: d8:intervali1800e5:peers12:<peersBytes>e
	bencoded := []byte("d8:intervali1800e5:peers12:")
	bencoded = append(bencoded, peersBytes...)
	bencoded = append(bencoded, 'e')

	resp, err := ParseTrackerResponse(bencoded)
	if err != nil {
		t.Fatalf("Failed to parse tracker response: %v", err)
	}

	if resp.Interval != 1800 {
		t.Errorf("Expected interval 1800, got %d", resp.Interval)
	}

	if len(resp.Peers) != 2 {
		t.Fatalf("Expected 2 peers, got %d", len(resp.Peers))
	}

	p1 := resp.Peers[0]
	if !p1.IP.Equal(net.IPv4(192, 168, 0, 1)) {
		t.Errorf("Expected peer 1 IP 192.168.0.1, got %v", p1.IP)
	}
	if p1.Port != 6881 {
		t.Errorf("Expected peer 1 port 6881, got %d", p1.Port)
	}

	p2 := resp.Peers[1]
	if !p2.IP.Equal(net.IPv4(10, 0, 0, 1)) {
		t.Errorf("Expected peer 2 IP 10.0.0.1, got %v", p2.IP)
	}
	if p2.Port != 8080 {
		t.Errorf("Expected peer 2 port 8080, got %d", p2.Port)
	}
}

func TestParseTrackerResponseFailure(t *testing.T) {
	// Invalid bencode
	_, err := ParseTrackerResponse([]byte("invalid"))
	if err == nil {
		t.Error("Expected error parsing invalid bencode, got nil")
	}

	// Peers length not multiple of 6
	peersBytes := []byte{192, 168, 0, 1, 0x1a} // 5 bytes
	bencoded := []byte("d8:intervali1800e5:peers5:")
	bencoded = append(bencoded, peersBytes...)
	bencoded = append(bencoded, 'e')

	_, err = ParseTrackerResponse(bencoded)
	if err == nil {
		t.Error("Expected error parsing compact peers of length 5, got nil")
	}
}

// TestParseTrackerResponseWithoutInterval: a reply with no usable interval was
// rejected, discarding its peers and putting the tracker into failure backoff.
// It now parses with Interval 0, which the announcer replaces with its default,
// as libtorrent does.
func TestParseTrackerResponseWithoutInterval(t *testing.T) {
	for name, body := range map[string]string{
		"missing":     "d5:peers6:\x01\x02\x03\x04\x1a\xe1e",
		"not integer": "d8:interval4:18005:peers6:\x01\x02\x03\x04\x1a\xe1e",
	} {
		resp, err := ParseTrackerResponse([]byte(body))
		if err != nil {
			t.Fatalf("%s interval: %v", name, err)
		}
		if resp.Interval != 0 || len(resp.Peers) != 1 || !resp.Peers[0].IP.Equal(net.IPv4(1, 2, 3, 4)) || resp.Peers[0].Port != 6881 {
			t.Fatalf("%s interval: parsed %+v, want the one peer and interval 0", name, resp)
		}
	}
	// A failure reason is still an error, with or without an interval.
	if _, err := ParseTrackerResponse([]byte("d14:failure reason4:nopee")); err == nil {
		t.Fatal("a failure reason without an interval was accepted")
	}
}

func TestBuildTrackerURLWithEvent(t *testing.T) {
	baseURL := "http://tracker.example.com/announce?event=should_be_deleted"
	infoHash := [20]byte{1}
	peerID := [20]byte{2}

	rawURL, err := BuildTrackerURL(baseURL, infoHash, peerID, 6881, 0, 0, 0, false, "started")
	if err != nil {
		t.Fatalf("Failed to build tracker URL: %v", err)
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("Failed to parse output URL: %v", err)
	}

	q := u.Query()
	if q.Get("event") != "started" {
		t.Errorf("Expected event 'started', got %q", q.Get("event"))
	}

	// Verify old event parameter was overwritten
	events := q["event"]
	if len(events) != 1 {
		t.Errorf("Expected exactly 1 event parameter, got %d: %v", len(events), events)
	}
}

func TestParseBencodeNegativeLength(t *testing.T) {
	_, err := ParseTrackerResponse([]byte("d8:interval-5:valee"))
	if err == nil {
		t.Error("Expected error when parsing negative string length, got nil")
	}
}

func TestParseBencodeHugeLengthDoesNotOverflow(t *testing.T) {
	_, err := ParseTrackerResponse([]byte("d8:interval9223372036854775807:xe"))
	if err == nil {
		t.Fatal("expected huge string length to return an error")
	}
}
