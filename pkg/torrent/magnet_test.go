package torrent

import (
	"encoding/base32"
	"encoding/hex"
	"strings"
	"testing"
)

// A known 20-byte info hash used across test cases.
var testHashBytes = [20]byte{
	0xd0, 0xd1, 0x4c, 0x92, 0x6e, 0x6e, 0x99, 0x76,
	0x1a, 0x2f, 0xdc, 0xea, 0x2f, 0x72, 0xc1, 0x55,
	0xe3, 0x25, 0x6b, 0x48,
}

// testHashHex is the 40-char lowercase hex encoding of testHashBytes.
var testHashHex = strings.ToLower(hex.EncodeToString(testHashBytes[:]))

// testHashBase32 is the 32-char base32 (no padding) encoding of testHashBytes.
var testHashBase32 = base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(testHashBytes[:])

func TestParseMagnet_ValidHex(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + testHashHex + "&dn=Test+Torrent"
	ml, err := ParseMagnet(uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ml.InfoHash != testHashBytes {
		t.Errorf("info hash mismatch:\n  got  %x\n  want %x", ml.InfoHash, testHashBytes)
	}
	if ml.Name != "Test Torrent" {
		t.Errorf("name = %q, want %q", ml.Name, "Test Torrent")
	}
}

func TestParseMagnet_ValidBase32(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + testHashBase32 + "&dn=Base32+Test"
	ml, err := ParseMagnet(uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ml.InfoHash != testHashBytes {
		t.Errorf("info hash mismatch:\n  got  %x\n  want %x", ml.InfoHash, testHashBytes)
	}
	if ml.Name != "Base32 Test" {
		t.Errorf("name = %q, want %q", ml.Name, "Base32 Test")
	}
}

func TestParseMagnet_MultipleTrackers(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + testHashHex +
		"&tr=udp%3A%2F%2Ftracker.one.org%3A1337" +
		"&tr=http%3A%2F%2Ftracker.two.org%2Fannounce" +
		"&tr=udp%3A%2F%2Ftracker.three.org%3A6969"
	ml, err := ParseMagnet(uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{
		"udp://tracker.one.org:1337",
		"http://tracker.two.org/announce",
		"udp://tracker.three.org:6969",
	}
	if len(ml.Trackers) != len(want) {
		t.Fatalf("trackers count = %d, want %d", len(ml.Trackers), len(want))
	}
	for i, got := range ml.Trackers {
		if got != want[i] {
			t.Errorf("tracker[%d] = %q, want %q", i, got, want[i])
		}
	}
}

func TestParseMagnet_MissingXT(t *testing.T) {
	uri := "magnet:?dn=No+Hash"
	_, err := ParseMagnet(uri)
	if err == nil {
		t.Fatal("expected error for missing xt, got nil")
	}
	if !strings.Contains(err.Error(), "missing required xt") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseMagnet_InvalidHashLength(t *testing.T) {
	// 30-char string: neither 40 hex nor 32 base32
	uri := "magnet:?xt=urn:btih:abcdef1234567890abcdef12345678"
	_, err := ParseMagnet(uri)
	if err == nil {
		t.Fatal("expected error for invalid hash length, got nil")
	}
	if !strings.Contains(err.Error(), "info hash must be") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseMagnet_URLEncodedTrackers(t *testing.T) {
	// Tracker URL with special characters that get percent-encoded in the magnet URI.
	tracker := "http://tracker.example.com:8080/announce?passkey=abc123&event=started"
	uri := "magnet:?xt=urn:btih:" + testHashHex +
		"&tr=http%3A%2F%2Ftracker.example.com%3A8080%2Fannounce%3Fpasskey%3Dabc123%26event%3Dstarted"
	ml, err := ParseMagnet(uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ml.Trackers) != 1 {
		t.Fatalf("trackers count = %d, want 1", len(ml.Trackers))
	}
	if ml.Trackers[0] != tracker {
		t.Errorf("tracker = %q, want %q", ml.Trackers[0], tracker)
	}
}

func TestParseMagnet_InvalidPrefix(t *testing.T) {
	_, err := ParseMagnet("http://example.com")
	if err == nil {
		t.Fatal("expected error for invalid prefix, got nil")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseMagnet_InvalidHexChars(t *testing.T) {
	// 40 chars but not valid hex (contains 'z')
	uri := "magnet:?xt=urn:btih:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	_, err := ParseMagnet(uri)
	if err == nil {
		t.Fatal("expected error for invalid hex chars, got nil")
	}
	if !strings.Contains(err.Error(), "invalid hex") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseMagnet_NoTrackers(t *testing.T) {
	uri := "magnet:?xt=urn:btih:" + testHashHex
	ml, err := ParseMagnet(uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ml.Trackers) != 0 {
		t.Errorf("trackers = %v, want empty", ml.Trackers)
	}
	if ml.Name != "" {
		t.Errorf("name = %q, want empty", ml.Name)
	}
}

func TestParseMagnet_CaseInsensitiveBase32(t *testing.T) {
	// base32 should work with lowercase input too
	uri := "magnet:?xt=urn:btih:" + strings.ToLower(testHashBase32)
	ml, err := ParseMagnet(uri)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ml.InfoHash != testHashBytes {
		t.Errorf("info hash mismatch:\n  got  %x\n  want %x", ml.InfoHash, testHashBytes)
	}
}
