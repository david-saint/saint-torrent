package torrent

import (
	"crypto/sha1"
	"encoding/hex"
	"math"
	"testing"

	"sainttorrent/pkg/bencode"
)

func FuzzParseTorrent(f *testing.F) {
	addTorrentSeed(f, map[string]interface{}{
		"announce": "http://tracker.example.com/announce",
		"info": map[string]interface{}{
			"name":         "single.txt",
			"piece length": int64(16),
			"pieces":       string(make([]byte, 20)),
			"length":       int64(16),
		},
	})
	addTorrentSeed(f, map[string]interface{}{
		"announce-list": []interface{}{
			[]interface{}{"udp://tracker.one.example:1337"},
			[]interface{}{"http://tracker.two.example/announce"},
		},
		"info": map[string]interface{}{
			"name":         "../multi",
			"piece length": int64(2),
			"pieces":       string(make([]byte, 40)),
			"files": []interface{}{
				map[string]interface{}{"length": int64(1), "path": []interface{}{"subdir", "one.bin"}},
				map[string]interface{}{"length": int64(2), "path": []interface{}{"..", "two.bin"}},
			},
			"private": int64(1),
		},
	})
	for _, seed := range [][]byte{
		{},
		[]byte("not bencode"),
		[]byte("de"),
		[]byte("d4:infode"),
		[]byte("d4:infod4:name4:bad12:piece lengthi0e6:pieces20:aaaaaaaaaaaaaaaaaaaa6:lengthi1eee"),
		[]byte("d4:infod4:name4:bad12:piece lengthi1e6:pieces5:short6:lengthi1eee"),
		[]byte("d4:infod4:name4:bad12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaa6:lengthi-1eee"),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		tor, err := Parse(data)
		if err != nil {
			return
		}
		if tor.PieceLength <= 0 {
			t.Fatalf("accepted non-positive piece length: %d", tor.PieceLength)
		}
		if len(tor.PieceHashes) == 0 {
			t.Fatalf("accepted torrent without piece hashes")
		}
		if len(tor.InfoBytes) == 0 {
			t.Fatalf("accepted torrent without raw info bytes")
		}
		if got := sha1.Sum(tor.InfoBytes); got != tor.InfoHash {
			t.Fatalf("info hash mismatch: got %x from info bytes, torrent has %x", got, tor.InfoHash)
		}
		if len(tor.Files) == 0 {
			t.Fatalf("accepted torrent without files")
		}
		var totalLength int64
		for _, file := range tor.Files {
			if file.Length < 0 {
				t.Fatalf("accepted negative file length: %d", file.Length)
			}
			if len(file.Path) == 0 {
				t.Fatalf("accepted file without path")
			}
			if totalLength > math.MaxInt64-file.Length {
				t.Fatalf("accepted torrent with overflowing total file length")
			}
			totalLength += file.Length
		}
		if totalLength <= 0 {
			t.Fatalf("accepted non-positive total length: %d", totalLength)
		}
		expectedPieces := int((totalLength-1)/tor.PieceLength + 1)
		if len(tor.PieceHashes) != expectedPieces {
			t.Fatalf("piece count mismatch after parse: got %d, want %d", len(tor.PieceHashes), expectedPieces)
		}
	})
}

func FuzzParseMagnet(f *testing.F) {
	const hexHash = "d0d14c926e6e99761a2fdcea2f72c155e3256b48"
	for _, seed := range []string{
		"magnet:?xt=urn:btih:" + hexHash,
		"magnet:?xt=urn:btih:2DIUZETON2MXMGRP3TVCR4WBLXRSK22I&dn=Base32+Seed",
		"magnet:?xt=urn:btih:" + hexHash + "&dn=Test+Torrent&tr=udp%3A%2F%2Ftracker.example%3A1337&tr=http%3A%2F%2Ftracker.example%2Fannounce",
		"magnet:?dn=missing-hash",
		"magnet:?xt=urn:btih:short",
		"magnet:?xt=urn:sha1:" + hexHash,
		"http://example.com/not-a-magnet",
		"magnet:?xt=%zz",
		"",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, uri string) {
		ml, err := ParseMagnet(uri)
		if err != nil {
			return
		}
		roundTripURI := "magnet:?xt=urn:btih:" + hex.EncodeToString(ml.InfoHash[:])
		roundTrip, err := ParseMagnet(roundTripURI)
		if err != nil {
			t.Fatalf("failed to parse round-trip magnet URI %q: %v", roundTripURI, err)
		}
		if roundTrip.InfoHash != ml.InfoHash {
			t.Fatalf("magnet info hash changed after round trip: got %x, want %x", roundTrip.InfoHash, ml.InfoHash)
		}
	})
}

func addTorrentSeed(f *testing.F, value map[string]interface{}) {
	data, err := bencode.Marshal(value)
	if err != nil {
		f.Fatalf("failed to marshal torrent fuzz seed: %v", err)
	}
	f.Add(data)
}
