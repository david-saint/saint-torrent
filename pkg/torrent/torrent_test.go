package torrent

import (
	"crypto/sha1"
	"reflect"
	"testing"

	"sainttorrent/pkg/bencode"
)

func TestParseTorrent(t *testing.T) {
	// 1. Single File Torrent Data
	singleInfo := map[string]interface{}{
		"name":         "single.txt",
		"piece length": int64(16384),
		"pieces":       string(make([]byte, 40)), // 2 pieces (40 bytes total)
		"length":       int64(25000),
	}
	bencodedInfo, err := bencode.Marshal(singleInfo)
	if err != nil {
		t.Fatalf("Failed to marshal single info: %v", err)
	}
	expectedSingleInfoHash := sha1.Sum(bencodedInfo)

	singleTorrentMap := map[string]interface{}{
		"announce": "http://tracker.example.com/announce",
		"info":     singleInfo,
	}
	bencodedSingleTorrent, err := bencode.Marshal(singleTorrentMap)
	if err != nil {
		t.Fatalf("Failed to marshal single torrent: %v", err)
	}

	// 2. Multi File Torrent Data
	multiInfo := map[string]interface{}{
		"name":         "multi_dir",
		"piece length": int64(32768),
		"pieces":       string(make([]byte, 20)), // 1 piece for 30KB total
		"files": []interface{}{
			map[string]interface{}{
				"length": int64(10000),
				"path":   []interface{}{"subdir", "file1.txt"},
			},
			map[string]interface{}{
				"length": int64(20000),
				"path":   []interface{}{"file2.txt"},
			},
		},
	}
	bencodedMultiInfo, err := bencode.Marshal(multiInfo)
	if err != nil {
		t.Fatalf("Failed to marshal multi info: %v", err)
	}
	expectedMultiInfoHash := sha1.Sum(bencodedMultiInfo)

	multiTorrentMap := map[string]interface{}{
		"announce": "http://tracker.example.com/announce",
		"announce-list": []interface{}{
			[]interface{}{"http://tracker1.example.com/announce"},
			[]interface{}{"http://tracker2.example.com/announce"},
		},
		"info": multiInfo,
	}
	bencodedMultiTorrent, err := bencode.Marshal(multiTorrentMap)
	if err != nil {
		t.Fatalf("Failed to marshal multi torrent: %v", err)
	}

	// Run tests
	t.Run("Single-file Torrent", func(t *testing.T) {
		tor, err := Parse(bencodedSingleTorrent)
		if err != nil {
			t.Fatalf("Parse() failed: %v", err)
		}

		if tor.Announce != "http://tracker.example.com/announce" {
			t.Errorf("Expected Announce %q, got %q", "http://tracker.example.com/announce", tor.Announce)
		}

		if tor.Name != "single.txt" {
			t.Errorf("Expected Name %q, got %q", "single.txt", tor.Name)
		}

		if tor.PieceLength != 16384 {
			t.Errorf("Expected PieceLength 16384, got %d", tor.PieceLength)
		}

		if len(tor.PieceHashes) != 2 {
			t.Errorf("Expected 2 piece hashes, got %d", len(tor.PieceHashes))
		}

		if tor.InfoHash != expectedSingleInfoHash {
			t.Errorf("Expected InfoHash %x, got %x", expectedSingleInfoHash, tor.InfoHash)
		}

		if len(tor.Files) != 1 {
			t.Fatalf("Expected 1 file, got %d", len(tor.Files))
		}

		f := tor.Files[0]
		if f.Length != 25000 {
			t.Errorf("Expected file length 25000, got %d", f.Length)
		}
		if !reflect.DeepEqual(f.Path, []string{"single.txt"}) {
			t.Errorf("Expected file path [\"single.txt\"], got %v", f.Path)
		}

		// Trackers should contain fallback announce if announce-list is missing
		expectedTrackers := []string{"http://tracker.example.com/announce"}
		if !reflect.DeepEqual(tor.Trackers, expectedTrackers) {
			t.Errorf("Expected Trackers %v, got %v", expectedTrackers, tor.Trackers)
		}
	})

	t.Run("Multi-file Torrent", func(t *testing.T) {
		tor, err := Parse(bencodedMultiTorrent)
		if err != nil {
			t.Fatalf("Parse() failed: %v", err)
		}

		if tor.Name != "multi_dir" {
			t.Errorf("Expected Name %q, got %q", "multi_dir", tor.Name)
		}

		if tor.PieceLength != 32768 {
			t.Errorf("Expected PieceLength 32768, got %d", tor.PieceLength)
		}

		if len(tor.PieceHashes) != 1 {
			t.Errorf("Expected 1 piece hash, got %d", len(tor.PieceHashes))
		}

		if tor.InfoHash != expectedMultiInfoHash {
			t.Errorf("Expected InfoHash %x, got %x", expectedMultiInfoHash, tor.InfoHash)
		}

		if len(tor.Files) != 2 {
			t.Fatalf("Expected 2 files, got %d", len(tor.Files))
		}

		f1 := tor.Files[0]
		if f1.Length != 10000 {
			t.Errorf("Expected file 1 length 10000, got %d", f1.Length)
		}
		if !reflect.DeepEqual(f1.Path, []string{"multi_dir", "subdir", "file1.txt"}) {
			t.Errorf("Expected file 1 path [\"multi_dir\", \"subdir\", \"file1.txt\"], got %v", f1.Path)
		}

		f2 := tor.Files[1]
		if f2.Length != 20000 {
			t.Errorf("Expected file 2 length 20000, got %d", f2.Length)
		}
		if !reflect.DeepEqual(f2.Path, []string{"multi_dir", "file2.txt"}) {
			t.Errorf("Expected file 2 path [\"multi_dir\", \"file2.txt\"], got %v", f2.Path)
		}

		expectedTrackers := []string{
			"http://tracker1.example.com/announce",
			"http://tracker2.example.com/announce",
		}
		if !reflect.DeepEqual(tor.Trackers, expectedTrackers) {
			t.Errorf("Expected Trackers %v, got %v", expectedTrackers, tor.Trackers)
		}
	})

	t.Run("Malformed & Edge Cases", func(t *testing.T) {
		// Missing fields
		invalidMap := map[string]interface{}{
			"announce": "http://tracker.example.com/announce",
			// missing info
		}
		b, _ := bencode.Marshal(invalidMap)
		_, err := Parse(b)
		if err == nil {
			t.Error("Expected error for missing info dict, got nil")
		}

		// Pieces length not multiple of 20
		badInfo := map[string]interface{}{
			"name":         "single.txt",
			"piece length": int64(16384),
			"pieces":       "12345", // 5 bytes (invalid)
			"length":       int64(25000),
		}
		badTorrentMap := map[string]interface{}{
			"announce": "http://tracker.example.com/announce",
			"info":     badInfo,
		}
		b2, _ := bencode.Marshal(badTorrentMap)
		_, err = Parse(b2)
		if err == nil {
			t.Error("Expected error for invalid pieces length, got nil")
		}
	})
}

func TestParseRejectsInvalidLengthsAndPathCollisions(t *testing.T) {
	t.Run("negative file length", func(t *testing.T) {
		info := map[string]interface{}{
			"name":         "bad.txt",
			"piece length": int64(16384),
			"pieces":       string(make([]byte, 20)),
			"length":       int64(-1),
		}
		data, _ := bencode.Marshal(map[string]interface{}{"info": info})
		if _, err := Parse(data); err == nil {
			t.Fatal("expected negative file length to be rejected")
		}
	})

	t.Run("non-positive piece length", func(t *testing.T) {
		info := map[string]interface{}{
			"name":         "bad.txt",
			"piece length": int64(0),
			"pieces":       string(make([]byte, 20)),
			"length":       int64(1),
		}
		data, _ := bencode.Marshal(map[string]interface{}{"info": info})
		if _, err := Parse(data); err == nil {
			t.Fatal("expected non-positive piece length to be rejected")
		}
	})

	t.Run("piece hash count mismatch", func(t *testing.T) {
		info := map[string]interface{}{
			"name":         "bad.txt",
			"piece length": int64(10),
			"pieces":       string(make([]byte, 20)),
			"length":       int64(11),
		}
		data, _ := bencode.Marshal(map[string]interface{}{"info": info})
		if _, err := Parse(data); err == nil {
			t.Fatal("expected piece hash count mismatch to be rejected")
		}
	})

	t.Run("sanitized path collision", func(t *testing.T) {
		info := map[string]interface{}{
			"name":         "root",
			"piece length": int64(16384),
			"pieces":       string(make([]byte, 20)),
			"files": []interface{}{
				map[string]interface{}{"length": int64(1), "path": []interface{}{"a/b"}},
				map[string]interface{}{"length": int64(1), "path": []interface{}{"a_b"}},
			},
		}
		data, _ := bencode.Marshal(map[string]interface{}{"info": info})
		if _, err := Parse(data); err == nil {
			t.Fatal("expected sanitized path collision to be rejected")
		}
	})
}

func TestParsePrivateFlag(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  bool
	}{
		{name: "missing", want: false},
		{name: "zero", value: int64(0), want: false},
		{name: "one", value: int64(1), want: true},
		{name: "other integer", value: int64(2), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := map[string]interface{}{
				"name":         "private.txt",
				"piece length": int64(16),
				"pieces":       string(make([]byte, 20)),
				"length":       int64(16),
			}
			if tt.value != nil {
				info["private"] = tt.value
			}
			data, err := bencode.Marshal(map[string]interface{}{"info": info})
			if err != nil {
				t.Fatalf("failed to marshal torrent: %v", err)
			}
			tor, err := Parse(data)
			if err != nil {
				t.Fatalf("Parse() failed: %v", err)
			}
			if tor.Private != tt.want {
				t.Fatalf("Private = %v, want %v", tor.Private, tt.want)
			}
		})
	}
}

func TestParseURLListWebSeeds(t *testing.T) {
	t.Run("single string", func(t *testing.T) {
		info := map[string]interface{}{
			"name":         "seeded.iso",
			"piece length": int64(16),
			"pieces":       string(make([]byte, 20)),
			"length":       int64(16),
		}
		data, err := bencode.Marshal(map[string]interface{}{
			"info":     info,
			"url-list": "  http://seed.example/seeded.iso  ",
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		tor, err := Parse(data)
		if err != nil {
			t.Fatalf("Parse() failed: %v", err)
		}
		want := []string{"http://seed.example/seeded.iso"}
		if !reflect.DeepEqual(tor.WebSeeds, want) {
			t.Fatalf("WebSeeds = %v, want %v", tor.WebSeeds, want)
		}
	})

	t.Run("list", func(t *testing.T) {
		info := map[string]interface{}{
			"name":         "root",
			"piece length": int64(16),
			"pieces":       string(make([]byte, 20)),
			"files": []interface{}{
				map[string]interface{}{"length": int64(8), "path": []interface{}{"a.bin"}},
				map[string]interface{}{"length": int64(8), "path": []interface{}{"b.bin"}},
			},
		}
		data, err := bencode.Marshal(map[string]interface{}{
			"info": info,
			"url-list": []interface{}{
				"http://seed-a.example/base/",
				"",
				" https://seed-b.example/base ",
				int64(12),
			},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		tor, err := Parse(data)
		if err != nil {
			t.Fatalf("Parse() failed: %v", err)
		}
		want := []string{"http://seed-a.example/base/", "https://seed-b.example/base"}
		if !reflect.DeepEqual(tor.WebSeeds, want) {
			t.Fatalf("WebSeeds = %v, want %v", tor.WebSeeds, want)
		}
	})
}
