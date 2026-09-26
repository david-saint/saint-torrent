package bencode

import (
	"strings"
	"testing"
)

// benchInputs are representative control-plane payloads: a DHT get_peers
// response, a BEP 10 extension handshake and a small multi-file metainfo.
func benchInputs(b *testing.B) map[string][]byte {
	b.Helper()
	files := make([]interface{}, 0, 64)
	for i := 0; i < 64; i++ {
		files = append(files, map[string]interface{}{
			"length": int64(1 << 20),
			"path":   []interface{}{"dir", strings.Repeat("f", 8+i%8) + ".bin"},
		})
	}
	values := map[string]interface{}{
		"dht": map[string]interface{}{
			"t": "aa", "y": "r",
			"r": map[string]interface{}{
				"id":     strings.Repeat("i", 20),
				"token":  "tok12345",
				"values": []interface{}{"abcdef", "ghijkl", "mnopqr", "stuvwx", "yz0123", "456789"},
				"nodes":  strings.Repeat("n", 26*8),
			},
		},
		"ext_handshake": map[string]interface{}{
			"m":             map[string]interface{}{"ut_metadata": int64(2), "ut_pex": int64(1)},
			"metadata_size": int64(31337),
			"p":             int64(6881),
			"v":             "saintTorrent 1.0",
			"reqq":          int64(250),
		},
		"metainfo": map[string]interface{}{
			"announce": "udp://tracker.example:1337/announce",
			"info": map[string]interface{}{
				"name":         "bench",
				"piece length": int64(1 << 18),
				"pieces":       strings.Repeat("h", 20*256),
				"files":        files,
			},
		},
	}
	out := make(map[string][]byte, len(values))
	for name, v := range values {
		data, err := Marshal(v)
		if err != nil {
			b.Fatalf("marshal %s: %v", name, err)
		}
		out[name] = data
	}
	return out
}

func BenchmarkUnmarshal(b *testing.B) {
	for name, data := range benchInputs(b) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				if _, err := Unmarshal(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
