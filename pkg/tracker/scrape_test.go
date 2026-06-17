package tracker

import (
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestScrapeURL(t *testing.T) {
	tests := []struct {
		name     string
		announce string
		want     string
		wantErr  bool
	}{
		{"simple", "http://tracker.example.com/announce", "http://tracker.example.com/scrape", false},
		{"with port", "http://t.io:8080/announce", "http://t.io:8080/scrape", false},
		{"php suffix", "http://t.io/announce.php", "http://t.io/scrape.php", false},
		{"nested path", "https://t.io/x/y/announce", "https://t.io/x/y/scrape", false},
		{"udp scheme", "udp://t.io:1337/announce", "udp://t.io:1337/scrape", false},
		{"keeps query", "http://t.io/announce?foo=bar", "http://t.io/scrape?foo=bar", false},
		{"no announce segment", "http://t.io/path", "", true},
		{"root only", "http://t.io/", "", true},
		{"no path", "http://t.io", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScrapeURL(tt.announce)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %q", tt.announce, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ScrapeURL(%q) = %q, want %q", tt.announce, got, tt.want)
			}
		})
	}
}

func TestBuildScrapeURL(t *testing.T) {
	hash1 := [20]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34,
		0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78}
	hash2 := [20]byte{1, 2, 3}

	raw, err := BuildScrapeURL("http://t.io/scrape", hash1, hash2)
	if err != nil {
		t.Fatalf("BuildScrapeURL failed: %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("failed to parse output URL: %v", err)
	}
	hashes := u.Query()["info_hash"]
	if len(hashes) != 2 {
		t.Fatalf("expected 2 info_hash params, got %d (%v)", len(hashes), hashes)
	}
	if hashes[0] != string(hash1[:]) {
		t.Errorf("first info_hash mismatch")
	}
	if hashes[1] != string(hash2[:]) {
		t.Errorf("second info_hash mismatch")
	}
}

func TestParseScrapeResponse(t *testing.T) {
	var hash [20]byte
	copy(hash[:], "aaaaaaaaaaaaaaaaaaaa")

	// d5:filesd20:<hash>d8:completei5e10:downloadedi42e10:incompletei3eeee
	bencoded := []byte("d5:filesd20:")
	bencoded = append(bencoded, hash[:]...)
	bencoded = append(bencoded, []byte("d8:completei5e10:downloadedi42e10:incompletei3eeee")...)

	result, err := ParseScrapeResponse(bencoded)
	if err != nil {
		t.Fatalf("ParseScrapeResponse failed: %v", err)
	}
	stats, ok := result[hash]
	if !ok {
		t.Fatalf("expected stats for hash, got %v", result)
	}
	if stats.Complete != 5 {
		t.Errorf("expected complete 5, got %d", stats.Complete)
	}
	if stats.Downloaded != 42 {
		t.Errorf("expected downloaded 42, got %d", stats.Downloaded)
	}
	if stats.Incomplete != 3 {
		t.Errorf("expected incomplete 3, got %d", stats.Incomplete)
	}
}

func TestParseScrapeResponse_Failures(t *testing.T) {
	cases := map[string][]byte{
		"invalid bencode": []byte("not-bencode"),
		"not a dict":      []byte("i5e"),
		"missing files":   []byte("d8:intervali5ee"),
		"failure reason":  []byte("d14:failure reason9:try latere"),
		"trailing data":   append([]byte("d5:filesdee"), 'x'),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseScrapeResponse(data); err == nil {
				t.Errorf("expected error for %q, got nil", name)
			}
		})
	}
}

func TestParseScrapeResponse_SkipsBadKeys(t *testing.T) {
	// files dict with a 3-byte key (not a valid 20-byte info hash) -> skipped.
	bencoded := []byte("d5:filesd3:abcd8:completei9eeee")
	result, err := ParseScrapeResponse(bencoded)
	if err != nil {
		t.Fatalf("ParseScrapeResponse failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected bad-key entry to be skipped, got %v", result)
	}
}

func TestHTTPScrape(t *testing.T) {
	var hash [20]byte
	copy(hash[:], "bbbbbbbbbbbbbbbbbbbb")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/scrape" {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		gotHash := r.URL.Query().Get("info_hash")
		if gotHash != string(hash[:]) {
			http.Error(w, "wrong info_hash", http.StatusBadRequest)
			return
		}
		body := []byte("d5:filesd20:")
		body = append(body, hash[:]...)
		body = append(body, []byte("d8:completei10e10:downloadedi100e10:incompletei7eeee")...)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := HTTPScrape(ctx, srv.URL+"/announce", hash)
	if err != nil {
		t.Fatalf("HTTPScrape failed: %v", err)
	}
	stats := result[hash]
	if stats.Complete != 10 || stats.Downloaded != 100 || stats.Incomplete != 7 {
		t.Errorf("unexpected stats: %+v", stats)
	}
}

func TestHTTPScrape_UnsupportedAnnounce(t *testing.T) {
	ctx := context.Background()
	if _, err := HTTPScrape(ctx, "http://t.io/path-without-announce", [20]byte{}); err == nil {
		t.Fatal("expected error for announce URL without scrape support, got nil")
	}
}

func TestHTTPScrape_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx := context.Background()
	if _, err := HTTPScrape(ctx, srv.URL+"/announce", [20]byte{}); err == nil {
		t.Fatal("expected error for HTTP 500, got nil")
	}
}

func TestScrape_DispatchUnsupportedScheme(t *testing.T) {
	ctx := context.Background()
	if _, err := Scrape(ctx, "ftp://t.io/announce", [20]byte{}); err == nil {
		t.Fatal("expected error for unsupported scheme, got nil")
	}
}

// --- UDP scrape ---

// mockUDPScrapeServer answers connect and scrape requests, replying with the
// provided per-hash stats (matched positionally to the requested hashes).
func mockUDPScrapeServer(t *testing.T, stats []ScrapeStats) (string, func()) {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 12 {
				continue
			}
			action := binary.BigEndian.Uint32(buf[8:12])
			switch action {
			case actionConnect:
				if n < udpConnectRequestSize {
					continue
				}
				txnID := binary.BigEndian.Uint32(buf[12:16])
				var resp [16]byte
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				binary.BigEndian.PutUint32(resp[4:8], txnID)
				binary.BigEndian.PutUint64(resp[8:16], 0xCAFEBABEDEADBEEF)
				pc.WriteTo(resp[:], addr)
			case actionScrape:
				txnID := binary.BigEndian.Uint32(buf[12:16])
				resp := make([]byte, udpScrapeResponseHeaderSize+len(stats)*udpScrapeStatSize)
				binary.BigEndian.PutUint32(resp[0:4], actionScrape)
				binary.BigEndian.PutUint32(resp[4:8], txnID)
				for i, st := range stats {
					off := udpScrapeResponseHeaderSize + i*udpScrapeStatSize
					binary.BigEndian.PutUint32(resp[off:off+4], uint32(st.Complete))
					binary.BigEndian.PutUint32(resp[off+4:off+8], uint32(st.Downloaded))
					binary.BigEndian.PutUint32(resp[off+8:off+12], uint32(st.Incomplete))
				}
				pc.WriteTo(resp, addr)
			}
		}
	}()

	cleanup := func() {
		pc.Close()
		<-done
	}
	return pc.LocalAddr().String(), cleanup
}

func TestUDPScrape_FullFlow(t *testing.T) {
	hash1 := [20]byte{1, 2, 3, 4}
	hash2 := [20]byte{5, 6, 7, 8}
	want := []ScrapeStats{
		{Complete: 100, Downloaded: 5000, Incomplete: 20},
		{Complete: 3, Downloaded: 9, Incomplete: 1},
	}

	addr, cleanup := mockUDPScrapeServer(t, want)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := UDPScrape(ctx, "udp://"+addr+"/announce", hash1, hash2)
	if err != nil {
		t.Fatalf("UDPScrape failed: %v", err)
	}
	if got := result[hash1]; got != want[0] {
		t.Errorf("hash1: got %+v, want %+v", got, want[0])
	}
	if got := result[hash2]; got != want[1] {
		t.Errorf("hash2: got %+v, want %+v", got, want[1])
	}
}

func TestUDPScrape_NoHashes(t *testing.T) {
	if _, err := UDPScrape(context.Background(), "udp://127.0.0.1:1/announce"); err == nil {
		t.Fatal("expected error when no info hashes provided, got nil")
	}
}

func TestUDPScrape_TooManyHashes(t *testing.T) {
	hashes := make([][20]byte, udpMaxScrapeHashes+1)
	if _, err := UDPScrape(context.Background(), "udp://127.0.0.1:1/announce", hashes...); err == nil {
		t.Fatal("expected error for too many info hashes, got nil")
	}
}

func TestUDPScrape_InvalidScheme(t *testing.T) {
	if _, err := UDPScrape(context.Background(), "http://t.io/announce", [20]byte{}); err == nil {
		t.Fatal("expected error for non-udp scheme, got nil")
	}
}

func TestUDPScrape_Timeout(t *testing.T) {
	// Server that never responds.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer pc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, err := UDPScrape(ctx, "udp://"+pc.LocalAddr().String()+"/announce", [20]byte{}); err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestParseUDPScrapeResponse_TooShort(t *testing.T) {
	if _, err := parseUDPScrapeResponse(make([]byte, 4), [][20]byte{{}}); err == nil {
		t.Fatal("expected error for too-short response, got nil")
	}
}

func TestParseUDPScrapeResponse_TruncatedStats(t *testing.T) {
	// header (8) + only 6 bytes of stats for one hash (needs 12) -> error.
	data := make([]byte, udpScrapeResponseHeaderSize+6)
	binary.BigEndian.PutUint32(data[0:4], actionScrape)
	if _, err := parseUDPScrapeResponse(data, [][20]byte{{}}); err == nil {
		t.Fatal("expected error for truncated stats, got nil")
	}
}

func TestScrape_DispatchUDP(t *testing.T) {
	hash := [20]byte{9, 9, 9}
	addr, cleanup := mockUDPScrapeServer(t, []ScrapeStats{{Complete: 1, Downloaded: 2, Incomplete: 3}})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := Scrape(ctx, "udp://"+addr+"/announce", hash)
	if err != nil {
		t.Fatalf("Scrape dispatch to UDP failed: %v", err)
	}
	if got := result[hash]; got.Complete != 1 || got.Downloaded != 2 || got.Incomplete != 3 {
		t.Errorf("unexpected stats: %+v", got)
	}
}

func TestParseUDPScrapeResponse_FewerTriplesThanHashes(t *testing.T) {
	// Two hashes requested, but the tracker returns only one triple. The first
	// hash must be mapped and the second simply absent (tolerated, not an error).
	h1 := [20]byte{1}
	h2 := [20]byte{2}
	data := make([]byte, udpScrapeResponseHeaderSize+udpScrapeStatSize)
	binary.BigEndian.PutUint32(data[0:4], actionScrape)
	off := udpScrapeResponseHeaderSize
	binary.BigEndian.PutUint32(data[off:off+4], 11)    // seeders
	binary.BigEndian.PutUint32(data[off+4:off+8], 22)  // completed
	binary.BigEndian.PutUint32(data[off+8:off+12], 33) // leechers

	result, err := parseUDPScrapeResponse(data, [][20]byte{h1, h2})
	if err != nil {
		t.Fatalf("expected fewer triples to be tolerated, got error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 mapped result, got %d", len(result))
	}
	if got := result[h1]; got.Complete != 11 || got.Downloaded != 22 || got.Incomplete != 33 {
		t.Errorf("unexpected stats for first hash: %+v", got)
	}
	if _, ok := result[h2]; ok {
		t.Errorf("did not expect stats for the omitted second hash")
	}
}
