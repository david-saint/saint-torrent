package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/torrent"
)

// TestTrackerPeerSource pins down which trackers may hand out local peers. It
// takes trackerLogID's output, which is what announceAndConnect passes.
func TestTrackerPeerSource(t *testing.T) {
	cases := map[string]string{
		"http://127.0.0.1:6969/announce":          "127.0.0.1",
		"udp://[::1]:6969/announce":               "::1",
		"http://localhost:6969/announce":          "127.0.0.1",
		"http://LOCALHOST.:6969/announce":         "127.0.0.1",
		"http://192.168.1.5/announce":             "192.168.1.5",
		"udp://tracker.example.org:1337/announce": "",
		"http://localhost.example.org/announce":   "",
		"not a url":                               "",
	}
	for raw, want := range cases {
		got := trackerPeerSource(trackerLogID(raw))
		if (want == "" && got.IsValid()) || (want != "" && got != netip.MustParseAddr(want)) {
			t.Errorf("trackerPeerSource(%q) = %v, want %q", raw, got, want)
		}
	}
}

// TestAnnounceDropsUnusableTrackerPeers covers a tracker listing endpoints that
// can never be a peer, or that are more local than the tracker itself.
func TestAnnounceDropsUnusableTrackerPeers(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 2 * time.Second
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	usable := refusingLoopbackPort(t)
	var compact []byte
	for _, ep := range []struct {
		ip   string
		port int
	}{
		{"127.0.0.1", usable},
		{"224.0.0.1", 6881},
		{"255.255.255.255", 6881},
		{"0.1.2.3", 6881},
		{"240.0.0.1", 6881},
	} {
		compact = append(compact, net.ParseIP(ep.ip).To4()...)
		compact = binary.BigEndian.AppendUint16(compact, uint16(ep.port))
	}
	resp, err := bencode.Marshal(map[string]interface{}{"interval": int64(1800), "peers": string(compact)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	sess, err := NewSession(&torrent.Torrent{
		Name:     "tracker-filter",
		InfoHash: sha1.Sum([]byte("tracker-filter")),
		Trackers: []string{srv.URL + "/announce"},
	}, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer sess.Close()
	sess.announceAndConnect()

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	want := fmt.Sprintf("127.0.0.1:%d", usable)
	for addr := range sess.Peers {
		if addr != want {
			t.Errorf("tracker endpoint %s was accepted", addr)
		}
	}
	if sess.Peers[want] == nil {
		t.Fatal("the loopback tracker's loopback peer was not accepted")
	}
}

func TestTrackerPeerAllowed(t *testing.T) {
	public := netip.Addr{}
	loopback := netip.MustParseAddr("127.0.0.1")
	cases := []struct {
		ip     string
		source netip.Addr
		want   bool
	}{
		{"198.51.100.7", public, true},
		{"127.0.0.1", public, false},
		{"::ffff:127.0.0.1", public, false},
		{"192.168.1.1", public, false},
		{"127.0.0.1", loopback, true},
		{"192.168.1.1", loopback, true},
		{"224.0.0.1", loopback, false},
	}
	for _, c := range cases {
		p := trackerPeer(c.ip, 6881)
		if got := trackerPeerAllowed(p, c.source); got != c.want {
			t.Errorf("trackerPeerAllowed(%s, %v) = %v, want %v", c.ip, c.source, got, c.want)
		}
	}
	if trackerPeerAllowed(trackerPeer("198.51.100.7", 0), public) {
		t.Error("port 0 was allowed")
	}
}
