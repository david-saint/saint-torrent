package torrent

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"sainttorrent/pkg/bencode"
)

func parseWithTopLevel(t *testing.T, extra map[string]interface{}) *Torrent {
	t.Helper()
	top := map[string]interface{}{"info": singleFileInfo("f.bin", 16, 16)}
	for k, v := range extra {
		top[k] = v
	}
	data, err := bencode.Marshal(top)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	tor, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse() = %v", err)
	}
	return tor
}

// TestParseTrackersNormalized checks that announce-list keeps only real
// tracker URLs, once each, in tier order. Every announce contacts each tracker
// concurrently, so junk and repeats used to cost a socket apiece.
func TestParseTrackersNormalized(t *testing.T) {
	tor := parseWithTopLevel(t, map[string]interface{}{
		"announce": " HTTP://Tracker.Example.com/announce ",
		"announce-list": []interface{}{
			[]interface{}{"udp://tracker.one.example:1337/announce", "UDP://TRACKER.ONE.EXAMPLE:1337/announce"},
			[]interface{}{"", "  ", "file:///etc/passwd", "javascript:alert(1)", "dht://abc", "udp:opaque", "http://:80/announce", int64(7)},
			"not a tier",
			[]interface{}{" https://tracker.two.example/announce?passkey=AbC ", "http://tracker.one.example:1337/announce"},
		},
	})
	want := []string{
		"udp://tracker.one.example:1337/announce",
		"https://tracker.two.example/announce?passkey=AbC", // path and query keep their case
		"http://tracker.one.example:1337/announce",         // another scheme is another tracker
	}
	if !reflect.DeepEqual(tor.Trackers, want) {
		t.Fatalf("Trackers = %q, want %q", tor.Trackers, want)
	}
	if tor.Announce != "http://tracker.example.com/announce" {
		t.Fatalf("Announce = %q, want normalized announce", tor.Announce)
	}
}

func TestParseTrackersFallbackAndInvalidAnnounce(t *testing.T) {
	tor := parseWithTopLevel(t, map[string]interface{}{
		"announce":      "udp://tracker.example:6969",
		"announce-list": []interface{}{[]interface{}{"ftp://nope.example/"}},
	})
	if want := []string{"udp://tracker.example:6969"}; !reflect.DeepEqual(tor.Trackers, want) {
		t.Fatalf("Trackers = %q, want announce fallback %q", tor.Trackers, want)
	}

	tor = parseWithTopLevel(t, map[string]interface{}{"announce": "javascript:alert(1)"})
	if tor.Announce != "" || len(tor.Trackers) != 0 {
		t.Fatalf("Announce=%q Trackers=%q, want invalid announce dropped", tor.Announce, tor.Trackers)
	}
}

func TestParseTrackersCapped(t *testing.T) {
	tier := make([]interface{}, 0, 5000)
	for i := 0; i < cap(tier); i++ {
		tier = append(tier, fmt.Sprintf("udp://127.0.0.1:%d/announce", 1000+i))
	}
	tor := parseWithTopLevel(t, map[string]interface{}{"announce-list": []interface{}{tier}})
	if len(tor.Trackers) != maxTrackers {
		t.Fatalf("kept %d trackers, want the cap of %d", len(tor.Trackers), maxTrackers)
	}
	if tor.Trackers[0] != "udp://127.0.0.1:1000/announce" || tor.Trackers[maxTrackers-1] != fmt.Sprintf("udp://127.0.0.1:%d/announce", 1000+maxTrackers-1) {
		t.Fatalf("cap did not keep the first entries: first=%q last=%q", tor.Trackers[0], tor.Trackers[maxTrackers-1])
	}
}

func TestParseWebSeedsNormalizedAndCapped(t *testing.T) {
	tor := parseWithTopLevel(t, map[string]interface{}{
		"url-list": []interface{}{
			"http://Seed.Example/base/",
			"http://seed.example/base/",
			"udp://seed.example/", // web seeds are HTTP only
			"ftp://seed.example/",
			"https://seed-b.example/x",
		},
	})
	if want := []string{"http://seed.example/base/", "https://seed-b.example/x"}; !reflect.DeepEqual(tor.WebSeeds, want) {
		t.Fatalf("WebSeeds = %q, want %q", tor.WebSeeds, want)
	}

	seeds := make([]interface{}, 0, 3000)
	for i := 0; i < cap(seeds); i++ {
		seeds = append(seeds, fmt.Sprintf("http://mirror%d.example/", i))
	}
	tor = parseWithTopLevel(t, map[string]interface{}{"url-list": seeds})
	if len(tor.WebSeeds) != maxWebSeeds || tor.WebSeeds[0] != "http://mirror0.example/" {
		t.Fatalf("kept %d web seeds starting %q, want the first %d", len(tor.WebSeeds), tor.WebSeeds[0], maxWebSeeds)
	}
}

// TestParseMagnetTrackersNormalizedAndCapped covers a link that carries
// thousands of tr= values (one socket each per announce) plus non-tracker
// schemes.
func TestParseMagnetTrackersNormalizedAndCapped(t *testing.T) {
	var b strings.Builder
	b.WriteString("magnet:?xt=urn:btih:" + testHashHex)
	for _, tr := range []string{"file:///etc/passwd", "javascript:alert(1)", "UDP://Tracker.Example:80", "udp://tracker.example:80", " "} {
		b.WriteString("&tr=" + url.QueryEscape(tr))
	}
	for i := 0; i < 5000; i++ {
		b.WriteString("&tr=" + url.QueryEscape(fmt.Sprintf("http://127.0.0.1:%d/announce", 1000+i)))
	}
	ml, err := ParseMagnet(b.String())
	if err != nil {
		t.Fatalf("ParseMagnet() = %v", err)
	}
	if len(ml.Trackers) != maxTrackers {
		t.Fatalf("kept %d trackers, want the cap of %d", len(ml.Trackers), maxTrackers)
	}
	if ml.Trackers[0] != "udp://tracker.example:80" || ml.Trackers[1] != "http://127.0.0.1:1000/announce" {
		t.Fatalf("Trackers start %q, want the normalized udp tracker then the first http one", ml.Trackers[:2])
	}
}
