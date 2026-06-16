package downloader

import (
	"crypto/sha1"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sainttorrent/pkg/torrent"
)

// TestScrapeTrackersUpdatesStats verifies that scrapeTrackers queries the
// derived scrape endpoint and records the swarm-health counts so the TUI can
// surface seeders / leechers / completed.
func TestScrapeTrackersUpdatesStats(t *testing.T) {
	infoHash := sha1.Sum([]byte("scrape-me"))

	var gotScrapePath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotScrapePath = r.URL.Path
		body := []byte("d5:filesd20:")
		body = append(body, infoHash[:]...)
		body = append(body, []byte("d8:completei42e10:downloadedi1000e10:incompletei7eeee")...)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	tor := &torrent.Torrent{
		Name:     "scrape-me",
		InfoHash: infoHash,
		Trackers: []string{srv.URL + "/announce"},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.scrapeTrackers()

	if gotScrapePath != "/scrape" {
		t.Fatalf("expected scrape request to /scrape, got %q", gotScrapePath)
	}

	seeders, leechers, completed := sess.TrackerSwarmStats()
	if seeders != 42 {
		t.Errorf("expected 42 seeders, got %d", seeders)
	}
	if leechers != 7 {
		t.Errorf("expected 7 leechers, got %d", leechers)
	}
	if completed != 1000 {
		t.Errorf("expected 1000 completed, got %d", completed)
	}
}

// TestScrapeTrackersBestEffort verifies that an unreachable or non-scrapeable
// tracker leaves the stored stats untouched rather than failing.
func TestScrapeTrackersBestEffort(t *testing.T) {
	oldTimeout := trackerAnnounceTimeout
	trackerAnnounceTimeout = 200 * time.Millisecond
	defer func() { trackerAnnounceTimeout = oldTimeout }()

	tor := &torrent.Torrent{
		Name:     "no-scrape",
		InfoHash: sha1.Sum([]byte("no-scrape")),
		// Path has no "announce" segment, so scrape URL derivation fails.
		Trackers: []string{"http://127.0.0.1:1/tracker"},
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.mu.Lock()
	sess.trackerSeeders = 5
	sess.trackerLeechers = 3
	sess.mu.Unlock()

	sess.scrapeTrackers()

	seeders, leechers, completed := sess.TrackerSwarmStats()
	if seeders != 5 || leechers != 3 || completed != 0 {
		t.Errorf("expected stats untouched (5/3/0), got %d/%d/%d", seeders, leechers, completed)
	}
}

// TestScrapeTrackersNoTrackers verifies scrapeTrackers is a no-op when the
// torrent has no trackers configured.
func TestScrapeTrackersNoTrackers(t *testing.T) {
	tor := &torrent.Torrent{
		Name:     "trackerless",
		InfoHash: sha1.Sum([]byte("trackerless")),
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, "")
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	sess.scrapeTrackers() // must not panic or block

	seeders, leechers, completed := sess.TrackerSwarmStats()
	if seeders != 0 || leechers != 0 || completed != 0 {
		t.Errorf("expected zero stats, got %d/%d/%d", seeders, leechers, completed)
	}
}
