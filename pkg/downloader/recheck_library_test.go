package downloader

// Benchmarks the recheck scheduler against a real saintTorrent library, so the
// numbers come from real torrents (multi-file layouts, pieces spanning file
// boundaries, the piece sizes those torrents actually use) on the volume that
// actually holds them.
//
// It is read-only with respect to the library:
//
//   - it never calls Session.Start, so the persistence goroutine never runs
//   - it calls runVerification, not verifyResume, so finishVerify/SaveState never run
//   - it skips any torrent whose files are not all present at their declared size,
//     which is the only condition under which NewFileStorage would resize a file
//   - it copies every .<infohash>.state aside before touching anything and restores
//     them afterwards
//
// It lists what it found and stops. Set ST_LIB_RUN=1 to actually hash:
//
//	ST_LIB_RUN=1 ST_LIB_MAX_GIB=70 \
//	  go test -count=1 -run TestLibraryRecheckPolicies -timeout 180m ./pkg/downloader -v
//
// On macOS the page cache cannot be dropped from here, so run a corpus larger than
// RAM, or `sudo purge` between policies, or run one policy per invocation with
// ST_LIB_POLICY=current / ST_LIB_POLICY=pre108.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func defaultLibraryConfigDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("ST_LIB_CONFIG"); dir != "" {
		return dir
	}
	// Match how the app picks its state directory (see cmd/sainttorrent/main.go):
	// os.UserConfigDir, which is ~/Library/Application Support on macOS and
	// ~/.config on Linux. The ~/.config/sainttorrent path holds config.json and UI
	// preferences, which is a different directory on macOS.
	base, err := os.UserConfigDir()
	if err != nil {
		t.Skipf("cannot resolve user config directory: %v", err)
	}
	return filepath.Join(base, "sainttorrent")
}

// loadLibrary reads the persisted session and returns the torrents whose payload
// is fully present at the declared size, largest first. Anything else is reported
// as skipped rather than silently measured or, worse, resized.
func loadLibrary(t *testing.T, configDir string, maxBytes int64) (lib []benchTorrent, skipped []string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(configDir, "session.json"))
	if err != nil {
		t.Skipf("no library at %s: %v", configDir, err)
	}
	var persisted PersistedState
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("parse session.json: %v", err)
	}

	var cands []benchTorrent
	for _, entry := range persisted.Torrents {
		metaPath := filepath.Join(configDir, "torrents", entry.InfoHashHex+".torrent")
		metaRaw, err := os.ReadFile(metaPath)
		if err != nil {
			skipped = append(skipped, fmt.Sprintf("%s: no cached .torrent", entry.InfoHashHex[:12]))
			continue
		}
		tor, err := torrent.Parse(metaRaw)
		if err != nil {
			skipped = append(skipped, fmt.Sprintf("%s: unparseable .torrent: %v", entry.InfoHashHex[:12], err))
			continue
		}
		files := make([]storage.FileInfo, len(tor.Files))
		var total int64
		for i, f := range tor.Files {
			files[i] = storage.FileInfo{Path: filepath.Join(f.Path...), Length: f.Length}
			total += f.Length
		}
		// Only fully-present payloads. A short or missing file is the one case where
		// constructing storage would resize it, which would move its mtime and
		// invalidate the resume data this harness must leave alone.
		intact := true
		for _, f := range files {
			info, err := os.Stat(filepath.Join(entry.DownloadDir, f.Path))
			if err != nil || info.Size() != f.Length {
				intact = false
				break
			}
		}
		if !intact {
			skipped = append(skipped, fmt.Sprintf("%s (%s): payload incomplete or missing", short(tor.Name), entry.InfoHashHex[:12]))
			continue
		}
		cands = append(cands, benchTorrent{tor: tor, files: files, dir: entry.DownloadDir, bytes: total})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].bytes > cands[j].bytes })

	var running int64
	for _, c := range cands {
		if maxBytes > 0 && running+c.bytes > maxBytes {
			continue
		}
		running += c.bytes
		lib = append(lib, c)
	}
	return lib, skipped
}

func short(name string) string {
	if len(name) <= 48 {
		return name
	}
	return name[:45] + "..."
}

// guardResumeState copies every torrent's fast-resume file aside and restores it
// when the test ends, so even an unexpected write cannot cost the user a recheck.
func guardResumeState(t *testing.T, lib []benchTorrent) {
	t.Helper()
	backup := t.TempDir()
	type saved struct{ src, dst string }
	var files []saved
	for _, item := range lib {
		name := "." + fmt.Sprintf("%x", item.tor.InfoHash) + ".state"
		src := filepath.Join(item.dir, name)
		data, err := os.ReadFile(src)
		if err != nil {
			continue
		}
		dst := filepath.Join(backup, fmt.Sprintf("%x.state", item.tor.InfoHash))
		if err := os.WriteFile(dst, data, 0o600); err != nil {
			t.Fatalf("back up %s: %v", src, err)
		}
		files = append(files, saved{src: src, dst: dst})
	}
	t.Logf("backed up %d fast-resume files to %s", len(files), backup)
	t.Cleanup(func() {
		for _, f := range files {
			data, err := os.ReadFile(f.dst)
			if err != nil {
				continue
			}
			if err := os.WriteFile(f.src, data, 0o644); err != nil {
				t.Errorf("restore %s: %v", f.src, err)
			}
		}
	})
}

func TestLibraryRecheckPolicies(t *testing.T) {
	configDir := defaultLibraryConfigDir(t)
	maxBytes := int64(benchEnvInt("ST_LIB_MAX_GIB", 0)) << 30
	paused := benchEnvInt("ST_LIB_PAUSED", 1) == 1
	writeMBps := float64(benchEnvInt("ST_LIB_WRITE_MBPS", 0))
	only := os.Getenv("ST_LIB_POLICY")

	lib, skipped := loadLibrary(t, configDir, maxBytes)
	var total int64
	multiFile := 0
	pieceSizes := map[int64]int{}
	for _, item := range lib {
		total += item.bytes
		if len(item.files) > 1 {
			multiFile++
		}
		pieceSizes[item.tor.PieceLength>>10]++
	}

	t.Logf("library: %s", configDir)
	t.Logf("selected %d torrents, %.2f GiB (%d multi-file), cpus=%d, cache-eviction=%v",
		len(lib), float64(total)/(1<<30), multiFile, runtime.NumCPU(), cacheEvictionSupported)
	var sizes []string
	for k, v := range pieceSizes {
		sizes = append(sizes, fmt.Sprintf("%dKiB x%d", k, v))
	}
	sort.Strings(sizes)
	t.Logf("piece sizes: %s", strings.Join(sizes, ", "))
	for _, s := range skipped {
		t.Logf("  skipped %s", s)
	}
	if len(lib) == 0 {
		t.Skip("no fully-present torrents to measure")
	}

	if os.Getenv("ST_LIB_RUN") != "1" {
		t.Log("")
		t.Log("dry run: set ST_LIB_RUN=1 to hash the above. Nothing was read or written.")
		return
	}

	guardResumeState(t, lib)

	for _, policy := range recheckPolicies() {
		if only != "" && only != policy.name {
			continue
		}
		t.Run(policy.name, func(t *testing.T) {
			policy.apply(t)
			evictLibrary(lib)
			liveTransfers.Store(0)
			w := startRateWriter(t, lib[0].dir, writeMBps)
			if writeMBps > 0 {
				liveTransfers.Store(1)
			}
			elapsed := recheckAll(t, lib, paused)
			liveTransfers.Store(0)
			report(t, policy.name, total, elapsed, w)
		})
	}
}
