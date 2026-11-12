package storage

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// padLayout is [file A | pad | file B | pad]: two payload files, each padded to
// a piece boundary the way libtorrent aligns them, so the trailing pad covers
// the end of the torrent. Both pad files carry the same name, as libtorrent's
// ".pad/<size>" names do for equal sizes; storage must not care.
const padPieceLength = 16

func padLayoutFiles() []FileInfo {
	return []FileInfo{
		{Path: filepath.Join("t", "a.bin"), Length: 20},
		{Path: filepath.Join("t", ".pad", "12"), Length: 12, Pad: true},
		{Path: filepath.Join("t", "b.bin"), Length: 21},
		{Path: filepath.Join("t", ".pad", "12"), Length: 11, Pad: true},
	}
}

// padLayoutContent returns the torrent bytes: payload where the files are and
// zeros in the pad ranges.
func padLayoutContent() []byte {
	content := make([]byte, 64)
	for i := 0; i < 20; i++ {
		content[i] = byte(0x10 + i)
	}
	for i := 32; i < 53; i++ {
		content[i] = byte(0x40 + i)
	}
	return content
}

func padIsPayload(off int) bool { return off < 20 || (off >= 32 && off < 53) }

type padBackend struct {
	name string
	new  func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error)
}

func padBackends(t *testing.T) []padBackend {
	backends := []padBackend{
		{"file", func(d string, f []FileInfo, p int64) (Storage, error) {
			return NewStorageWithBackend(BackendFile, d, f, p)
		}},
		{"mem", func(d string, f []FileInfo, p int64) (Storage, error) {
			return NewStorageWithBackend(BackendMemory, d, f, p)
		}},
	}
	if _, err := FactoryForBackend(BackendMMap); err == nil {
		backends = append(backends, padBackend{"mmap", func(d string, f []FileInfo, p int64) (Storage, error) {
			return NewStorageWithBackend(BackendMMap, d, f, p)
		}})
	} else {
		t.Logf("mmap backend unavailable here: %v", err)
	}
	return backends
}

// TestPadRangesAcrossBackends: pad ranges read as zeros whatever the caller's
// buffer held, writes into them are dropped, verification hashes zeros for
// them, and nothing is created on disk for them.
func TestPadRangesAcrossBackends(t *testing.T) {
	content := padLayoutContent()
	for _, backend := range padBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			base := t.TempDir()
			st, err := backend.new(base, padLayoutFiles(), padPieceLength)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if got := st.TotalSize(); got != int64(len(content)) {
				t.Fatalf("TotalSize = %d, want %d", got, len(content))
			}
			numPieces := len(content) / padPieceLength

			// A freshly created storage already reads its pad ranges as zeros.
			for piece := 0; piece < numPieces; piece++ {
				buf := bytes.Repeat([]byte{0xAA}, padPieceLength)
				if _, err := st.ReadBlock(int64(piece), 0, buf); err != nil {
					t.Fatal(err)
				}
				for i, b := range buf {
					if off := piece*padPieceLength + i; !padIsPayload(off) && b != 0 {
						t.Fatalf("fresh read: pad byte %d = %#x, want 0", off, b)
					}
				}
			}

			// Write every piece with garbage where the pad bytes go, in blocks
			// that straddle the payload/pad boundaries.
			for piece := 0; piece < numPieces; piece++ {
				data := append([]byte(nil), content[piece*padPieceLength:(piece+1)*padPieceLength]...)
				for i := range data {
					if !padIsPayload(piece*padPieceLength + i) {
						data[i] = 0xEE
					}
				}
				for _, cut := range [][2]int{{0, 3}, {3, 11}, {11, 16}} {
					if err := st.WriteBlock(int64(piece), int64(cut[0]), data[cut[0]:cut[1]]); err != nil {
						t.Fatalf("WriteBlock(%d, %d): %v", piece, cut[0], err)
					}
				}
			}

			// Every range of every piece, aligned or not, reads back the payload
			// with zeros in the pad ranges, into a buffer that held 0xAA.
			for piece := 0; piece < numPieces; piece++ {
				for off := 0; off < padPieceLength; off++ {
					for end := off + 1; end <= padPieceLength; end++ {
						buf := bytes.Repeat([]byte{0xAA}, end-off)
						n, err := st.ReadBlock(int64(piece), int64(off), buf)
						if err != nil || n != len(buf) {
							t.Fatalf("ReadBlock(%d, %d, %d) = %d, %v", piece, off, len(buf), n, err)
						}
						want := content[piece*padPieceLength+off : piece*padPieceLength+end]
						if !bytes.Equal(buf, want) {
							t.Fatalf("ReadBlock(%d, %d, %d) = %x, want %x", piece, off, len(buf), buf, want)
						}
					}
				}
				hash := sha1.Sum(content[piece*padPieceLength : (piece+1)*padPieceLength])
				if ok, err := st.VerifyPiece(int64(piece), hash); err != nil || !ok {
					t.Fatalf("VerifyPiece(%d) = %v, %v; want the zero-padded hash to match", piece, ok, err)
				}
				garbage := append([]byte(nil), content[piece*padPieceLength:(piece+1)*padPieceLength]...)
				for i := range garbage {
					if !padIsPayload(piece*padPieceLength + i) {
						garbage[i] = 0xEE
					}
				}
				if piece == 1 || piece == 3 {
					if ok, _ := st.VerifyPiece(int64(piece), sha1.Sum(garbage)); ok {
						t.Fatalf("VerifyPiece(%d) matched the garbage written into its pad range", piece)
					}
				}
			}

			if backend.name != "mem" {
				assertOnlyPayloadOnDisk(t, base, map[string]int64{"t/a.bin": 20, "t/b.bin": 21})
			}
		})
	}
}

// assertOnlyPayloadOnDisk fails unless the regular files under base are
// exactly want (slash paths to sizes), and nothing named .pad exists.
func assertOnlyPayloadOnDisk(t *testing.T, base string, want map[string]int64) {
	t.Helper()
	got := map[string]int64{}
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(base, path)
		rel = filepath.ToSlash(rel)
		if d.Name() == ".pad" || strings.Contains(rel, "/.pad/") {
			t.Errorf("pad path created on disk: %s", rel)
		}
		if d.Type().IsRegular() && !strings.HasSuffix(rel, ".state") {
			info, err := d.Info()
			if err != nil {
				return err
			}
			got[rel] = info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload on disk = %v, want %v", got, want)
	}
}

// TestPadResumeRoundTrip: resume state records the payload files only, and
// round-trips through both the checkpoint and the legacy state format.
func TestPadResumeRoundTrip(t *testing.T) {
	content := padLayoutContent()
	const hash = "0123456789abcdef0123456789abcdef01234567"
	all := []int{0, 1, 2, 3}
	for _, backend := range padBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			base := t.TempDir()
			st, err := backend.new(base, padLayoutFiles(), padPieceLength)
			if err != nil {
				t.Fatal(err)
			}
			for piece := 0; piece < 4; piece++ {
				if err := st.WriteBlock(int64(piece), 0, content[piece*padPieceLength:(piece+1)*padPieceLength]); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.SaveState(hash, all); err != nil {
				t.Fatal(err)
			}
			if got, err := st.LoadState(hash); err != nil || !reflect.DeepEqual(got, all) {
				t.Fatalf("LoadState = %v, %v; want %v", got, err, all)
			}
			checkpoint, ok := st.(ResumeStorage)
			if !ok {
				_ = st.Close()
				return // mem keeps only the legacy format
			}
			if err := checkpoint.SaveResumeState(hash, all, nil, true); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}

			data, err := os.ReadFile(filepath.Join(base, "."+hash+".state"))
			if err != nil {
				t.Fatal(err)
			}
			var state FastResumeState
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			var paths []string
			for _, f := range state.Files {
				paths = append(paths, filepath.ToSlash(f.Path))
			}
			if want := []string{"t/a.bin", "t/b.bin"}; !reflect.DeepEqual(paths, want) || len(state.Identities) != 2 {
				t.Fatalf("checkpoint files %v with %d identities, want %v with 2", paths, len(state.Identities), want)
			}

			reopened, err := backend.new(base, padLayoutFiles(), padPieceLength)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			result, err := reopened.(ResumeStorage).LoadResumeState(hash)
			if err != nil {
				t.Fatal(err)
			}
			if want := (ResumeState{Verified: all}); !reflect.DeepEqual(result, want) {
				t.Fatalf("LoadResumeState = %+v, want %+v", result, want)
			}
			assertOnlyPayloadOnDisk(t, base, map[string]int64{"t/a.bin": 20, "t/b.bin": 21})
		})
	}
}

// TestPadResumeFromBuildThatWrotePadFiles is the upgrade path: a build without
// BEP 47 support created the pad files as real files and recorded them in the
// resume state. Loading that state now must fail cleanly, which the downloader
// turns into a recheck of the untouched payload, and the old pad files are left
// where they are.
func TestPadResumeFromBuildThatWrotePadFiles(t *testing.T) {
	content := padLayoutContent()
	const hash = "89abcdef0123456789abcdef0123456789abcdef"
	all := []int{0, 1, 2, 3}
	// The old build refused repeated names, so these pad files are distinct.
	old := []FileInfo{
		{Path: filepath.Join("t", "a.bin"), Length: 20},
		{Path: filepath.Join("t", ".pad", "12"), Length: 12},
		{Path: filepath.Join("t", "b.bin"), Length: 21},
		{Path: filepath.Join("t", ".pad", "11"), Length: 11},
	}
	current := append([]FileInfo(nil), old...)
	current[1].Pad, current[3].Pad = true, true

	base := t.TempDir()
	st, err := NewFileStorage(base, old, padPieceLength)
	if err != nil {
		t.Fatal(err)
	}
	for piece := 0; piece < 4; piece++ {
		if err := st.WriteBlock(int64(piece), 0, content[piece*padPieceLength:(piece+1)*padPieceLength]); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SaveResumeState(hash, all, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveState("legacy"+hash[6:], all); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	for _, backend := range padBackends(t) {
		if backend.name == "mem" {
			continue // in-memory state does not survive a restart
		}
		t.Run(backend.name, func(t *testing.T) {
			reopened, err := backend.new(base, current, padPieceLength)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if result, err := reopened.(ResumeStorage).LoadResumeState(hash); err == nil || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("LoadResumeState = %+v, %v; want an incompatible-state error", result, err)
			}
			if got, err := reopened.LoadState("legacy" + hash[6:]); err == nil {
				t.Fatalf("LoadState = %v, want a file count mismatch", got)
			}
			// The recheck the downloader then runs finds the payload intact.
			for piece := 0; piece < 4; piece++ {
				hash := sha1.Sum(content[piece*padPieceLength : (piece+1)*padPieceLength])
				if ok, err := reopened.VerifyPiece(int64(piece), hash); err != nil || !ok {
					t.Fatalf("VerifyPiece(%d) = %v, %v", piece, ok, err)
				}
			}
		})
	}

	var left []string
	for _, name := range []string{"12", "11"} {
		if info, err := os.Stat(filepath.Join(base, "t", ".pad", name)); err == nil && info.Size() > 0 {
			left = append(left, name)
		}
	}
	sort.Strings(left)
	if !reflect.DeepEqual(left, []string{"11", "12"}) {
		t.Fatalf("old pad files left on disk: %v, want both untouched", left)
	}
}
