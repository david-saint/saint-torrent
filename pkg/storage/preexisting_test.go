package storage

import (
	"bytes"
	"crypto/sha1"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// preexistingFixture writes a 9000-byte user file where a torrent is about to
// declare a 16-byte one.
func preexistingFixture(t *testing.T) (dir string, original []byte) {
	t.Helper()
	dir = t.TempDir()
	original = bytes.Repeat([]byte("user data "), 900)
	if err := os.WriteFile(filepath.Join(dir, "thesis.docx"), original, 0644); err != nil {
		t.Fatal(err)
	}
	return dir, original
}

func requireUserTail(t *testing.T, dir string, original []byte) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, "thesis.docx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(original) || !bytes.Equal(got[16:], original[16:]) {
		t.Fatalf("pre-existing file is %d bytes with tail intact=%v; want %d bytes, tail untouched", len(got), len(got) >= 16 && bytes.Equal(got[16:], original[16:]), len(original))
	}
}

// TestNewFileStorageNeverShrinksExistingFile: adding a torrent that names a
// file already in the download directory truncated it to the declared length
// at once, before any piece had been received or verified, destroying
// everything past it. The file is now only ever grown, block I/O stays inside
// the declared length, and the size mismatch keeps resume from trusting it.
func TestNewFileStorageNeverShrinksExistingFile(t *testing.T) {
	dir, original := preexistingFixture(t)
	files := []FileInfo{{Path: "thesis.docx", Length: 16}}
	st, err := NewFileStorage(dir, files, 16)
	if err != nil {
		t.Fatal(err)
	}
	requireUserTail(t, dir, original)

	piece := bytes.Repeat([]byte{'z'}, 16)
	if err := st.WriteBlock(0, 0, piece); err != nil {
		t.Fatalf("WriteBlock over a longer pre-existing file: %v", err)
	}
	got := make([]byte, 16)
	if _, err := st.ReadBlock(0, 0, got); err != nil || !bytes.Equal(got, piece) {
		t.Fatalf("ReadBlock = %q, %v; want %q", got, err, piece)
	}
	if ok, err := st.VerifyPiece(0, sha1.Sum(piece)); err != nil || !ok {
		t.Fatalf("VerifyPiece = %v, %v; want true", ok, err)
	}
	requireUserTail(t, dir, original)

	const hash = "preexisting"
	if err := st.SaveResumeState(hash, []int{0}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = NewFileStorage(dir, files, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	result, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ResumeState{Recheck: []int{0}}); !reflect.DeepEqual(result, want) {
		t.Fatalf("LoadResumeState = %+v, want %+v: a file whose size differs from the torrent must be rehashed", result, want)
	}
	requireUserTail(t, dir, original)
}

// TestNewFileStorageGrowsShortExistingFile keeps the sparse pre-allocation of a
// file shorter than declared.
func TestNewFileStorageGrowsShortExistingFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("abc"), 0644); err != nil {
		t.Fatal(err)
	}
	st, err := NewFileStorage(dir, []FileInfo{{Path: "a", Length: 32}}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if fi, err := os.Stat(filepath.Join(dir, "a")); err != nil || fi.Size() != 32 {
		t.Fatalf("short pre-existing file: size %v, err %v; want 32", fi, err)
	}
}

// TestNewFileStorageAcceptsPreexistingHardLinks: dedup tools (rdfind, jdupes
// -L) replace identical payload files with hard links to one file. The check
// that catches two layouts opening one file refused such a torrent outright,
// so it could no longer be restored. Hard links that already exist now restore
// normally on every backend built on NewFileStorage: both layouts read and
// verify the same bytes, a checkpoint trusts them, and the links survive.
func TestNewFileStorageAcceptsPreexistingHardLinks(t *testing.T) {
	payload := []byte("identical track!")
	sum := sha1.Sum(payload)
	first := filepath.Join("disc1", "track.flac")
	second := filepath.Join("disc2", "track.flac")
	// The file between the two links is created by the first open, so a created
	// file and a pre-existing pair meet in one call.
	files := []FileInfo{{Path: first, Length: 16}, {Path: "notes.txt", Length: 16}, {Path: second, Length: 16}}
	for _, backend := range []Backend{BackendFile, BackendMMap} {
		t.Run(string(backend), func(t *testing.T) {
			if _, err := FactoryForBackend(backend); err != nil {
				t.Skipf("%s backend unavailable: %v", backend, err)
			}
			dir := t.TempDir()
			for _, name := range []string{first, second} {
				if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, first), payload, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(filepath.Join(dir, first), filepath.Join(dir, second)); err != nil {
				t.Skipf("hard links unavailable: %v", err)
			}

			const hash = "hardlinked"
			for run := 0; run < 2; run++ {
				st, err := NewStorageWithBackend(backend, dir, files, 16)
				if err != nil {
					t.Fatalf("run %d: NewStorage over pre-existing hard links = %v, want it accepted", run, err)
				}
				resume, ok := st.(ResumeStorage)
				if run == 1 && ok {
					got, err := resume.LoadResumeState(hash)
					if want := (ResumeState{Verified: []int{0, 1, 2}}); err != nil || !reflect.DeepEqual(got, want) {
						t.Errorf("LoadResumeState after restart = %+v, %v; want %+v", got, err, want)
					}
				}
				for _, piece := range []int64{0, 2} {
					got := make([]byte, len(payload))
					if n, err := st.ReadBlock(piece, 0, got); err != nil || n != len(payload) || !bytes.Equal(got, payload) {
						t.Errorf("run %d: ReadBlock(%d) = %q, %d, %v; want %q", run, piece, got, n, err, payload)
					}
					if ok, err := st.VerifyPiece(piece, sum); err != nil || !ok {
						t.Errorf("run %d: VerifyPiece(%d) = %v, %v; want true", run, piece, ok, err)
					}
				}
				if run == 0 && ok {
					if err := resume.SaveResumeState(hash, []int{0, 1, 2}, nil, true); err != nil {
						t.Fatal(err)
					}
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
			}

			a, errA := os.Stat(filepath.Join(dir, first))
			b, errB := os.Stat(filepath.Join(dir, second))
			if errA != nil || errB != nil || !os.SameFile(a, b) || a.Size() != int64(len(payload)) {
				t.Fatalf("hard links after restore: %v, %v; want one %d-byte file under both names", errA, errB, len(payload))
			}
		})
	}
}
