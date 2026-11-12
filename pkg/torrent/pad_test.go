package torrent

import (
	"crypto/sha1"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// padInfoFile is one entry of a test info dict's file list.
type padInfoFile struct {
	length int64
	path   []string
	attr   string
}

// padInfo returns a multi-file info dict named "root" with the given files and
// piece length. hashes overrides the piece hashes; nil leaves them all zero
// bytes (which are not the hash of anything).
func padInfo(pieceLength int64, hashes [][20]byte, files ...padInfoFile) map[string]interface{} {
	var total int64
	list := make([]interface{}, 0, len(files))
	for _, f := range files {
		comps := make([]interface{}, len(f.path))
		for i, c := range f.path {
			comps[i] = c
		}
		entry := map[string]interface{}{"length": f.length, "path": comps}
		if f.attr != "" {
			entry["attr"] = f.attr
		}
		list = append(list, entry)
		total += f.length
	}
	numPieces := (total + pieceLength - 1) / pieceLength
	pieces := make([]byte, 20*numPieces)
	for i, h := range hashes {
		copy(pieces[20*i:], h[:])
	}
	return map[string]interface{}{
		"name":         "root",
		"piece length": pieceLength,
		"pieces":       string(pieces),
		"files":        list,
	}
}

// pad returns a pad file named the way libtorrent 2.x names them.
func pad(length int64) padInfoFile {
	return padInfoFile{length: length, path: []string{".pad", strconv.FormatInt(length, 10)}, attr: "p"}
}

func zeroHash(length int) [20]byte {
	return sha1.Sum(make([]byte, length))
}

// TestParseHybridPadFiles is the issue #115 case: libtorrent 2.x names every
// pad file ".pad/<size>", so a hybrid or aligned torrent repeats the name for
// equal sizes. Such a torrent must load, with the pad files flagged.
func TestParseHybridPadFiles(t *testing.T) {
	const pieceLength = 32 << 10
	info := padInfo(pieceLength, nil,
		padInfoFile{length: 40000, path: []string{"a.bin"}, attr: "x"},
		pad(2*pieceLength-40000),
		padInfoFile{length: 40000, path: []string{"dir", "b.bin"}, attr: "hx"},
		pad(2*pieceLength-40000),
		padInfoFile{length: 100, path: []string{"c.bin"}, attr: "l?"},
	)
	for name, parse := range map[string]func() (*Torrent, error){
		"Parse":     func() (*Torrent, error) { return Parse(marshalTorrent(t, info)) },
		"ParseInfo": func() (*Torrent, error) { return ParseInfo(marshalInfo(t, info)) },
	} {
		tor, err := parse()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(tor.Files) != 5 {
			t.Fatalf("%s: parsed %d files, want 5", name, len(tor.Files))
		}
		for i, wantPad := range []bool{false, true, false, true, false} {
			if tor.Files[i].Pad != wantPad {
				t.Errorf("%s: file %d Pad = %v, want %v", name, i, tor.Files[i].Pad, wantPad)
			}
		}
		if !reflect.DeepEqual(tor.Files[1].Path, tor.Files[3].Path) {
			t.Errorf("%s: pad paths %q and %q differ; the test needs them to collide", name, tor.Files[1].Path, tor.Files[3].Path)
		}
		if f := tor.Files[0]; !f.Exec || f.Hidden || f.Symlink {
			t.Errorf("%s: a.bin flags exec=%v hidden=%v symlink=%v, want x only", name, f.Exec, f.Hidden, f.Symlink)
		}
		if f := tor.Files[2]; !f.Exec || !f.Hidden || f.Symlink {
			t.Errorf("%s: b.bin flags exec=%v hidden=%v symlink=%v, want x and h", name, f.Exec, f.Hidden, f.Symlink)
		}
		if f := tor.Files[4]; f.Exec || f.Hidden || !f.Symlink || f.Pad {
			t.Errorf("%s: c.bin flags %+v, want only l (unknown flags ignored)", name, f)
		}
		want := []PadSpan{{40000, 2 * pieceLength}, {2*pieceLength + 40000, 4 * pieceLength}}
		if got := PadSpans(tor.Files); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: PadSpans = %v, want %v", name, got, want)
		}
	}
}

// TestParsePadExemptionIsNarrow: only pad files escape the path checks.
// Payload files that collide are refused exactly as before, even in a torrent
// that also has pad files.
func TestParsePadExemptionIsNarrow(t *testing.T) {
	cases := map[string][]padInfoFile{
		"duplicate payload path": {
			{length: 10, path: []string{"a"}}, pad(100), {length: 10, path: []string{"a"}},
		},
		"case-folded payload duplicate": {
			{length: 10, path: []string{"A"}}, pad(100), {length: 10, path: []string{"a"}},
		},
		"payload file is also a directory": {
			{length: 10, path: []string{"a"}}, pad(100), {length: 10, path: []string{"a", "b"}},
		},
		"payload duplicates a pad file's path": {
			pad(100), {length: 10, path: []string{".pad", "100"}}, {length: 10, path: []string{".pad", "100"}},
		},
	}
	for name, files := range cases {
		if _, err := ParseInfo(marshalInfo(t, padInfo(16<<10, nil, files...))); err == nil {
			t.Errorf("%s: ParseInfo accepted it", name)
		}
	}

	// A payload file may share a path with a pad file: the pad file is never
	// created, so nothing collides on disk.
	if _, err := ParseInfo(marshalInfo(t, padInfo(16<<10, nil,
		padInfoFile{length: 10, path: []string{".pad", "100"}}, pad(100)))); err != nil {
		t.Errorf("payload file at a pad file's path: %v", err)
	}
	// A pad file has no legacy path to migrate.
	tor, err := ParseInfo(marshalInfo(t, padInfo(16<<10, nil,
		padInfoFile{length: 10, path: []string{"a"}}, padInfoFile{length: 100, path: []string{".pad", "bad:name"}, attr: "p"})))
	if err != nil {
		t.Fatal(err)
	}
	if tor.Files[1].LegacyPath != nil {
		t.Errorf("pad file LegacyPath = %q, want none", tor.Files[1].LegacyPath)
	}
}

// TestParseRefusesPadPieceThatCannotVerify: a piece lying entirely in pad files
// is complete from the start, so its hash must be that of zeros or the torrent
// could never finish. Such a torrent is refused rather than left stuck.
func TestParseRefusesPadPieceThatCannotVerify(t *testing.T) {
	const pieceLength = 16 << 10
	// Pieces 1 and 2 lie entirely in the middle pad file.
	middle := []padInfoFile{{length: pieceLength, path: []string{"a"}}, pad(2 * pieceLength), {length: 100, path: []string{"b"}}}
	zero := zeroHash(pieceLength)
	if _, err := ParseInfo(marshalInfo(t, padInfo(pieceLength, [][20]byte{{1}, zero, zero, {2}}, middle...))); err != nil {
		t.Fatalf("zero-hash pad pieces: %v", err)
	}
	_, err := ParseInfo(marshalInfo(t, padInfo(pieceLength, [][20]byte{{1}, zero, {9}, {2}}, middle...)))
	if err == nil || !strings.Contains(err.Error(), "piece 2") || !strings.Contains(err.Error(), "pad") {
		t.Fatalf("ParseInfo = %v, want piece 2 refused", err)
	}
	if _, err := Parse(marshalTorrent(t, padInfo(pieceLength, [][20]byte{{1}, zero, {9}, {2}}, middle...))); err == nil {
		t.Fatal("Parse accepted a pad piece that cannot verify")
	}

	// The short last piece lies entirely in tail padding.
	tail := []padInfoFile{{length: pieceLength, path: []string{"a"}}, pad(10000)}
	if _, err := ParseInfo(marshalInfo(t, padInfo(pieceLength, [][20]byte{{1}, zeroHash(10000)}, tail...))); err != nil {
		t.Fatalf("zero-hash short tail piece: %v", err)
	}
	if _, err := ParseInfo(marshalInfo(t, padInfo(pieceLength, [][20]byte{{1}, zero}, tail...))); err == nil {
		t.Fatal("accepted a short pad piece hashed at the full piece length")
	}

	// Adjacent pad files merge: neither covers piece 1 alone.
	split := []padInfoFile{{length: pieceLength, path: []string{"a"}}, pad(pieceLength / 2), pad(pieceLength / 2), {length: 1, path: []string{"b"}}}
	if _, err := ParseInfo(marshalInfo(t, padInfo(pieceLength, [][20]byte{{1}, {9}, {2}}, split...))); err == nil {
		t.Fatal("accepted a pad piece made of two pad files whose hash is not that of zeros")
	}

	// Ordinary libtorrent padding, shorter than a piece, places no constraint.
	partial := []padInfoFile{{length: 100, path: []string{"a"}}, pad(pieceLength - 100), {length: 100, path: []string{"b"}}}
	if _, err := ParseInfo(marshalInfo(t, padInfo(pieceLength, [][20]byte{{1}, {2}}, partial...))); err != nil {
		t.Fatalf("partial pad piece: %v", err)
	}
}

// TestParseDegeneratePadTorrents: "p" means nothing on a single-file torrent,
// whose one file is the payload, and a multi-file torrent made only of pad
// files has no payload to download.
func TestParseDegeneratePadTorrents(t *testing.T) {
	info := singleFileInfo("only.bin", 16<<10, 1000)
	info["attr"] = "px"
	tor, err := ParseInfo(marshalInfo(t, info))
	if err != nil {
		t.Fatalf("single-file torrent with attr p: %v", err)
	}
	if f := tor.Files[0]; f.Pad || !f.Exec {
		t.Fatalf("single file Pad=%v Exec=%v, want p ignored and x kept", f.Pad, f.Exec)
	}

	for name, files := range map[string][]padInfoFile{
		"only pad files":             {pad(1000), pad(2000)},
		"pad and empty payload file": {{length: 0, path: []string{"empty"}}, pad(1000)},
	} {
		_, err := ParseInfo(marshalInfo(t, padInfo(16<<10, [][20]byte{zeroHash(3000)}, files...)))
		if err == nil || !strings.Contains(err.Error(), "no payload") {
			t.Errorf("%s: ParseInfo = %v, want a no-payload refusal", name, err)
		}
	}
}
