package torrent

import (
	"bytes"
	"crypto/sha1"
	"strings"
	"testing"

	"sainttorrent/pkg/bencode"
)

// singleFileInfo returns a single-file info dict with one piece hash per
// pieceLength-sized chunk of length.
func singleFileInfo(name string, pieceLength, length int64) map[string]interface{} {
	pieces := (length-1)/pieceLength + 1
	return map[string]interface{}{
		"name":         name,
		"piece length": pieceLength,
		"pieces":       string(make([]byte, 20*pieces)),
		"length":       length,
	}
}

func marshalInfo(t *testing.T, info map[string]interface{}) []byte {
	t.Helper()
	data, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("marshal info: %v", err)
	}
	return data
}

func marshalTorrent(t *testing.T, info map[string]interface{}) []byte {
	t.Helper()
	data, err := bencode.Marshal(map[string]interface{}{"info": info})
	if err != nil {
		t.Fatalf("marshal torrent: %v", err)
	}
	return data
}

// TestParseRejectsOversizedPieceLength covers the process-crash class: Parse
// accepted any positive piece length, and piece buffers are allocated at that
// size, so a one-piece torrent with a 2^40 or 2^62 piece length killed the
// client (unrecoverable OOM or makeslice panic) on the first piece, again on
// every restart because the cached .torrent is re-added.
func TestParseRejectsOversizedPieceLength(t *testing.T) {
	for _, pieceLength := range []int64{MaxPieceLength + 1, 1 << 31, 1 << 40, 1 << 62, 1<<63 - 1} {
		data := marshalTorrent(t, singleFileInfo("huge.bin", pieceLength, 16384))
		if _, err := Parse(data); err == nil || !strings.Contains(err.Error(), "exceeds the maximum") {
			t.Errorf("Parse(piece length %d) = %v, want piece length rejected", pieceLength, err)
		}
	}
	// The cap itself is a legitimate (if large) piece length.
	tor, err := Parse(marshalTorrent(t, singleFileInfo("big.bin", MaxPieceLength, 3*MaxPieceLength)))
	if err != nil {
		t.Fatalf("Parse(piece length %d) = %v, want success", MaxPieceLength, err)
	}
	if tor.PieceLength != MaxPieceLength || len(tor.PieceHashes) != 3 {
		t.Fatalf("PieceLength=%d pieces=%d, want %d and 3", tor.PieceLength, len(tor.PieceHashes), MaxPieceLength)
	}
}

func TestParseRejectsOversizedFileLengths(t *testing.T) {
	const pieceLength = MaxPieceLength
	t.Run("single file", func(t *testing.T) {
		// One hash short of matching is fine here: the length check runs first.
		info := singleFileInfo("big.bin", pieceLength, pieceLength)
		info["length"] = MaxTotalLength + 1
		if _, err := Parse(marshalTorrent(t, info)); err == nil || !strings.Contains(err.Error(), "exceeds the maximum") {
			t.Fatalf("Parse(file length %d) = %v, want rejection", MaxTotalLength+1, err)
		}
	})
	t.Run("one file too large", func(t *testing.T) {
		info := singleFileInfo("root", pieceLength, pieceLength)
		delete(info, "length")
		info["files"] = []interface{}{
			map[string]interface{}{"length": MaxTotalLength + 1, "path": []interface{}{"a"}},
		}
		if _, err := Parse(marshalTorrent(t, info)); err == nil || !strings.Contains(err.Error(), "exceeds the maximum") {
			t.Fatalf("Parse(file of %d bytes) = %v, want rejection", MaxTotalLength+1, err)
		}
	})
	t.Run("sum too large", func(t *testing.T) {
		info := singleFileInfo("root", pieceLength, pieceLength)
		delete(info, "length")
		info["files"] = []interface{}{
			map[string]interface{}{"length": MaxTotalLength, "path": []interface{}{"a"}},
			map[string]interface{}{"length": int64(1), "path": []interface{}{"b"}},
		}
		if _, err := Parse(marshalTorrent(t, info)); err == nil || !strings.Contains(err.Error(), "total length exceeds") {
			t.Fatalf("Parse(total %d) = %v, want rejection", MaxTotalLength+1, err)
		}
	})
}

// TestCheckPieceCount exercises the piece-count cap directly; a torrent that
// actually carries MaxPieceCount+1 hashes would be an 80 MiB test fixture.
func TestCheckPieceCount(t *testing.T) {
	const pieceLength = 16 << 10
	if err := checkPieceCount(MaxPieceCount, pieceLength, MaxPieceCount*pieceLength); err != nil {
		t.Fatalf("checkPieceCount(MaxPieceCount) = %v, want success", err)
	}
	if err := checkPieceCount(MaxPieceCount+1, pieceLength, (MaxPieceCount+1)*pieceLength); err == nil || !strings.Contains(err.Error(), "more than the maximum") {
		t.Fatalf("checkPieceCount(MaxPieceCount+1) = %v, want cap rejection", err)
	}
	if err := checkPieceCount(2, pieceLength, pieceLength); err == nil {
		t.Fatal("checkPieceCount(mismatch) = nil, want error")
	}
}

// TestCheckFileCount exercises the file-count cap directly; a torrent that
// actually lists MaxFileCount+1 files would be a 20 MiB fixture that decodes
// into a million maps.
func TestCheckFileCount(t *testing.T) {
	if MaxFileCount < 1<<20 {
		t.Fatalf("MaxFileCount = %d, want at least 1<<20 so large datasets still load", MaxFileCount)
	}
	if err := checkFileCount(MaxFileCount); err != nil {
		t.Fatalf("checkFileCount(MaxFileCount) = %v, want success", err)
	}
	if err := checkFileCount(MaxFileCount + 1); err == nil || !strings.Contains(err.Error(), "more than the maximum") {
		t.Fatalf("checkFileCount(MaxFileCount+1) = %v, want cap rejection", err)
	}
}

// TestParsePieceCountComparedIn64Bits is the GOARCH=386 regression: the
// expected count used to be converted to int before comparing, so 2^32+1
// expected pieces truncated to 1 and matched a single hash. CI runs this test
// on linux/386 too.
func TestParsePieceCountComparedIn64Bits(t *testing.T) {
	for _, tc := range []struct {
		pieceLength, length int64
	}{
		{1, 1<<32 + 1},                             // 4 GiB + 1 behind one 1-byte piece
		{16384, (1<<32)*16384 + 1},                 // 64 TiB + 1 behind one 16 KiB piece
		{MaxPieceLength, 1<<32*MaxPieceLength + 1}, // beyond MaxTotalLength anyway
	} {
		info := singleFileInfo("wrap.bin", tc.pieceLength, 1)
		info["length"] = tc.length
		if _, err := Parse(marshalTorrent(t, info)); err == nil {
			t.Errorf("Parse(piece length %d, length %d, 1 hash) = nil, want rejection", tc.pieceLength, tc.length)
		}
	}
}

func TestParseRejectsDeepPaths(t *testing.T) {
	build := func(depth int) []byte {
		path := make([]interface{}, depth)
		for i := range path {
			path[i] = "d"
		}
		info := singleFileInfo("root", 16, 1)
		delete(info, "length")
		info["files"] = []interface{}{map[string]interface{}{"length": int64(1), "path": path}}
		return marshalTorrent(t, info)
	}
	if _, err := Parse(build(maxPathDepth)); err != nil {
		t.Fatalf("Parse(depth %d) = %v, want success", maxPathDepth, err)
	}
	if _, err := Parse(build(maxPathDepth + 1)); err == nil || !strings.Contains(err.Error(), "components") {
		t.Fatalf("Parse(depth %d) = %v, want depth rejection", maxPathDepth+1, err)
	}
}

// TestParseRejectsDuplicateKeys covers the info-hash/content split: with two
// "info" keys the info-hash came from the first dict while name, files and
// piece hashes came from the second, and a repeated key inside one info dict
// made this client read a different file name than libtorrent-based clients.
func TestParseRejectsDuplicateKeys(t *testing.T) {
	good := marshalInfo(t, singleFileInfo("good.txt", 16, 16))
	evil := marshalInfo(t, singleFileInfo("malware.exe", 16, 16))
	concat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

	for name, data := range map[string][]byte{
		"two info dicts":           concat([]byte("d4:info"), good, []byte("4:info"), evil, []byte("e")),
		"two info dicts, non-dict": concat([]byte("d4:info"), good, []byte("4:infoi1ee")),
		"repeated name in info":    concat([]byte("d4:infod6:lengthi16e4:name9:movie.mkv4:name13:movie.mkv.exe12:piece lengthi16e6:pieces20:"), make([]byte, 20), []byte("ee")),
		"repeated key in info file": concat([]byte("d4:infod5:filesld6:lengthi16e4:pathl1:ae4:pathl1:beee4:name1:x12:piece lengthi16e6:pieces20:"),
			make([]byte, 20), []byte("ee")),
	} {
		if _, err := Parse(data); err == nil || !strings.Contains(err.Error(), "duplicate dictionary key") {
			t.Errorf("%s: Parse() = %v, want duplicate key rejection", name, err)
		}
	}
}

// TestParseAcceptsRepeatedOuterKeys: the whole .torrent was decoded strictly,
// so a key repeated outside the info dict, such as the second "comment" or
// "announce" a torrent editor appends, made the torrent unloadable, and a
// cached copy of it failed to restore. Those keys are not hashed; libtorrent
// keeps the first value, and so does Parse.
func TestParseAcceptsRepeatedOuterKeys(t *testing.T) {
	info := marshalInfo(t, singleFileInfo("good.txt", 16, 16))
	data := bytes.Join([][]byte{
		[]byte("d8:announce15:http://a.com/an8:announce15:http://b.com/an7:comment1:a7:comment1:b4:info"), info,
		[]byte("8:url-listl16:http://a.com/ws/e8:url-listl16:http://b.com/ws/ee"),
	}, nil)
	tor, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse() = %v, want repeated outer keys accepted", err)
	}
	if tor.Announce != "http://a.com/an" || len(tor.Trackers) != 1 || tor.Trackers[0] != "http://a.com/an" {
		t.Fatalf("Announce=%q Trackers=%q, want the first announce", tor.Announce, tor.Trackers)
	}
	if len(tor.WebSeeds) != 1 || tor.WebSeeds[0] != "http://a.com/ws/" {
		t.Fatalf("WebSeeds=%q, want the first url-list", tor.WebSeeds)
	}
	if tor.InfoHash != sha1.Sum(info) || tor.Name != "good.txt" {
		t.Fatalf("InfoHash=%x Name=%q, want the info dict's", tor.InfoHash, tor.Name)
	}
}

func TestParseInfo(t *testing.T) {
	infoBytes := marshalInfo(t, singleFileInfo("file.bin", 16, 40))
	tor, err := ParseInfo(infoBytes)
	if err != nil {
		t.Fatalf("ParseInfo() = %v", err)
	}
	if tor.InfoHash != sha1.Sum(infoBytes) || !bytes.Equal(tor.InfoBytes, infoBytes) {
		t.Fatalf("InfoHash/InfoBytes do not match the input bytes")
	}
	if tor.Name != "file.bin" || tor.PieceLength != 16 || len(tor.PieceHashes) != 3 || len(tor.Files) != 1 {
		t.Fatalf("ParseInfo() = %+v, want file.bin with 3 pieces", tor)
	}

	// Parse of the same info dict, wrapped, must agree field for field.
	wrapped, err := Parse(bytes.Join([][]byte{[]byte("d4:info"), infoBytes, []byte("e")}, nil))
	if err != nil {
		t.Fatalf("Parse(wrapped) = %v", err)
	}
	if wrapped.InfoHash != tor.InfoHash || wrapped.Name != tor.Name || len(wrapped.PieceHashes) != len(tor.PieceHashes) {
		t.Fatalf("Parse(wrapped) = %+v, ParseInfo = %+v", wrapped, tor)
	}

	// The metadata must be exactly one dictionary: extra values after it made
	// the wrapped parse hash only the first dict, so the session's InfoBytes no
	// longer matched the magnet's info-hash.
	other := marshalInfo(t, singleFileInfo("other.bin", 16, 16))
	for name, data := range map[string][]byte{
		"trailing key/value": append(append([]byte(nil), infoBytes...), "3:xyzi1e"...),
		"second info dict":   bytes.Join([][]byte{infoBytes, []byte("4:info"), other}, nil),
		"trailing byte":      append(append([]byte(nil), infoBytes...), 'e'),
		"not a dict":         []byte("l4:spame"),
		"empty":              nil,
	} {
		if _, err := ParseInfo(data); err == nil {
			t.Errorf("%s: ParseInfo() = nil error, want rejection", name)
		}
	}
}

// multiFileInfo returns a one-piece multi-file info dict named "root" with a
// one-byte file at each of paths.
func multiFileInfo(paths ...[]string) map[string]interface{} {
	files := make([]interface{}, 0, len(paths))
	for _, p := range paths {
		comps := make([]interface{}, len(p))
		for i, c := range p {
			comps[i] = c
		}
		files = append(files, map[string]interface{}{"length": int64(1), "path": comps})
	}
	return map[string]interface{}{
		"name":         "root",
		"piece length": int64(16384),
		"pieces":       string(make([]byte, 20)),
		"files":        files,
	}
}

// TestParseRejectsFileThatIsAlsoADirectory (libtorrent@dd04258ef): a torrent
// listing both "a" and "a/b" was accepted and only failed when storage tried
// to create one of them, with ENOTDIR or EISDIR. Such metadata is refused at
// parse time, folding case and normalization like the duplicate check.
func TestParseRejectsFileThatIsAlsoADirectory(t *testing.T) {
	deep := make([]string, maxPathDepth-1)
	for i := range deep {
		deep[i] = strings.Repeat("d", 200)
	}
	with := func(parents []string, last ...string) []string {
		return append(append([]string(nil), parents...), last...)
	}
	for name, paths := range map[string][][]string{
		"file then directory":     {{"a"}, {"a", "b"}},
		"directory then file":     {{"a", "b"}, {"a"}},
		"case folded":             {{"A"}, {"a", "b"}},
		"normalization folded":    {{"é"}, {"é", "x"}},
		"intermediate directory":  {{"x", "y", "z"}, {"x", "y"}},
		"deep shared parents":     {with(deep, "f"), deep},
		"among unrelated entries": {{"c"}, {"a", "b", "c"}, {"a", "b"}, {"d"}},
	} {
		info := multiFileInfo(paths...)
		if _, err := ParseInfo(marshalInfo(t, info)); err == nil || !strings.Contains(err.Error(), "is also a directory") {
			t.Errorf("%s: ParseInfo() = %v, want a file/directory collision error", name, err)
		}
		if _, err := Parse(marshalTorrent(t, info)); err == nil {
			t.Errorf("%s: Parse() = nil error, want rejection", name)
		}
	}

	for name, paths := range map[string][][]string{
		"siblings":                    {{"a", "b"}, {"a", "c"}},
		"same name in another parent": {{"a", "b"}, {"b"}, {"c", "a"}},
		"name prefix is not a parent": {{"a"}, {"ab", "c"}, {"a.d", "e"}},
		"deep siblings":               {with(deep, "f"), with(deep, "g")},
	} {
		tor, err := ParseInfo(marshalInfo(t, multiFileInfo(paths...)))
		if err != nil {
			t.Errorf("%s: ParseInfo() = %v, want success", name, err)
			continue
		}
		if len(tor.Files) != len(paths) {
			t.Errorf("%s: parsed %d files, want %d", name, len(tor.Files), len(paths))
		}
	}
}
