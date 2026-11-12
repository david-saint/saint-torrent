package downloader

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// padPieceLen is four blocks, so one piece can hold payload blocks, a block
// straddling payload and pad, and blocks lying entirely in pad files.
const padPieceLen = 4 * BlockSize

// padTestFile is one file of the hybrid test torrent.
type padTestFile struct {
	name   string // path below the torrent root; "" for a pad file
	length int64
}

// hybridPadFiles lays out six pieces the way libtorrent aligns a hybrid
// torrent, plus the cases it never produces but BEP 47 allows:
//
//	piece 0: a.bin, then tail padding: block 1 straddles, blocks 2-3 are pad
//	piece 1: b.bin, padded with the same ".pad/45536" name as a.bin
//	piece 2: an all-pad piece (a second pad file, merged with b.bin's)
//	piece 3: c.bin | one pad block | d.bin, a pad run mid-piece
//	piece 4: e.bin
//	piece 5: the end of e.bin, then tail padding: blocks 1-3 are pad
var hybridPadFiles = []padTestFile{
	{"a.bin", 20000}, {"", 45536},
	{"b.bin", 20000}, {"", 45536},
	{"", padPieceLen},
	{"c.bin", BlockSize}, {"", BlockSize}, {"d.bin", 2 * BlockSize},
	{"e.bin", 70000}, {"", 61072},
}

// hybridPadTorrent builds the bencoded torrent and parses it, so the pad flags
// come from "attr" as they would from a real .torrent. It returns the parsed
// torrent, the torrent's bytes (zeros in the pad ranges) and the payload of
// each file by slash path below the download directory.
func hybridPadTorrent(t *testing.T, webseeds ...string) (*torrent.Torrent, []byte, map[string][]byte) {
	t.Helper()
	var list []interface{}
	var content []byte
	payload := map[string][]byte{}
	for _, f := range hybridPadFiles {
		start := len(content)
		content = append(content, make([]byte, f.length)...)
		if f.name == "" {
			list = append(list, map[string]interface{}{
				"length": f.length,
				"path":   []interface{}{".pad", fmt.Sprint(f.length)},
				"attr":   "p",
			})
			continue
		}
		for i := start; i < len(content); i++ {
			content[i] = byte(i*7 + i/251 + 1)
		}
		list = append(list, map[string]interface{}{"length": f.length, "path": []interface{}{f.name}})
		payload["hybrid/"+f.name] = content[start:]
	}
	numPieces := (len(content) + padPieceLen - 1) / padPieceLen
	var pieces []byte
	for i := 0; i < numPieces; i++ {
		sum := sha1.Sum(content[i*padPieceLen : min((i+1)*padPieceLen, len(content))])
		pieces = append(pieces, sum[:]...)
	}
	meta := map[string]interface{}{"info": map[string]interface{}{
		"name":         "hybrid",
		"piece length": int64(padPieceLen),
		"pieces":       string(pieces),
		"files":        list,
	}}
	if len(webseeds) > 0 {
		urls := make([]interface{}, len(webseeds))
		for i, u := range webseeds {
			urls[i] = u
		}
		meta["url-list"] = urls
	}
	data, err := bencode.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	tor, err := torrent.Parse(data)
	if err != nil {
		t.Fatalf("parse hybrid torrent: %v", err)
	}
	return tor, content, payload
}

// hybridPadTorrentBytes returns the bencoded hybrid test torrent.
func hybridPadTorrentBytes(t *testing.T) []byte {
	t.Helper()
	tor, _, _ := hybridPadTorrent(t)
	data, err := bencode.Marshal(map[string]interface{}{"info": bencodeRaw(t, tor.InfoBytes)})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// bencodeRaw decodes an info dict so it can be re-marshalled inside a torrent.
func bencodeRaw(t *testing.T, info []byte) interface{} {
	t.Helper()
	v, err := bencode.Unmarshal(info)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// hybridIsPad reports whether torrent offset off lies in a pad file.
func hybridIsPad(off int64) bool {
	var start int64
	for _, f := range hybridPadFiles {
		if off < start+f.length {
			return f.name == ""
		}
		start += f.length
	}
	return false
}

// newPadSession builds a session on file storage in a fresh directory.
func newPadSession(t *testing.T, tor *torrent.Torrent) *Session {
	t.Helper()
	dir := t.TempDir()
	st, err := storage.NewStorage(dir, storageFileInfos(tor.Files), tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(sess.Close)
	return sess
}

func TestPadMaskBlocks(t *testing.T) {
	tor, _, _ := hybridPadTorrent(t)
	mask := newPadMask(tor.Files, tor.PieceLength, len(tor.PieceHashes))
	if mask == nil {
		t.Fatal("no mask for a torrent with pad files")
	}
	if want := []int{2}; !reflect.DeepEqual(mask.fullPieces, want) {
		t.Fatalf("fullPieces = %v, want %v", mask.fullPieces, want)
	}
	type blocks struct {
		runs                  []padRun
		padBlocks, requestEnd int64
		hasPad                bool
	}
	for piece, want := range map[int64]blocks{
		0: {[]padRun{{2, 4}}, 2, 2, true},
		1: {[]padRun{{2, 4}}, 2, 2, true},
		2: {[]padRun{{0, 4}}, 4, 0, true},
		3: {[]padRun{{1, 2}}, 1, 4, true},
		4: {nil, 0, 4, false},
		5: {[]padRun{{1, 4}}, 3, 1, true},
	} {
		runs, padBlocks, requestEnd, hasPad := mask.pieceBlocks(piece, padPieceLen, 4)
		if got := (blocks{runs, padBlocks, requestEnd, hasPad}); !reflect.DeepEqual(got, want) {
			t.Errorf("piece %d: %+v, want %+v", piece, got, want)
		}
	}
	// A short final block lying in pad counts as a pad block.
	short := []torrent.File{{Length: BlockSize + 100}, {Length: BlockSize - 100, Pad: true}, {Length: 120, Pad: true}}
	m := newPadMask(short, 2*BlockSize+120, 1)
	if runs, n, end, _ := m.pieceBlocks(0, 2*BlockSize+120, 3); len(runs) != 1 || runs[0] != (padRun{2, 3}) || n != 1 || end != 2 {
		t.Fatalf("short tail: runs=%v pad=%d end=%d, want [{2 3}] 1 2", runs, n, end)
	}

	buf := bytes.Repeat([]byte{0xAA}, padPieceLen)
	mask.zeroPad(0, buf)
	for i, b := range buf {
		if want := byte(0xAA); hybridIsPad(int64(i)) {
			want = 0
			if b != want {
				t.Fatalf("zeroPad left byte %d = %#x", i, b)
			}
		} else if b != want {
			t.Fatalf("zeroPad cleared payload byte %d", i)
		}
	}

	if newPadMask([]torrent.File{{Length: 10}, {Length: 0, Pad: true}}, 16, 1) != nil {
		t.Fatal("a torrent without pad bytes must get no mask")
	}
}

func TestPayloadFiles(t *testing.T) {
	plain := []torrent.File{{Path: []string{"a"}}, {Path: []string{"b"}}}
	if files, index := PayloadFiles(plain); &files[0] != &plain[0] || index != nil {
		t.Fatal("a torrent without pad files must be returned as is")
	}
	tor, _, _ := hybridPadTorrent(t)
	files, index := PayloadFiles(tor.Files)
	var names []string
	for i, f := range files {
		names = append(names, f.Path[len(f.Path)-1])
		if !reflect.DeepEqual(tor.Files[index[i]].Path, f.Path) {
			t.Fatalf("index %d maps %q to %q", i, f.Path, tor.Files[index[i]].Path)
		}
	}
	if want := []string{"a.bin", "b.bin", "c.bin", "d.bin", "e.bin"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("payload files %v, want %v", names, want)
	}
}

// TestPadFilesAlwaysSkipped: pad files keep their slot in the priorities, so
// indices stay those of the torrent, but nothing makes them wanted.
func TestPadFilesAlwaysSkipped(t *testing.T) {
	tor, _, _ := hybridPadTorrent(t)
	sess := newPadSession(t, tor)
	prios := sess.GetFilePriorities()
	if len(prios) != len(tor.Files) {
		t.Fatalf("%d priorities for %d files", len(prios), len(tor.Files))
	}
	for i, f := range tor.Files {
		want := PriorityNormal
		if f.Pad {
			want = PrioritySkip
		}
		if prios[i] != want {
			t.Fatalf("file %d (pad=%v) priority %v, want %v", i, f.Pad, prios[i], want)
		}
	}
	sess.SetFilePriority(1, PriorityHigh)
	sess.mu.Lock()
	sess.applyFilePrioritiesLocked([]FilePriority{PriorityLow, PriorityHigh, PriorityHigh})
	sess.mu.Unlock()
	prios = sess.GetFilePriorities()
	if prios[0] != PriorityLow || prios[1] != PrioritySkip || prios[2] != PriorityHigh {
		t.Fatalf("priorities after updates = %v, want [low skip high ...]", prios[:3])
	}
	if path, ok := sess.ContentPath(); !ok || filepath.Base(path) != "hybrid" {
		t.Fatalf("ContentPath = %q, %v", path, ok)
	}

	// Completion counts payload only: the all-pad piece is complete up front
	// and pad bytes are not wanted bytes.
	sess.mu.RLock()
	stats := sess.completionStatsLocked()
	state := sess.PieceStates[2]
	sess.mu.RUnlock()
	if state != PieceCompleted {
		t.Fatalf("all-pad piece state %v, want completed", state)
	}
	if want := int64(20000 + 20000 + BlockSize + 2*BlockSize + 70000); stats.wantedBytes != want {
		t.Fatalf("wanted bytes %d, want the payload's %d", stats.wantedBytes, want)
	}
}

// TestPadPiecesCompleteWithoutResume: with no resume state at all (memory
// storage), the all-pad piece is still complete from the start, so a full
// rescan never waits for it and the torrent can reach 100%.
func TestPadPiecesCompleteWithoutResume(t *testing.T) {
	tor, _, _ := hybridPadTorrent(t)
	st, err := storage.NewMemStorage(t.TempDir(), storageFileInfos(tor.Files), tor.PieceLength)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if sess.PieceStates[2] != PieceCompleted {
		t.Fatalf("all-pad piece %v, want completed", sess.PieceStates[2])
	}
	if !sess.verifyFullScan || sess.verifyTotalBytes != 5*padPieceLen {
		t.Fatalf("full scan %v of %d bytes, want the five other pieces", sess.verifyFullScan, sess.verifyTotalBytes)
	}
	if _, needed := sess.neededPieces[2]; needed {
		t.Fatal("all-pad piece is in the needed set")
	}
}

// recordingProxy relays a peer connection, recording every request message
// one side sends.
type recordingProxy struct {
	mu       sync.Mutex
	requests [][3]uint32
}

// relay copies messages from one end to the other. Reading never waits on the
// write: both peer loops write their opening messages before they read, so
// an unbuffered relay over net.Pipe would deadlock them.
func (p *recordingProxy) relay(from, to net.Conn, record bool) {
	queue := make(chan []byte, 1<<14)
	go func() {
		defer close(queue)
		for {
			msg, err := peer.ParseMessage(from)
			if err != nil {
				return
			}
			if msg == nil {
				queue <- make([]byte, 4) // keep-alive
				continue
			}
			if record && msg.ID == peer.MsgRequest && len(msg.Payload) == 12 {
				p.mu.Lock()
				p.requests = append(p.requests, [3]uint32{
					binary.BigEndian.Uint32(msg.Payload[0:4]),
					binary.BigEndian.Uint32(msg.Payload[4:8]),
					binary.BigEndian.Uint32(msg.Payload[8:12]),
				})
				p.mu.Unlock()
			}
			queue <- msg.Serialize()
		}
	}()
	for frame := range queue {
		if _, err := to.Write(frame); err != nil {
			break
		}
	}
	_ = to.Close()
	_ = from.Close()
}

func (p *recordingProxy) snapshot() [][3]uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][3]uint32(nil), p.requests...)
}

// unchokeRemote makes sess unchoke the connection it knows as addr, as its
// choker would.
func unchokeRemote(t *testing.T, sess *Session, addr string) {
	t.Helper()
	waitFor(t, "connection "+addr, 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		client := sess.activePeers[addr]
		if client == nil {
			return false
		}
		sess.Peers[addr].AmChoking = false
		client.Notify()
		return true
	})
}

// assertPayloadOnDisk fails unless the regular files under dir are exactly
// the torrent's payload files with their content, and nothing named .pad
// exists.
func assertPayloadOnDisk(t *testing.T, dir string, payload map[string][]byte) {
	t.Helper()
	found := map[string]bool{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		rel = filepath.ToSlash(rel)
		if d.Name() == ".pad" {
			t.Errorf("pad directory created: %s", rel)
		}
		if !d.Type().IsRegular() || strings.HasSuffix(rel, ".state") {
			return nil
		}
		want, ok := payload[rel]
		if !ok {
			t.Errorf("unexpected file on disk: %s", rel)
			return nil
		}
		got, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s holds the wrong bytes", rel)
		}
		found[rel] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != len(payload) {
		t.Fatalf("payload files on disk %v, want all of %d", found, len(payload))
	}
}

// TestPadTorrentSwarmEndToEnd is issue #115's "done when": a hybrid torrent
// with repeated pad names, partial-pad pieces and an all-pad piece loads,
// downloads from a peer, verifies and seeds. No block lying entirely in pad
// files is ever requested, no pad file reaches the disk, and the finished
// download serves zeros for pad ranges.
func TestPadTorrentSwarmEndToEnd(t *testing.T) {
	tor, content, payload := hybridPadTorrent(t)
	numPieces := len(tor.PieceHashes)

	seed := newPadSession(t, tor)
	for piece := 0; piece < numPieces; piece++ {
		// Garbage where the pad bytes go: storage must drop it.
		data := append([]byte(nil), content[piece*padPieceLen:min((piece+1)*padPieceLen, len(content))]...)
		for i := range data {
			if hybridIsPad(int64(piece*padPieceLen + i)) {
				data[i] = 0xEE
			}
		}
		if err := seed.Storage.WriteBlock(int64(piece), 0, data); err != nil {
			t.Fatal(err)
		}
	}
	seed.mu.Lock()
	for piece := 0; piece < numPieces; piece++ {
		seed.setPieceStateLocked(piece, PieceCompleted)
	}
	seed.mu.Unlock()

	leech := newPadSession(t, tor)
	dirtyPieceBufs(leech)
	leech.mu.RLock()
	padDone := leech.PieceStates[2] == PieceCompleted
	leech.mu.RUnlock()
	if !padDone {
		t.Fatal("the all-pad piece is not complete before any download")
	}

	// leecher <-> proxy <-> seed, the proxy recording the leecher's requests.
	leechConn, proxyDown := net.Pipe()
	proxyUp, seedConn := net.Pipe()
	proxy := &recordingProxy{}
	go proxy.relay(proxyDown, proxyUp, true)
	go proxy.relay(proxyUp, proxyDown, false)
	const seedAddr, leechAddr = "127.0.0.1:7101", "127.0.0.1:7102"
	done := make(chan struct{}, 2)
	go func() {
		leech.runPeerMessageLoop(peer.NewClient(leechConn, tor.InfoHash, leech.PeerID), leechConn, seedAddr, "127.0.0.1", 7101, fastReserved(), true)
		done <- struct{}{}
	}()
	go func() {
		seed.runPeerMessageLoop(peer.NewClient(seedConn, tor.InfoHash, seed.PeerID), seedConn, leechAddr, "127.0.0.1", 7102, fastReserved(), false)
		done <- struct{}{}
	}()
	t.Cleanup(func() {
		_ = leechConn.Close()
		_ = seedConn.Close()
		for i := 0; i < 2; i++ {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("peer loop did not exit")
			}
		}
	})
	unchokeRemote(t, seed, leechAddr)

	deadline := time.Now().Add(15 * time.Second)
	for !leech.IsCompleted() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !leech.IsCompleted() {
		leech.mu.RLock()
		lp := *leech.Peers[seedAddr]
		leech.mu.RUnlock()
		seed.mu.RLock()
		sp := *seed.Peers[leechAddr]
		seed.mu.RUnlock()
		t.Fatalf("download stalled: states %v, requests %v, err %v leechview=%+v seedview=%+v", leech.GetPieceStates(), proxy.snapshot(), leech.LastError(), lp, sp)
	}
	waitFor(t, "every piece to complete", 5*time.Second, func() bool {
		for _, state := range leech.GetPieceStates() {
			if state != PieceCompleted {
				return false
			}
		}
		return true
	})
	if pct := leech.PercentComplete(); pct != 100 {
		t.Fatalf("PercentComplete = %v", pct)
	}

	requests := proxy.snapshot()
	distinct := map[[3]uint32]bool{}
	straddling := 0
	for _, r := range requests {
		if distinct[r] {
			continue // an endgame copy of a piece still queued for writing
		}
		distinct[r] = true
		start := int64(r[0])*padPieceLen + int64(r[1])
		hasPayload, hasPad := false, false
		for off := start; off < start+int64(r[2]); off++ {
			if hybridIsPad(off) {
				hasPad = true
			} else {
				hasPayload = true
			}
		}
		if hasPayload && hasPad {
			straddling++
		}
		if !hasPayload {
			t.Errorf("requested piece %d begin %d length %d, which lies entirely in pad files", r[0], r[1], r[2])
		}
		if r[0] == 2 {
			t.Errorf("requested a block of the all-pad piece: %v", r)
		}
	}
	// The five payload pieces hold 2 + 2 + 3 + 4 + 1 blocks with payload, and
	// pieces 0, 1 and 5 each have one block straddling payload and pad.
	if len(distinct) != 12 || straddling != 3 {
		t.Fatalf("%d distinct requests, %d straddling; want 12 and 3: %v", len(distinct), straddling, requests)
	}

	assertPayloadOnDisk(t, leech.Storage.BaseDir(), payload)
	assertPayloadOnDisk(t, seed.Storage.BaseDir(), payload)

	// The finished download seeds: pad ranges read back as zeros, aligned or
	// not, through an upload buffer the pool hands out dirty.
	for i := 0; i < 4; i++ {
		dirty := bytes.Repeat([]byte{0xAA}, BlockSize)
		leech.putUploadBlockBuf(&dirty)
	}
	w := startWirePeer(t, leech, 7103, fastReserved())
	w.send(&peer.Message{ID: peer.MsgInterested})
	unchokeRemote(t, leech, "127.0.0.1:7103")
	for _, r := range [][3]uint32{
		{0, 2 * BlockSize, BlockSize},  // aligned, entirely pad
		{0, 19000, 5000},               // unaligned, payload into pad
		{2, 100, BlockSize},            // the all-pad piece
		{3, BlockSize - 10, BlockSize}, // payload into the mid-piece pad block
		{5, 4000, BlockSize},           // payload into tail padding
	} {
		w.sendRequest(r[0], r[1], r[2])
		msg := w.expect(peer.MsgPiece, 5*time.Second)
		index, begin := binary.BigEndian.Uint32(msg.Payload[0:4]), binary.BigEndian.Uint32(msg.Payload[4:8])
		start := int(index)*padPieceLen + int(begin)
		if index != r[0] || begin != r[1] || !bytes.Equal(msg.Payload[8:], content[start:start+int(r[2])]) {
			t.Fatalf("served %d/%d differs from the torrent bytes for request %v", index, begin, r)
		}
	}
}

// dirtyPieceBufs fills sess's piece-buffer pool with non-zero buffers, so an
// assembly that forgot to zero its pad blocks would hash garbage.
func dirtyPieceBufs(sess *Session) {
	for i := 0; i < 16; i++ {
		buf := bytes.Repeat([]byte{0xAA}, padPieceLen)
		sess.putPieceBuf(&buf)
	}
}

// TestPadAssemblyZeroesPadBytesPeersSend: the pad part of a block straddling
// payload and pad is whatever the peer sent. Assembly zeroes it, so the piece
// is hashed exactly as storage will serve and re-verify it.
func TestPadAssemblyZeroesPadBytesPeersSend(t *testing.T) {
	tor, content, _ := hybridPadTorrent(t)
	sess := newPadSession(t, tor)
	dirtyPieceBufs(sess)
	w := startWirePeer(t, sess, 7201, fastReserved())
	w.send(&peer.Message{ID: peer.MsgHaveAll})
	w.send(&peer.Message{ID: peer.MsgUnchoke})
	deadline := time.After(10 * time.Second)
	for !sess.IsCompleted() {
		select {
		case msg, ok := <-w.in:
			if !ok {
				t.Fatal("connection closed")
			}
			if msg.ID != peer.MsgRequest {
				continue
			}
			index, begin, length := binary.BigEndian.Uint32(msg.Payload[0:4]), binary.BigEndian.Uint32(msg.Payload[4:8]), binary.BigEndian.Uint32(msg.Payload[8:12])
			start := int64(index)*padPieceLen + int64(begin)
			block := append([]byte(nil), content[start:start+int64(length)]...)
			payload := false
			for i := range block {
				if hybridIsPad(start + int64(i)) {
					block[i] = 0xEE
				} else {
					payload = true
				}
			}
			if !payload {
				t.Fatalf("requested %d/%d/%d, which lies entirely in pad files", index, begin, length)
			}
			reply := make([]byte, 8, 8+len(block))
			binary.BigEndian.PutUint32(reply[0:4], index)
			binary.BigEndian.PutUint32(reply[4:8], begin)
			w.send(&peer.Message{ID: peer.MsgPiece, Payload: append(reply, block...)})
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatalf("not complete: %v, err %v", sess.GetPieceStates(), sess.LastError())
		}
	}
	got := make([]byte, padPieceLen)
	if _, err := sess.Storage.ReadBlock(0, 0, got); err != nil || !bytes.Equal(got, content[:padPieceLen]) {
		t.Fatalf("piece 0 reads back wrong: %v", err)
	}
}

// TestPadTorrentWebseed: a webseed is never asked for a pad file (mirrors do
// not host them, and a 404 retires the mirror); pad bytes are filled locally.
func TestPadTorrentWebseed(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	_, _, payload := hybridPadTorrent(t)
	requests := makeRangeRecorder()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.record(r.URL.Path, r.Header.Get("Range"))
		data, ok := payload[strings.TrimPrefix(r.URL.Path, "/mirror/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		serveRange(t, w, r, data)
	}))
	defer srv.Close()

	tor, _, _ := hybridPadTorrent(t, srv.URL+"/mirror")
	sess := newPadSession(t, tor)
	startWebseedsForTest(t, sess)
	waitFor(t, "the webseed download to complete", 10*time.Second, sess.IsCompleted)

	got := requests.snapshot()
	if len(got) == 0 {
		t.Fatal("no webseed requests")
	}
	for _, r := range got {
		if strings.Contains(r, ".pad") {
			t.Errorf("webseed asked for a pad file: %s", r)
		}
	}
	if downloaded := webseedPeerDownloaded(t, sess); downloaded != 20000+20000+BlockSize+2*BlockSize+70000 {
		t.Errorf("webseed downloaded %d bytes, want the payload's only", downloaded)
	}
	assertPayloadOnDisk(t, sess.Storage.BaseDir(), payload)
}

// TestPadUpgradeRechecksPayload: a build without BEP 47 support wrote the pad
// files as real files and recorded them in its resume state. That state no
// longer matches the file list, which must cost a recheck, not an error: the
// payload is untouched, so every piece verifies again. The old pad files are
// left alone.
func TestPadUpgradeRechecksPayload(t *testing.T) {
	tor, content, _ := hybridPadTorrent(t)
	dir := t.TempDir()
	hash := fmt.Sprintf("%x", tor.InfoHash)

	// The old build: every file real, pad files included. It refused repeated
	// names, so give its pad files distinct ones.
	oldInfos := storageFileInfos(tor.Files)
	for i := range oldInfos {
		if oldInfos[i].Pad {
			oldInfos[i].Pad = false
			oldInfos[i].Path = filepath.Join("hybrid", ".pad", fmt.Sprintf("%d-%d", i, oldInfos[i].Length))
		}
	}
	old, err := storage.NewFileStorage(dir, oldInfos, tor.PieceLength)
	if err != nil {
		t.Fatal(err)
	}
	all := make([]int, len(tor.PieceHashes))
	for piece := range all {
		all[piece] = piece
		if err := old.WriteBlock(int64(piece), 0, content[piece*padPieceLen:min((piece+1)*padPieceLen, len(content))]); err != nil {
			t.Fatal(err)
		}
	}
	if err := old.SaveResumeState(hash, all, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := storage.NewStorage(dir, storageFileInfos(tor.Files), tor.PieceLength)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := newSession(tor, st, [20]byte{}, 0, dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	sess.mu.RLock()
	fullScan := sess.verifyFullScan
	sess.mu.RUnlock()
	if !fullScan {
		t.Fatal("an incompatible resume state did not fall back to a recheck")
	}
	sess.maybeStartVerification()
	sess.WaitVerified()
	if !sess.IsCompleted() {
		t.Fatalf("recheck left pieces %v, err %v", sess.GetPieceStates(), sess.LastError())
	}
	for i := range oldInfos {
		if tor.Files[i].Pad {
			if info, err := os.Stat(filepath.Join(dir, oldInfos[i].Path)); err != nil || info.Size() != oldInfos[i].Length {
				t.Errorf("old pad file %s was disturbed: %v", oldInfos[i].Path, err)
			}
		}
	}
}

// TestPadTorrentManagerLifecycle: through the manager, a torrent with
// repeated pad names is added without claiming, migrating or creating any pad
// path, and deleting it with its files leaves a ".pad" file an older build
// wrote where it was.
func TestPadTorrentManagerLifecycle(t *testing.T) {
	downloadDir := t.TempDir()
	torrentPath := filepath.Join(t.TempDir(), "hybrid.torrent")
	if err := os.WriteFile(torrentPath, hybridPadTorrentBytes(t), 0600); err != nil {
		t.Fatal(err)
	}
	leftover := filepath.Join(downloadDir, "hybrid", ".pad", "45536")
	if err := os.MkdirAll(filepath.Dir(leftover), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, make([]byte, 45536), 0600); err != nil {
		t.Fatal(err)
	}

	mgr := NewTorrentManager()
	defer mgr.Close()
	sess, err := mgr.AddTorrentFile(torrentPath, downloadDir)
	if err != nil {
		t.Fatalf("add hybrid torrent: %v", err)
	}
	for _, name := range []string{"a.bin", "b.bin", "c.bin", "d.bin", "e.bin"} {
		if _, err := os.Stat(filepath.Join(downloadDir, "hybrid", name)); err != nil {
			t.Fatalf("payload file %s: %v", name, err)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(leftover))
	if err != nil || len(entries) != 1 {
		t.Fatalf(".pad holds %v (%v), want only the old file", entries, err)
	}

	if err := mgr.RemoveSession(fmt.Sprintf("%x", sess.Torrent.InfoHash), true); err != nil {
		t.Fatalf("remove with files: %v", err)
	}
	if _, err := os.Stat(filepath.Join(downloadDir, "hybrid", "a.bin")); !os.IsNotExist(err) {
		t.Fatalf("payload survived deletion: %v", err)
	}
	if info, err := os.Stat(leftover); err != nil || info.Size() != 45536 {
		t.Fatalf("the old pad file was deleted or changed: %v", err)
	}
}

// TestPadMetadataWithUnverifiablePadPieceRefused: a magnet whose metadata has
// a piece lying entirely in pad files with a hash that is not that of zeros
// is refused with a clear error, as the same .torrent would be, instead of
// downloading to 99% and stalling.
func TestPadMetadataWithUnverifiablePadPieceRefused(t *testing.T) {
	info := map[string]interface{}{
		"name":         "lying",
		"piece length": int64(16),
		"pieces":       string(make([]byte, 20*3)), // piece 1 is all pad; zeros are not its hash
		"files": []interface{}{
			map[string]interface{}{"length": int64(16), "path": []interface{}{"a"}},
			map[string]interface{}{"length": int64(16), "path": []interface{}{".pad", "16"}, "attr": "p"},
			map[string]interface{}{"length": int64(16), "path": []interface{}{"b"}},
		},
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	tor := &torrent.Torrent{Name: "lying", InfoHash: sha1.Sum(infoBytes)}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	err = sess.onMetadataDownloaded(infoBytes)
	if err == nil || !strings.Contains(err.Error(), "pad") {
		t.Fatalf("onMetadataDownloaded = %v, want the pad piece refused", err)
	}
	if last := sess.LastError(); last == nil || !strings.Contains(last.Error(), "pad") {
		t.Fatalf("LastError = %v, want the refusal shown", last)
	}
	if !sess.IsMetadataMode() || sess.Storage != nil {
		t.Fatal("the refused metadata was applied")
	}
}
