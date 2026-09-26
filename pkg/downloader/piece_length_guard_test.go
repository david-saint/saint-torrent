package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/torrent"
)

// hugePieceStorage reports piece lengths without backing any data; only
// piece-length queries are expected of it.
type hugePieceStorage struct {
	pieceLen, total int64
}

func (h *hugePieceStorage) BaseDir() string         { return "" }
func (h *hugePieceStorage) TotalSize() int64        { return h.total }
func (h *hugePieceStorage) PieceLengthValue() int64 { return h.pieceLen }
func (h *hugePieceStorage) PieceLength(i int64) int64 {
	return min(h.pieceLen, h.total-i*h.pieceLen)
}
func (h *hugePieceStorage) ReadBlock(int64, int64, []byte) (int, error) {
	return 0, errors.New("no data")
}
func (h *hugePieceStorage) WriteBlock(int64, int64, []byte) error     { return errors.New("no data") }
func (h *hugePieceStorage) VerifyPiece(int64, [20]byte) (bool, error) { return false, nil }
func (h *hugePieceStorage) SaveState(string, []int) error             { return nil }
func (h *hugePieceStorage) LoadState(string) ([]int, error)           { return nil, nil }
func (h *hugePieceStorage) Close() error                              { return nil }

// TestOpenNewPieceSkipsUnaddressablePieces is the regression test for eager
// per-piece allocation on crafted piece lengths: parsing only checks that the
// piece length is positive, and opening a piece allocates two slices of
// pieceLen/16 KiB entries before any data arrives (gigabytes for a 1 TiB piece,
// per connection that advertises it). A piece longer than the wire protocol can
// address (4 GiB, or 2 GiB where int is 32 bits) is now never claimed; a piece of
// normal length in the same torrent is still fetched.
func TestOpenNewPieceSkipsUnaddressablePieces(t *testing.T) {
	st := &hugePieceStorage{pieceLen: 1 << 33, total: 1<<33 + 32}
	const lastPiece = 1 // 32 bytes long
	tor := &torrent.Torrent{
		Name:        "huge",
		InfoHash:    sha1.Sum([]byte("huge-piece")),
		PieceLength: st.pieceLen,
		PieceHashes: make([][20]byte, 2),
		Files:       []torrent.File{{Length: st.total, Path: []string{"huge.bin"}}},
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	w := startWirePeer(t, sess, 6280, fastReserved())

	w.send(&peer.Message{ID: peer.MsgHaveAll})
	w.send(&peer.Message{ID: peer.MsgUnchoke})
	req := w.expect(peer.MsgRequest, 2*time.Second)
	if got := binary.BigEndian.Uint32(req.Payload[0:4]); got != lastPiece {
		t.Fatalf("requested piece %d; want only the addressable piece %d", got, lastPiece)
	}
	for _, msg := range w.barrier() {
		if msg.ID == peer.MsgRequest && binary.BigEndian.Uint32(msg.Payload[0:4]) != lastPiece {
			t.Fatalf("requested unaddressable piece %d", binary.BigEndian.Uint32(msg.Payload[0:4]))
		}
	}
}

func TestPieceLengthAssemblable(t *testing.T) {
	for _, tc := range []struct {
		length int64
		want   bool
	}{
		{0, false},
		{-1, false},
		{1, true},
		{16 * 1024 * 1024, true},
		{1 << 30, true},
		{maxWirePieceLength + 1, false},
		{1 << 40, false},
	} {
		if got := pieceLengthAssemblable(tc.length); got != tc.want {
			t.Errorf("pieceLengthAssemblable(%d) = %v, want %v", tc.length, got, tc.want)
		}
	}
}
