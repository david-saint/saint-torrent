package downloader

import (
	"crypto/sha1"
	"errors"
	"strings"
	"testing"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// TestMagnetStorageFailureKeepsSessionInMetadataMode: a peer-supplied info
// dict whose files cannot be created (a reserved name, a file-vs-directory
// collision, a name over NAME_MAX) used to install a nil pointer boxed in the
// Storage interface and crash in loadResumeState, or, with a factory that
// returned a real nil, leave PieceStates sized so the next peer Request
// dereferenced the missing storage. Either way the whole process died.
func TestMagnetStorageFailureKeepsSessionInMetadataMode(t *testing.T) {
	for name, factory := range map[string]storage.Factory{
		"default": nil,
		"typed nil": func(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
			return (*storage.FileStorage)(nil), errors.New("disk unavailable")
		},
	} {
		t.Run(name, func(t *testing.T) {
			infoBytes, err := bencode.Marshal(map[string]interface{}{
				"name":         ".dht_nodes",
				"piece length": int64(16),
				"pieces":       string(make([]byte, 20)),
				"length":       int64(16),
			})
			if err != nil {
				t.Fatal(err)
			}
			sess, err := NewSession(&torrent.Torrent{Name: "magnet", InfoHash: sha1.Sum(infoBytes)}, nil, [20]byte{}, 0, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer sess.Close()
			sess.storageFactory = factory

			err = sess.onMetadataDownloaded(infoBytes)
			if err == nil || !strings.Contains(err.Error(), "failed to initialize storage") {
				t.Fatalf("onMetadataDownloaded = %v, want a storage initialization error", err)
			}
			sess.mu.RLock()
			defer sess.mu.RUnlock()
			if sess.Storage != nil {
				t.Fatalf("Storage = %T(%v), want nil after a failed init", sess.Storage, sess.Storage)
			}
			if len(sess.PieceStates) != 0 || len(sess.pieceAvailability) != 0 {
				t.Fatalf("PieceStates=%d availability=%d, want both empty so no peer path reaches storage", len(sess.PieceStates), len(sess.pieceAvailability))
			}
			if !sess.metadataMode || sess.Torrent.Name != "magnet" || len(sess.Torrent.PieceHashes) != 0 {
				t.Fatalf("metadataMode=%v name=%q hashes=%d, want the pre-metadata session", sess.metadataMode, sess.Torrent.Name, len(sess.Torrent.PieceHashes))
			}
			if sess.statusErr == nil {
				t.Fatal("storage failure was not surfaced as the session status")
			}
		})
	}
}
