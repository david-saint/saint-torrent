package downloader

import (
	"crypto/sha1"
	"testing"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// memStorageFactory keeps metadata tests off the disk.
func memStorageFactory(dir string, files []storage.FileInfo, pieceLength int64) (storage.Storage, error) {
	return storage.NewMemStorage(dir, files, pieceLength)
}

// newTestMagnetSession returns a metadata-mode session for infoBytes, closed at
// the end of the test.
func newTestMagnetSession(t *testing.T, infoBytes []byte, factory storage.Factory) *Session {
	t.Helper()
	sess, err := NewSession(&torrent.Torrent{Name: "magnet", InfoHash: sha1.Sum(infoBytes)}, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sess.storageFactory = factory
	t.Cleanup(sess.Close)
	return sess
}

func testInfoDict(t *testing.T, name string) []byte {
	t.Helper()
	infoBytes, err := bencode.Marshal(map[string]interface{}{
		"name":         name,
		"piece length": int64(16),
		"pieces":       string(make([]byte, 20)),
		"length":       int64(16),
	})
	if err != nil {
		t.Fatal(err)
	}
	return infoBytes
}

// TestMetadataWithTrailingDataIsRejected: the magnet's hash covers the whole
// ut_metadata buffer, but the session parsed "d4:info" + buffer + "e". An info
// dict followed by an extra key/value pair parsed as the first dict alone, so
// the session served and cached info bytes that no longer hashed to the magnet
// and came back under a different hash after a restart.
func TestMetadataWithTrailingDataIsRejected(t *testing.T) {
	dict := testInfoDict(t, "first.bin")
	infoBytes := append(dict[:len(dict):len(dict)], "3:xyzi1e"...)
	sess := newTestMagnetSession(t, infoBytes, memStorageFactory)

	if err := sess.onMetadataDownloaded(infoBytes); err == nil {
		t.Fatal("metadata with a value after the info dict was accepted")
	}
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if !sess.metadataMode || sess.Storage != nil || len(sess.Torrent.InfoBytes) != 0 {
		t.Fatalf("metadataMode=%v storage=%v infoBytes=%d, want the pre-metadata session", sess.metadataMode, sess.Storage, len(sess.Torrent.InfoBytes))
	}
	if sess.statusErr == nil {
		t.Fatal("invalid metadata was not surfaced as the session status")
	}
	// The bytes matched the hash, so every peer sends the same ones: fetching
	// them again would only repeat the failure.
	if !sess.metadataCompleted || sess.metadataBuf != nil {
		t.Fatalf("metadataCompleted=%v buffered=%d, want fetching stopped and the buffer released", sess.metadataCompleted, len(sess.metadataBuf))
	}
}
