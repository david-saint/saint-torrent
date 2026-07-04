package downloader

import (
	"crypto/sha1"
	"testing"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestPriorityStalenessRegression(t *testing.T) {
	// 1. Path 1 & Path 2: Test session with 2 files, piece length 16, total size 32 (2 pieces)
	fileLengths := []int64{16, 16}
	sess := newTestSessionBuilder(t, 16, fileLengths, nil)
	defer sess.Close()

	// Initially, both should be wanted (PriorityNormal)
	sess.mu.RLock()
	wanted0 := sess.isPieceWanted(0)
	wanted1 := sess.isPieceWanted(1)
	pri0 := sess.piecePriority(0)
	pri1 := sess.piecePriority(1)
	sess.mu.RUnlock()

	if !wanted0 || !wanted1 {
		t.Errorf("expected both pieces initially wanted, got %v, %v", wanted0, wanted1)
	}
	if pri0 != PriorityNormal || pri1 != PriorityNormal {
		t.Errorf("expected PriorityNormal, got %v, %v", pri0, pri1)
	}

	// Test Path 1: Individual Setter (SetFilePriority)
	sess.SetFilePriority(0, PrioritySkip)

	sess.mu.RLock()
	wanted0 = sess.isPieceWanted(0)
	wanted1 = sess.isPieceWanted(1)
	pri0 = sess.piecePriority(0)
	pri1 = sess.piecePriority(1)
	sess.mu.RUnlock()

	if wanted0 {
		t.Error("expected piece 0 to be unwanted after skipping file 0")
	}
	if !wanted1 {
		t.Error("expected piece 1 to remain wanted")
	}
	if pri0 != PrioritySkip {
		t.Errorf("expected piece 0 priority to be PrioritySkip, got %v", pri0)
	}
	if pri1 != PriorityNormal {
		t.Errorf("expected piece 1 priority to remain PriorityNormal, got %v", pri1)
	}

	// Test Path 2: Bulk Apply (applyFilePrioritiesLocked)
	sess.mu.Lock()
	sess.applyFilePrioritiesLocked([]FilePriority{PriorityHigh, PrioritySkip})
	sess.mu.Unlock()

	sess.mu.RLock()
	wanted0 = sess.isPieceWanted(0)
	wanted1 = sess.isPieceWanted(1)
	pri0 = sess.piecePriority(0)
	pri1 = sess.piecePriority(1)
	sess.mu.RUnlock()

	if !wanted0 {
		t.Error("expected piece 0 to be wanted after setting file 0 to PriorityHigh")
	}
	if wanted1 {
		t.Error("expected piece 1 to be unwanted after skipping file 1")
	}
	if pri0 != PriorityHigh {
		t.Errorf("expected piece 0 priority to be PriorityHigh, got %v", pri0)
	}
	if pri1 != PrioritySkip {
		t.Errorf("expected piece 1 priority to be PrioritySkip, got %v", pri1)
	}

	// 2. Path 3: Metadata Arrival reinit
	// Create a session in metadata mode
	infoBytes := []byte("d5:filesld6:lengthi16e4:pathl9:file1.txteed6:lengthi16e4:pathl9:file2.txteee4:name4:test12:piece lengthi16e6:pieces40:1234567890123456789012345678901234567890e")
	infoHash := sha1.Sum(infoBytes)
	tor := &torrent.Torrent{
		InfoHash: infoHash,
	}
	sessMeta, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create metadata session: %v", err)
	}
	defer sessMeta.Close()

	// Configure mock storage factory
	f, _ := storage.FactoryForBackend(storage.BackendMemory)
	sessMeta.storageFactory = f

	// Apply pending priorities (metadata mode)
	sessMeta.mu.Lock()
	// Stash it in pendingFilePriorities directly in test for metadataMode stashing
	var pending []FilePriority
	for _, prio := range []FilePriority{PriorityHigh, PrioritySkip} {
		pending = append(pending, prio)
	}
	sessMeta.pendingFilePriorities = pending
	sessMeta.mu.Unlock()

	// Verify they are stored in pendingFilePriorities
	sessMeta.mu.RLock()
	pendingStored := sessMeta.pendingFilePriorities
	sessMeta.mu.RUnlock()
	if len(pendingStored) != 2 || pendingStored[0] != PriorityHigh || pendingStored[1] != PrioritySkip {
		t.Fatalf("unexpected pending priorities: %v", pendingStored)
	}

	// Simulate metadata download and trigger onMetadataDownloaded
	err = sessMeta.onMetadataDownloaded(infoBytes)
	if err != nil {
		t.Fatalf("onMetadataDownloaded: %v", err)
	}

	// Verify priorities are applied to filePriorities and cache is rebuilt correctly
	sessMeta.mu.RLock()
	pri0 = sessMeta.piecePriority(0)
	pri1 = sessMeta.piecePriority(1)
	wanted0 = sessMeta.isPieceWanted(0)
	wanted1 = sessMeta.isPieceWanted(1)
	sessMeta.mu.RUnlock()

	if pri0 != PriorityHigh || pri1 != PrioritySkip {
		t.Errorf("expected priorities to be restored on metadata completion, got %v and %v", pri0, pri1)
	}
	if !wanted0 || wanted1 {
		t.Errorf("expected wanted0 = true, wanted1 = false, got %v and %v", wanted0, wanted1)
	}
}
