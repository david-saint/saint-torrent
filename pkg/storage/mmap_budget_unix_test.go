//go:build !windows

package storage

import (
	"bytes"
	"crypto/sha1"
	"slices"
	"testing"
)

// TestMMapStorageServesPastMappingBudgetThroughHandles: the mmap backend mapped
// every file it touched, so a torrent with tens of thousands of files ran the
// process out of vm.max_map_count, and the Go runtime dies when its own next
// mapping fails. Past the mapping budget, files are served through the
// budgeted handles instead, and every operation still works.
func TestMMapStorageServesPastMappingBudgetThroughHandles(t *testing.T) {
	const budget = 4
	const fileCount, fileLength, pieceLength = 32, 16, 24
	base := liveMappings.Load()
	mappingLimit.Store(base + budget)
	t.Cleanup(func() { mappingLimit.Store(0) })

	root := t.TempDir()
	files := manySmallFiles(fileCount, fileLength)
	st, err := NewMMapStorage(root, files, pieceLength)
	if err != nil {
		t.Fatal(err)
	}
	pieces := int(st.TotalSize() / pieceLength)
	for i := range pieces {
		if err := st.WriteBlock(int64(i), 0, budgetPiece(i, pieceLength)); err != nil {
			t.Fatalf("WriteBlock(%d): %v", i, err)
		}
	}
	got := make([]byte, pieceLength)
	for i := range pieces {
		if _, err := st.ReadBlock(int64(i), 0, got); err != nil || !bytes.Equal(got, budgetPiece(i, pieceLength)) {
			t.Fatalf("ReadBlock(%d) = %v, %v; want the written piece", i, got, err)
		}
		if ok, err := st.VerifyPiece(int64(i), sha1.Sum(budgetPiece(i, pieceLength))); err != nil || !ok {
			t.Fatalf("VerifyPiece(%d) = %v, %v; want a match", i, ok, err)
		}
	}

	if live := liveMappings.Load() - base; live > budget {
		t.Fatalf("%d files mapped, want at most %d", live, budget)
	}
	st.mu.RLock()
	mapped, viaHandles := 0, 0
	for _, m := range st.maps {
		if len(m.data) > 0 {
			mapped++
		}
		if m.viaHandles {
			viaHandles++
		}
	}
	st.mu.RUnlock()
	if mapped != budget || viaHandles != fileCount-budget {
		t.Fatalf("%d files mapped and %d served through handles, want %d and %d", mapped, viaHandles, budget, fileCount-budget)
	}

	// A durable checkpoint must cover the files written through handles as well
	// as the mapped ones, so a restart trusts every piece.
	verified := make([]int, pieces)
	for i := range verified {
		verified[i] = i
	}
	if err := st.SaveResumeState("budget", verified, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if live := liveMappings.Load(); live != base {
		t.Fatalf("%d mappings live after Close, %d before", live, base)
	}

	reopened, err := NewMMapStorage(root, files, pieceLength)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.LoadResumeState("budget")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(state.Verified, verified) || len(state.Recheck) != 0 {
		t.Fatalf("LoadResumeState = %+v, want every piece verified", state)
	}
	for i := range pieces {
		if _, err := reopened.ReadBlock(int64(i), 0, got); err != nil || !bytes.Equal(got, budgetPiece(i, pieceLength)) {
			t.Fatalf("ReadBlock(%d) after reopening = %v, %v; want the written piece", i, got, err)
		}
	}
}
