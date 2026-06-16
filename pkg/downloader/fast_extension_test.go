package downloader

import "testing"

func TestAllowedFastSetMatchesBEP6Example(t *testing.T) {
	var infoHash [20]byte
	for i := range infoHash {
		infoHash[i] = 0xaa
	}

	got := allowedFastSet(infoHash, "80.4.4.200", 1313, 9)
	want := []int{1059, 431, 808, 1217, 287, 376, 1188, 353, 508}
	if len(got) != len(want) {
		t.Fatalf("allowedFastSet length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("allowedFastSet[%d] = %d, want %d (full set %v)", i, got[i], want[i], got)
		}
	}
}

func TestCompletedPieceBitfieldClassifiesEmptyPartialAndComplete(t *testing.T) {
	if bf, hasAny, hasAll := completedPieceBitfield(nil); bf != nil || hasAny || hasAll {
		t.Fatalf("nil states produced bf=%v hasAny=%t hasAll=%t", bf, hasAny, hasAll)
	}

	bf, hasAny, hasAll := completedPieceBitfield([]PieceState{PieceEmpty, PieceCompleted, PieceEmpty})
	if !hasAny || hasAll || len(bf) != 1 || bf[0] != 0x40 {
		t.Fatalf("partial states produced bf=%08b hasAny=%t hasAll=%t", bf, hasAny, hasAll)
	}

	bf, hasAny, hasAll = completedPieceBitfield([]PieceState{PieceCompleted, PieceCompleted, PieceCompleted})
	if !hasAny || !hasAll || len(bf) != 1 || bf[0] != 0xe0 {
		t.Fatalf("complete states produced bf=%08b hasAny=%t hasAll=%t", bf, hasAny, hasAll)
	}
}
