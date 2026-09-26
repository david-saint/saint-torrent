package storage

import "testing"

// TestFileIdentityRequiresChangeTime: FAT and exFAT report a zero change time,
// so an identity built on it only names a directory entry. A checkpoint must
// not bless such a file; an empty identity forces its pieces to be rechecked.
func TestFileIdentityRequiresChangeTime(t *testing.T) {
	if got := formatFileIdentity(7, 1, 2, 0); got != "" {
		t.Fatalf("identity without a change time = %q, want none", got)
	}
	if got := formatFileIdentity(7, 1, 2, 133); got != "7:1:2:133" {
		t.Fatalf("identity = %q, want 7:1:2:133", got)
	}
}
