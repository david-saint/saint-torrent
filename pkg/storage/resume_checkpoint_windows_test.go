package storage

import "testing"

// TestFileIdentityWithoutChangeTime: FAT and exFAT report a zero change time.
// Refusing every identity there hashed each torrent on such a drive in full on
// every launch, so those volumes record a weaker, marked identity on the exact
// write time. Anything else without a change time, or without the parts that
// name the file, still gets none, which forces its pieces to be rechecked.
func TestFileIdentityWithoutChangeTime(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		indexHigh, indexLow   uint32
		changeTime, lastWrite int64
		fat                   bool
		want                  string
	}{
		{"change time", 1, 2, 133, 99, false, "7:1:2:133"},
		{"change time on FAT", 1, 2, 133, 99, true, "7:1:2:133"},
		{"FAT", 1, 2, 0, 99, true, "mtime:7:1:2:99"},
		{"FAT, high index", 1, 0, 0, 99, true, "mtime:7:1:0:99"},
		{"not FAT", 1, 2, 0, 99, false, ""},
		{"FAT without file index", 0, 0, 0, 99, true, ""},
		{"FAT without write time", 1, 2, 0, 0, true, ""},
	} {
		if got := formatFileIdentity(7, tc.indexHigh, tc.indexLow, tc.changeTime, tc.lastWrite, tc.fat); got != tc.want {
			t.Errorf("%s: identity = %q, want %q", tc.name, got, tc.want)
		}
	}
	// A weak identity never equals a full one, so neither is trusted as the other.
	if formatFileIdentity(7, 1, 2, 99, 99, false) == formatFileIdentity(7, 1, 2, 0, 99, true) {
		t.Fatal("weak and full identities collide")
	}
}

func TestIsFATFileSystem(t *testing.T) {
	for name, want := range map[string]bool{
		"FAT": true, "FAT32": true, "exFAT": true, "EXFAT": true, "FAT16": true,
		"NTFS": false, "ReFS": false, "": false, "FATX": false, "UDF": false,
	} {
		if got := isFATFileSystem(name); got != want {
			t.Errorf("isFATFileSystem(%q) = %v, want %v", name, got, want)
		}
	}
}
