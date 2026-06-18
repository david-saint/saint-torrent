package downloader

import (
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestPeerStateAtomicCountersAreAligned(t *testing.T) {
	ps := &PeerState{}
	for name, ptr := range map[string]*int64{
		"Downloaded": &ps.Downloaded,
		"Uploaded":   &ps.Uploaded,
	} {
		if addr := uintptr(unsafe.Pointer(ptr)); addr%8 != 0 {
			t.Fatalf("PeerState.%s is not 64-bit aligned: address %#x", name, addr)
		}
	}

	atomic.AddInt64(&ps.Downloaded, 1)
	atomic.AddInt64(&ps.Uploaded, 1)
}
