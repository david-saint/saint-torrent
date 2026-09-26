package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"net"

	"sainttorrent/pkg/peer"
)

const allowedFastSetSize = 10

// pendingAllowedFastCap bounds how many distinct allowed_fast offers we keep from
// one peer, both buffered before metadata is known (when indices cannot be
// validated yet) and once they can be. It sits far above any real client's
// allowed-fast set (allowedFastSetSize) so legitimate offers all survive, while
// stopping a peer from growing our memory at wire rate or making the per-message
// hasAllowedFastWork scan O(pieces) by offering every index.
const pendingAllowedFastCap = 256

// allowedFastServeRounds is how many times over (in blocks) a peer we are choking
// may fetch each piece of its allowed-fast set, as libtorrent does. An honest
// peer fetches a piece once; the cap stops a choked peer re-downloading its fast
// set indefinitely.
const allowedFastServeRounds = 3

func completedPieceBitfield(states []PieceState) (bitfield []byte, hasAny bool, hasAll bool) {
	if len(states) == 0 {
		return nil, false, false
	}
	bitfield = make([]byte, (len(states)+7)/8)
	hasAll = true
	for i, state := range states {
		if state != PieceCompleted {
			hasAll = false
			continue
		}
		setBit(bitfield, i)
		hasAny = true
	}
	return bitfield, hasAny, hasAll
}

// maxPendingBitfieldLen bounds a bitfield buffered before metadata is known.
// Metadata is capped at peer.MaxMetadataSize and every piece costs a 20-byte hash
// in it, so no valid torrent needs a longer bitfield than this (~100 KiB).
const maxPendingBitfieldLen = (peer.MaxMetadataSize/sha1.Size + 7) / 8

func fullPieceBitfield(numPieces int) []byte {
	if numPieces <= 0 {
		return nil
	}
	bitfield := make([]byte, (numPieces+7)/8)
	for i := range bitfield {
		bitfield[i] = 0xff
	}
	// Spare bits past the last piece stay clear, as BEP 3 requires.
	if spare := numPieces % 8; spare != 0 {
		bitfield[len(bitfield)-1] = byte(0xff) << (8 - spare)
	}
	return bitfield
}

// bitfieldComplete reports whether bf has all of the first numPieces bits set.
// Spare bits are ignored.
func bitfieldComplete(bf []byte, numPieces int) bool {
	if numPieces <= 0 || len(bf) < (numPieces+7)/8 {
		return false
	}
	full := numPieces / 8
	for _, b := range bf[:full] {
		if b != 0xff {
			return false
		}
	}
	if spare := numPieces % 8; spare != 0 {
		mask := byte(0xff) << (8 - spare)
		return bf[full]&mask == mask
	}
	return true
}

// bitfieldAny reports whether any bit of bf is set.
func bitfieldAny(bf []byte) bool {
	for _, b := range bf {
		if b != 0 {
			return true
		}
	}
	return false
}

func allowedFastSet(infoHash [20]byte, ip string, numPieces int, limit int) []int {
	if numPieces <= 0 || limit <= 0 {
		return nil
	}
	if limit > numPieces {
		limit = numPieces
	}
	ip4 := net.ParseIP(ip).To4()
	if ip4 == nil {
		return nil
	}

	x := make([]byte, 24)
	maskedIP := binary.BigEndian.Uint32(ip4) & 0xffffff00
	binary.BigEndian.PutUint32(x[0:4], maskedIP)
	copy(x[4:], infoHash[:])

	seen := make(map[int]struct{}, limit)
	result := make([]int, 0, limit)
	for len(result) < limit {
		sum := sha1.Sum(x)
		x = sum[:]
		for i := 0; i < 5 && len(result) < limit; i++ {
			index := int(binary.BigEndian.Uint32(x[i*4:i*4+4]) % uint32(numPieces))
			if _, ok := seen[index]; ok {
				continue
			}
			seen[index] = struct{}{}
			result = append(result, index)
		}
	}
	return result
}
