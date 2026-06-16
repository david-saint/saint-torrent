package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"net"
)

const allowedFastSetSize = 10

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
		bitfield[i/8] |= 1 << (7 - (i % 8))
		hasAny = true
	}
	return bitfield, hasAny, hasAll
}

func fullPieceBitfield(numPieces int) []byte {
	if numPieces <= 0 {
		return nil
	}
	bitfield := make([]byte, (numPieces+7)/8)
	for i := 0; i < numPieces; i++ {
		bitfield[i/8] |= 1 << (7 - (i % 8))
	}
	return bitfield
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
