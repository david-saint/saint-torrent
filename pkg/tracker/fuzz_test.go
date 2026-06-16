package tracker

import "testing"

// FuzzParseTrackerResponse ensures the (now bencode-backed) tracker response
// parser never panics on hostile input and that any peers it returns are
// structurally well formed.
func FuzzParseTrackerResponse(f *testing.F) {
	seeds := []string{
		"d8:intervali1800e5:peers0:e",
		"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e",
		"d8:intervali900e8:completei5e10:incompletei2e5:peersld2:ip9:127.0.0.14:porti6881eeee",
		"d8:intervali900e6:peers618:\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x1a\xe1e",
		"d14:failure reason3:bade",
		"d8:intervali1e15:warning message2:hie",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		resp, err := ParseTrackerResponse(data)
		if err != nil {
			return
		}
		for _, p := range resp.Peers {
			if p.IP == nil {
				t.Fatalf("parsed peer with nil IP")
			}
		}
	})
}
