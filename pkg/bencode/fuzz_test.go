package bencode

import (
	"bytes"
	"testing"
)

// FuzzUnmarshal exercises the strict decoder against arbitrary input. It must
// never panic, and any value it accepts must survive a Marshal/Unmarshal round
// trip with a stable canonical encoding.
func FuzzUnmarshal(f *testing.F) {
	seeds := []string{
		"i42e",
		"i-1e",
		"i-0e",   // invalid: negative zero
		"i03e",   // invalid: leading zero
		"4:spam",
		"0:",
		"le",
		"de",
		"l4:spami42ee",
		"d3:bar4:spam3:fooi42ee",
		// Torrent-file shaped metainfo.
		"d8:announce19:http://t.example/x4:infod6:lengthi12e4:name2:hi12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
		"d1:ad1:bd1:cleeee",
		"",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		val, err := Unmarshal(data)
		if err != nil {
			return
		}
		encoded, err := Marshal(val)
		if err != nil {
			t.Fatalf("Marshal of decoded value failed: %v", err)
		}
		val2, err := Unmarshal(encoded)
		if err != nil {
			t.Fatalf("re-Unmarshal of canonical encoding %q failed: %v", encoded, err)
		}
		reencoded, err := Marshal(val2)
		if err != nil {
			t.Fatalf("re-Marshal failed: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("canonical encoding not stable: %q vs %q", encoded, reencoded)
		}
	})
}

// FuzzDecodePrefix checks that the prefix decoder never panics and that the
// bytes it reports as consumed form a complete, standalone bencoded value.
func FuzzDecodePrefix(f *testing.F) {
	for _, s := range []string{"i42eTRAILING", "4:spamX", "d1:a1:be", "lee", ""} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, rest, err := DecodePrefix(data)
		if err != nil {
			return
		}
		if len(rest) > len(data) {
			t.Fatalf("rest (%d) longer than input (%d)", len(rest), len(data))
		}
		consumed := data[:len(data)-len(rest)]
		if _, err := Unmarshal(consumed); err != nil {
			t.Fatalf("consumed prefix %q does not Unmarshal cleanly: %v", consumed, err)
		}
	})
}

// FuzzValueSpan checks that the span finder never panics and always reports a
// span within the bounds of the input, and that the span is idempotent.
func FuzzValueSpan(f *testing.F) {
	for _, s := range []string{"i42e", "d3:fooi42eeextra", "l4:spam4:eggse", "0:", ""} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		n, err := ValueSpan(data)
		if err != nil {
			return
		}
		if n < 1 || n > len(data) {
			t.Fatalf("span %d out of bounds for input of length %d", n, len(data))
		}
		// Spanning the reported prefix again must yield the same length.
		if n2, err := ValueSpan(data[:n]); err != nil || n2 != n {
			t.Fatalf("span not idempotent: first=%d second=%d err=%v", n, n2, err)
		}
	})
}
