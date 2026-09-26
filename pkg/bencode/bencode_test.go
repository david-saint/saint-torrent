package bencode

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    interface{}
		wantErr bool
	}{
		// Integers
		{"integer zero", "i0e", int64(0), false},
		{"integer positive", "i42e", int64(42), false},
		{"integer negative", "i-42e", int64(-42), false},
		{"integer leading zero", "i03e", int64(3), false},
		{"integer negative zero", "i-0e", int64(0), false},

		// Strings
		{"string normal", "4:spam", "spam", false},
		{"string empty", "0:", "", false},
		{"string with special bytes", "5:\x00\x01\x02\x03\x04", "\x00\x01\x02\x03\x04", false},

		// Lists
		{"empty list", "le", []interface{}{}, false},
		{"list of strings", "l4:spam4:eggse", []interface{}{"spam", "eggs"}, false},
		{"list of mixed types", "l4:spami42ee", []interface{}{"spam", int64(42)}, false},

		// Dictionaries
		{"empty dict", "de", map[string]interface{}{}, false},
		{"dict normal", "d3:cow3:moo4:spam4:eggse", map[string]interface{}{"cow": "moo", "spam": "eggs"}, false},
		{"dict nested", "d4:spamd3:cow3:mooee", map[string]interface{}{"spam": map[string]interface{}{"cow": "moo"}}, false},

		// Malformed edge cases
		{"empty input", "", nil, true},
		{"integer no e", "i42", nil, true},
		{"integer no digits", "ie", nil, true},
		{"integer invalid minus", "i-e", nil, true},
		{"string missing colon", "4spam", nil, true},
		{"string length too long", "10:spam", nil, true},
		{"string negative length", "-4:spam", nil, true},
		{"list unterminated", "l4:spam", nil, true},
		{"dict unterminated", "d3:cow3:moo", nil, true},
		{"dict odd number of elements", "d3:cowe", nil, true},
		{"dict key not string", "di42e3:mooe", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Unmarshal([]byte(tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Unmarshal() = %v (type %T), want %v (type %T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestDecodeReader(t *testing.T) {
	buf := bytes.NewBufferString("i42e")
	got, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode() unexpected error: %v", err)
	}
	if got != int64(42) {
		t.Errorf("Decode() = %v, want %v", got, int64(42))
	}
}

func TestMarshal(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		want    string
		wantErr bool
	}{
		{"integer", int(42), "i42e", false},
		{"int64", int64(-100), "i-100e", false},
		{"string", "spam", "4:spam", false},
		{"bytes", []byte("eggs"), "4:eggs", false},
		{"slice interface", []interface{}{"spam", int64(42)}, "l4:spami42ee", false},
		{"slice string", []string{"spam", "eggs"}, "l4:spam4:eggse", false},
		{"map interface", map[string]interface{}{"cow": "moo", "spam": "eggs"}, "d3:cow3:moo4:spam4:eggse", false},
		{"map string", map[string]string{"cow": "moo"}, "d3:cow3:mooe", false},
		// Check that maps are marshaled with sorted keys
		{"map sorting key check", map[string]interface{}{"b": 2, "a": 1}, "d1:ai1e1:bi2ee", false},
		{"unsupported type", 3.14, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Marshal(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && string(got) != tt.want {
				t.Errorf("Marshal() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEncodeWriter(t *testing.T) {
	var buf bytes.Buffer
	err := Encode(&buf, "spam")
	if err != nil {
		t.Fatalf("Encode() unexpected error: %v", err)
	}
	if buf.String() != "4:spam" {
		t.Errorf("Encode() wrote %q, want %q", buf.String(), "4:spam")
	}
}

// TestUnmarshalDepthLimit verifies that a pathologically nested input is rejected
// with an error instead of recursing until the goroutine stack is exhausted (which
// would crash the whole process — a remote DoS via a malicious peer/tracker).
func TestUnmarshalDepthLimit(t *testing.T) {
	const n = 5000 // well above maxDepth

	// n nested lists: lll...l (no terminators needed; the depth guard trips first).
	if _, err := Unmarshal([]byte(bytes.Repeat([]byte("l"), n))); err == nil {
		t.Error("Unmarshal(deeply nested lists) = nil error, want depth-limit error")
	}

	// n nested dicts via repeated "d1:a" (each opens a dict and its single key).
	deepDict := append(bytes.Repeat([]byte("d1:a"), n), bytes.Repeat([]byte("e"), n)...)
	if _, err := Unmarshal(deepDict); err == nil {
		t.Error("Unmarshal(deeply nested dicts) = nil error, want depth-limit error")
	}

	// FindRawValue walks the same recursive span finder; it must also stay bounded.
	deepInfo := append([]byte("d4:info"), bytes.Repeat([]byte("l"), n)...)
	if _, err := FindRawValue(deepInfo, "info"); err == nil {
		t.Error("FindRawValue(deeply nested value) = nil error, want depth-limit error")
	}

	// A modestly nested structure (within the limit) must still parse fine.
	ok := append(bytes.Repeat([]byte("l"), 50), bytes.Repeat([]byte("e"), 50)...)
	if _, err := Unmarshal(ok); err != nil {
		t.Errorf("Unmarshal(50 nested lists) = %v, want success", err)
	}
}

// TestUnmarshalTokenBudget verifies that decoding stops once an input holds
// more values than its budget, so a flat run of tiny values (each one a heap
// allocation) cannot grow the decoded tree without bound.
func TestUnmarshalTokenBudget(t *testing.T) {
	// A list of three strings is four values.
	list := []byte("l1:a1:b1:ce")
	if _, err := UnmarshalLimited(list, 4); err != nil {
		t.Fatalf("UnmarshalLimited(4 values, budget 4) = %v, want success", err)
	}
	if _, err := UnmarshalLimited(list, 3); !errors.Is(err, errTokenBudget) {
		t.Fatalf("UnmarshalLimited(4 values, budget 3) = %v, want token budget error", err)
	}
	// Dictionary keys count too: one dict, one key, one value.
	if _, err := UnmarshalLimited([]byte("d1:ai1ee"), 2); !errors.Is(err, errTokenBudget) {
		t.Fatalf("UnmarshalLimited(3 values, budget 2) = %v, want token budget error", err)
	}
	if _, err := UnmarshalLimited([]byte("i1e"), 0); !errors.Is(err, errTokenBudget) {
		t.Fatalf("UnmarshalLimited(budget 0) = %v, want token budget error", err)
	}

	// The default budget applies to Unmarshal and DecodePrefix. One repeated
	// empty key keeps this cheap: the lenient decoder stores it once and empty
	// strings box without allocating.
	pairs := DefaultMaxTokens / 2
	over := make([]byte, 0, 4*pairs+2)
	over = append(over, 'd')
	for i := 0; i < pairs; i++ {
		over = append(over, "0:0:"...)
	}
	over = append(over, 'e') // DefaultMaxTokens keys and values plus the dict
	if _, err := Unmarshal(over); !errors.Is(err, errTokenBudget) {
		t.Fatalf("Unmarshal(DefaultMaxTokens+1 values) = %v, want token budget error", err)
	}
	if _, _, err := DecodePrefix(over); !errors.Is(err, errTokenBudget) {
		t.Fatalf("DecodePrefix(DefaultMaxTokens+1 values) = %v, want token budget error", err)
	}
	within := append([]byte{'d'}, over[5:]...) // one pair fewer
	if _, err := Unmarshal(within); err != nil {
		t.Fatalf("Unmarshal(DefaultMaxTokens-1 values) = %v, want success", err)
	}
}

// TestUnmarshalDuplicateKeysFirstWins pins the lenient decoder to the value
// FindRawValue reports for a repeated key. The map used to keep the last value
// while FindRawValue returned the first, so a torrent's info-hash could commit
// to one info dict while its files came from another.
func TestUnmarshalDuplicateKeysFirstWins(t *testing.T) {
	data := []byte("d4:infod4:name5:firste4:infod4:name6:secondee")
	val, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() = %v", err)
	}
	wantInfo := map[string]interface{}{"name": "first"}
	if want := map[string]interface{}{"info": wantInfo}; !reflect.DeepEqual(val, want) {
		t.Fatalf("Unmarshal() = %v, want first value %v", val, want)
	}
	raw, err := FindRawValue(data, "info")
	if err != nil {
		t.Fatalf("FindRawValue() = %v", err)
	}
	fromRaw, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("Unmarshal(raw) = %v", err)
	}
	if !reflect.DeepEqual(fromRaw, wantInfo) {
		t.Fatalf("FindRawValue decoded to %v, map holds %v", fromRaw, wantInfo)
	}
	if _, rest, err := DecodePrefix([]byte("d1:ai1e1:ai2eeX")); err != nil || string(rest) != "X" {
		t.Fatalf("DecodePrefix(duplicate key) rest=%q err=%v, want lenient decode", rest, err)
	}
}

func TestFindUniqueRawValue(t *testing.T) {
	for _, tc := range []struct {
		in, want, err string
	}{
		{in: "d1:ai1e4:infod1:xi1ee1:zi2ee", want: "d1:xi1ee"},
		// Other keys may repeat; only the requested one must be unique.
		{in: "d1:ai1e1:ai2e4:infoi3ee", want: "i3e"},
		{in: "d4:infoi1e1:ai1e4:infoi2ee", err: "duplicate dictionary key"},
		{in: "d1:ai1ee", err: "not found"},
		{in: "d4:infoi1e", err: "unterminated"},
	} {
		got, err := FindUniqueRawValue([]byte(tc.in), "info")
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("FindUniqueRawValue(%q) = %q, %v; want error %q", tc.in, got, err, tc.err)
			}
			continue
		}
		if err != nil || string(got) != tc.want {
			t.Errorf("FindUniqueRawValue(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	// FindRawValue still stops at the first copy.
	if got, err := FindRawValue([]byte("d4:infoi1e4:infoi2ee"), "info"); err != nil || string(got) != "i1e" {
		t.Fatalf("FindRawValue(repeated key) = %q, %v; want the first value", got, err)
	}
}

func TestUnmarshalStrictRejectsDuplicateKeys(t *testing.T) {
	for _, input := range []string{
		"d1:ai1e1:ai2ee",                       // top level
		"d1:xd1:ai1e1:bi2e1:ai3eee",            // nested dict, non-adjacent repeat
		"d1:xld1:ai1e1:ai1eeee",                // dict inside a list
		"d4:infod4:name1:ae4:infod4:name1:bee", // repeated info
	} {
		if _, err := UnmarshalStrict([]byte(input)); err == nil || !strings.Contains(err.Error(), "duplicate dictionary key") {
			t.Errorf("UnmarshalStrict(%q) = %v, want duplicate key error", input, err)
		}
		if _, err := Unmarshal([]byte(input)); err != nil {
			t.Errorf("Unmarshal(%q) = %v, want lenient success", input, err)
		}
	}
	// Unsorted but unique keys stay accepted: some creators emit them.
	val, err := UnmarshalStrict([]byte("d1:bi2e1:ai1ee"))
	if err != nil {
		t.Fatalf("UnmarshalStrict(unsorted keys) = %v, want success", err)
	}
	if want := map[string]interface{}{"a": int64(1), "b": int64(2)}; !reflect.DeepEqual(val, want) {
		t.Fatalf("UnmarshalStrict(unsorted keys) = %v, want %v", val, want)
	}
}

// TestUnmarshalErrorOmitsTrailingBytes keeps untrusted trailing input out of
// error strings, which reach logs and the terminal.
func TestUnmarshalErrorOmitsTrailingBytes(t *testing.T) {
	_, err := Unmarshal([]byte("i1e\x1b]0;spoofed\x07"))
	if err == nil {
		t.Fatal("Unmarshal(trailing data) = nil, want error")
	}
	if strings.Contains(err.Error(), "spoofed") {
		t.Fatalf("error %q echoes trailing input", err)
	}
}
