package bencode

import (
	"bytes"
	"reflect"
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
		{"integer leading zero", "i03e", nil, true},  // spec does not allow leading zeros except for zero itself (let's check standard rule)
		{"integer negative zero", "i-0e", nil, true}, // not allowed
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
