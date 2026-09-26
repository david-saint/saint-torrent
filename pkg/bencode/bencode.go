// Package bencode implements encoding and decoding of bencoded data.
package bencode

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
)

// maxDepth bounds how deeply bencode containers (lists/dicts) may nest. Real
// torrent, tracker, and DHT payloads nest only a few levels; this guard stops a
// maliciously deep input from exhausting the goroutine stack via recursion.
const maxDepth = 100

// DefaultMaxTokens is the token budget Unmarshal, UnmarshalStrict and
// DecodePrefix allow, matching libtorrent's max_decode_tokens. Every decoded
// value costs one token (dictionary keys included), so the budget bounds how
// many heap objects one input can make the decoder allocate. Callers decoding
// small, frequent messages should pass a tighter budget to UnmarshalLimited.
const DefaultMaxTokens = 3_000_000

// errTokenBudget is returned once an input holds more values than its budget.
var errTokenBudget = errors.New("bencode input exceeds token budget")

// decoder carries per-call decode limits through the recursive parse.
type decoder struct {
	tokens int  // values still allowed before the input is rejected
	strict bool // reject any dictionary that repeats a key
}

// Decode reads bencoded data from an io.Reader and returns the parsed value.
func Decode(r io.Reader) (interface{}, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read from reader: %w", err)
	}
	return Unmarshal(data)
}

// Unmarshal decodes a bencoded byte slice and returns the parsed value. When a
// dictionary repeats a key the first value wins, matching FindRawValue (and
// libtorrent), so the decoded map and the raw span of a key never disagree.
func Unmarshal(data []byte) (interface{}, error) {
	return UnmarshalLimited(data, DefaultMaxTokens)
}

// UnmarshalLimited is Unmarshal with a caller-chosen token budget: decoding
// fails once the input holds more than maxTokens values.
func UnmarshalLimited(data []byte, maxTokens int) (interface{}, error) {
	d := decoder{tokens: maxTokens}
	return d.unmarshal(data)
}

// UnmarshalStrict is Unmarshal that also rejects any dictionary, at any depth,
// that repeats a key. It is meant for a torrent's info dictionary, the bytes
// the info-hash commits to: BEP 3 requires unique keys, and a repeated key
// there lets two readers of the same bytes disagree about what a torrent
// contains. The rest of a .torrent, and tracker and DHT traffic, keep using
// the lenient Unmarshal, as libtorrent does.
func UnmarshalStrict(data []byte) (interface{}, error) {
	d := decoder{tokens: DefaultMaxTokens, strict: true}
	return d.unmarshal(data)
}

func (d *decoder) unmarshal(data []byte) (interface{}, error) {
	val, rest, err := d.parse(data, 0)
	if err != nil {
		return nil, err
	}
	if len(rest) > 0 {
		// Report only the size: the trailing bytes are untrusted and can be
		// megabytes long.
		return nil, fmt.Errorf("extra data at end of input: %d bytes", len(rest))
	}
	return val, nil
}

// DecodePrefix decodes the single bencoded value at the start of data and
// returns it along with any unconsumed trailing bytes. Unlike Unmarshal it does
// not itself reject trailing data, leaving the caller to decide what the
// remainder means (e.g. raw piece bytes after a BEP 9 metadata dictionary, or a
// caller-specific "trailing data" error). Otherwise it decodes exactly like
// Unmarshal, leniency included: integers with leading zeros or negative zero
// are accepted for non-compliant trackers and peers, a repeated dictionary key
// keeps its first value, and string lengths must fit the input.
func DecodePrefix(data []byte) (value interface{}, rest []byte, err error) {
	d := decoder{tokens: DefaultMaxTokens}
	return d.parse(data, 0)
}

// ValueSpan returns the number of bytes occupied by the bencoded value at the
// start of data, without materializing it. It is used to locate the boundary
// between a bencoded value and trailing bytes (for example, splitting a BEP 9
// ut_metadata dictionary from the piece data that follows it).
func ValueSpan(data []byte) (int, error) {
	return findValueSpan(data, 0)
}

func (d *decoder) parse(data []byte, depth int) (interface{}, []byte, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("empty input")
	}
	if depth > maxDepth {
		return nil, nil, errors.New("bencode value nested too deeply")
	}
	if d.tokens <= 0 {
		return nil, nil, errTokenBudget
	}
	d.tokens--

	switch data[0] {
	case 'i':
		// Integer format: i<number>e
		end := bytes.IndexByte(data, 'e')
		if end == -1 {
			return nil, nil, errors.New("unterminated integer")
		}
		numBytes := data[1:end]
		if len(numBytes) == 0 {
			return nil, nil, errors.New("empty integer")
		}

		// Enforce spec constraints but remain lenient for compatibility:
		// - Allow leading zeros and negative zero for non-compliant tracker/peer implementations.
		// - Ensure sign is followed by digits.
		switch numBytes[0] {
		case '-':
			if len(numBytes) == 1 {
				return nil, nil, errors.New("invalid integer: sign only")
			}
			if numBytes[1] == '-' {
				return nil, nil, errors.New("multiple negative signs")
			}
		}

		// Ensure all chars in range are digits (skipping negative sign)
		startIdx := 0
		if numBytes[0] == '-' {
			startIdx = 1
		}
		for i := startIdx; i < len(numBytes); i++ {
			if numBytes[i] < '0' || numBytes[i] > '9' {
				return nil, nil, fmt.Errorf("invalid character %q in integer", numBytes[i])
			}
		}

		val, err := strconv.ParseInt(string(numBytes), 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse integer: %w", err)
		}
		return val, data[end+1:], nil

	case 'l':
		// List format: l<elements>e
		list := make([]interface{}, 0)
		rest := data[1:]
		for len(rest) > 0 && rest[0] != 'e' {
			var val interface{}
			var err error
			val, rest, err = d.parse(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
			list = append(list, val)
		}
		if len(rest) == 0 {
			return nil, nil, errors.New("unterminated list")
		}
		return list, rest[1:], nil

	case 'd':
		// Dictionary format: d<key><value>e
		dict := make(map[string]interface{})
		// maxKey is the largest key seen so far. Canonical input has sorted
		// keys, so a key above it cannot be a repeat and skips the map probe.
		var maxKey string
		rest := data[1:]
		for len(rest) > 0 && rest[0] != 'e' {
			// Key MUST be a string
			var keyVal interface{}
			var err error
			keyVal, rest, err = d.parse(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
			key, ok := keyVal.(string)
			if !ok {
				return nil, nil, errors.New("dictionary key must be a string")
			}
			dup := false
			if len(dict) > 0 && key <= maxKey {
				_, dup = dict[key]
			} else {
				maxKey = key
			}
			if dup && d.strict {
				return nil, nil, fmt.Errorf("duplicate dictionary key %q", key)
			}

			// Value
			var val interface{}
			val, rest, err = d.parse(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
			if !dup {
				dict[key] = val
			}
		}
		if len(rest) == 0 {
			return nil, nil, errors.New("unterminated dictionary")
		}
		return dict, rest[1:], nil

	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// String format: <length>:<data>
		colon := bytes.IndexByte(data, ':')
		if colon == -1 {
			return nil, nil, errors.New("missing colon in string")
		}
		lenStr := data[:colon]
		length, err := strconv.Atoi(string(lenStr))
		if err != nil {
			return nil, nil, fmt.Errorf("invalid string length: %w", err)
		}
		if length < 0 {
			return nil, nil, errors.New("negative string length")
		}
		if length > len(data)-colon-1 {
			return nil, nil, errors.New("string length exceeds data size")
		}
		return string(data[colon+1 : colon+1+length]), data[colon+1+length:], nil

	default:
		return nil, nil, fmt.Errorf("unexpected character: %q", data[0])
	}
}

// Encode writes the bencoded representation of val to an io.Writer.
func Encode(w io.Writer, val interface{}) error {
	if val == nil {
		return errors.New("cannot encode nil value")
	}

	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		_, err := fmt.Fprintf(w, "i%de", v.Int())
		return err
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		_, err := fmt.Fprintf(w, "i%de", v.Uint())
		return err
	case reflect.String:
		str := v.String()
		_, err := fmt.Fprintf(w, "%d:%s", len(str), str)
		return err
	case reflect.Slice, reflect.Array:
		// Specialized encoding for byte slices (strings in bencode)
		if v.Type().Elem().Kind() == reflect.Uint8 {
			var bytesVal []byte
			if v.Kind() == reflect.Slice {
				bytesVal = v.Bytes()
			} else {
				// Array: copy to slice
				bytesVal = make([]byte, v.Len())
				reflect.Copy(reflect.ValueOf(bytesVal), v)
			}
			_, err := fmt.Fprintf(w, "%d:", len(bytesVal))
			if err != nil {
				return err
			}
			_, err = w.Write(bytesVal)
			return err
		}

		// Otherwise, encode as list
		_, err := w.Write([]byte{'l'})
		if err != nil {
			return err
		}
		for i := 0; i < v.Len(); i++ {
			err := Encode(w, v.Index(i).Interface())
			if err != nil {
				return err
			}
		}
		_, err = w.Write([]byte{'e'})
		return err

	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return errors.New("map key must be string")
		}
		_, err := w.Write([]byte{'d'})
		if err != nil {
			return err
		}

		// Get all keys and sort them alphabetically
		keys := v.MapKeys()
		keyStrings := make([]string, len(keys))
		for i, k := range keys {
			keyStrings[i] = k.String()
		}
		sort.Strings(keyStrings)

		for _, kStr := range keyStrings {
			// Write key
			_, err := fmt.Fprintf(w, "%d:%s", len(kStr), kStr)
			if err != nil {
				return err
			}
			// Write value
			valVal := v.MapIndex(reflect.ValueOf(kStr))
			err = Encode(w, valVal.Interface())
			if err != nil {
				return err
			}
		}
		_, err = w.Write([]byte{'e'})
		return err

	default:
		return fmt.Errorf("unsupported type: %s", v.Type())
	}
}

// Marshal encodes a Go value to bencoded byte slice.
func Marshal(val interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := Encode(&buf, val)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// findValueSpan returns the number of bytes that the bencoded value at the start of data occupies.
func findValueSpan(data []byte, depth int) (int, error) {
	if len(data) == 0 {
		return 0, errors.New("empty input")
	}
	if depth > maxDepth {
		return 0, errors.New("bencode value nested too deeply")
	}
	switch data[0] {
	case 'i':
		end := bytes.IndexByte(data, 'e')
		if end == -1 {
			return 0, errors.New("unterminated integer")
		}
		return end + 1, nil
	case 'l':
		rest := data[1:]
		consumed := 1
		for len(rest) > 0 && rest[0] != 'e' {
			span, err := findValueSpan(rest, depth+1)
			if err != nil {
				return 0, err
			}
			consumed += span
			rest = rest[span:]
		}
		if len(rest) == 0 {
			return 0, errors.New("unterminated list")
		}
		return consumed + 1, nil
	case 'd':
		rest := data[1:]
		consumed := 1
		for len(rest) > 0 && rest[0] != 'e' {
			// Key (must be a string)
			keySpan, err := findValueSpan(rest, depth+1)
			if err != nil {
				return 0, err
			}
			consumed += keySpan
			rest = rest[keySpan:]
			if len(rest) == 0 || rest[0] == 'e' {
				return 0, errors.New("dictionary key without value")
			}
			// Value
			valSpan, err := findValueSpan(rest, depth+1)
			if err != nil {
				return 0, err
			}
			consumed += valSpan
			rest = rest[valSpan:]
		}
		if len(rest) == 0 {
			return 0, errors.New("unterminated dictionary")
		}
		return consumed + 1, nil
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		colon := bytes.IndexByte(data, ':')
		if colon == -1 {
			return 0, errors.New("missing colon in string")
		}
		lenStr := data[:colon]
		length, err := strconv.Atoi(string(lenStr))
		if err != nil {
			return 0, fmt.Errorf("invalid string length: %w", err)
		}
		if length < 0 {
			return 0, errors.New("negative string length")
		}
		if length > len(data)-colon-1 {
			return 0, errors.New("string length exceeds data size")
		}
		totalLen := colon + 1 + length
		return totalLen, nil
	default:
		return 0, fmt.Errorf("unexpected character: %q", data[0])
	}
}

// FindRawValue scans a bencoded dictionary from the start of data and returns the exact raw byte span of targetKey's value at the root level.
// A repeated key yields its first value, the one Unmarshal keeps.
func FindRawValue(data []byte, targetKey string) ([]byte, error) {
	return findRootValue(data, targetKey, false)
}

// FindUniqueRawValue is FindRawValue that also fails when the root dictionary
// holds targetKey more than once, so no reader of data can pick another copy
// of the value. It scans the whole dictionary.
func FindUniqueRawValue(data []byte, targetKey string) ([]byte, error) {
	return findRootValue(data, targetKey, true)
}

func findRootValue(data []byte, targetKey string, unique bool) ([]byte, error) {
	if len(data) == 0 || data[0] != 'd' {
		return nil, errors.New("input is not a bencoded dictionary")
	}
	var found []byte
	rest := data[1:]
	for len(rest) > 0 && rest[0] != 'e' {
		// Key must be a string: <length>:<data>
		if rest[0] < '0' || rest[0] > '9' {
			return nil, fmt.Errorf("unexpected character %q looking for dictionary key", rest[0])
		}
		colon := bytes.IndexByte(rest, ':')
		if colon == -1 {
			return nil, errors.New("missing colon in dictionary key")
		}
		lenStr := rest[:colon]
		length, err := strconv.Atoi(string(lenStr))
		if err != nil {
			return nil, fmt.Errorf("invalid dictionary key length: %w", err)
		}
		if length < 0 {
			return nil, errors.New("negative dictionary key length")
		}
		keyStart := colon + 1
		if length > len(rest)-keyStart {
			return nil, errors.New("dictionary key length exceeds remaining data")
		}
		keyEnd := keyStart + length
		key := rest[keyStart:keyEnd]
		rest = rest[keyEnd:]

		// Value starts here
		valSpan, err := findValueSpan(rest, 0)
		if err != nil {
			return nil, err
		}

		if string(key) == targetKey {
			if found != nil {
				return nil, fmt.Errorf("duplicate dictionary key %q", targetKey)
			}
			found = rest[:valSpan]
			if !unique {
				return found, nil
			}
		}

		rest = rest[valSpan:]
	}
	if found == nil {
		return nil, fmt.Errorf("key %q not found in bencoded dictionary", targetKey)
	}
	if len(rest) == 0 {
		return nil, errors.New("unterminated dictionary")
	}
	return found, nil
}
