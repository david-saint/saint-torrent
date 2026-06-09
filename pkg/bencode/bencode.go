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

// Decode reads bencoded data from an io.Reader and returns the parsed value.
func Decode(r io.Reader) (interface{}, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read from reader: %w", err)
	}
	return Unmarshal(data)
}

// Unmarshal decodes a bencoded byte slice and returns the parsed value.
func Unmarshal(data []byte) (interface{}, error) {
	val, rest, err := parse(data, 0)
	if err != nil {
		return nil, err
	}
	if len(rest) > 0 {
		return nil, fmt.Errorf("extra data at end of input: %q", rest)
	}
	return val, nil
}

func parse(data []byte, depth int) (interface{}, []byte, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("empty input")
	}
	if depth > maxDepth {
		return nil, nil, errors.New("bencode value nested too deeply")
	}

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

		// Enforce spec constraints:
		// - "i-0e" is invalid.
		// - "i03e" (leading zeros) is invalid, unless it is "i0e".
		switch numBytes[0] {
		case '-':
			if len(numBytes) == 1 {
				return nil, nil, errors.New("invalid integer: sign only")
			}
			if numBytes[1] == '0' {
				return nil, nil, errors.New("negative zero is invalid")
			}
			if numBytes[1] == '-' {
				return nil, nil, errors.New("multiple negative signs")
			}
		case '0':
			if len(numBytes) > 1 {
				return nil, nil, errors.New("leading zero in integer")
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
			val, rest, err = parse(rest, depth+1)
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
		rest := data[1:]
		for len(rest) > 0 && rest[0] != 'e' {
			// Key MUST be a string
			var keyVal interface{}
			var err error
			keyVal, rest, err = parse(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
			key, ok := keyVal.(string)
			if !ok {
				return nil, nil, errors.New("dictionary key must be a string")
			}

			// Value
			var val interface{}
			val, rest, err = parse(rest, depth+1)
			if err != nil {
				return nil, nil, err
			}
			dict[key] = val
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
func FindRawValue(data []byte, targetKey string) ([]byte, error) {
	if len(data) == 0 || data[0] != 'd' {
		return nil, errors.New("input is not a bencoded dictionary")
	}
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
		key := string(rest[keyStart:keyEnd])
		rest = rest[keyEnd:]

		// Value starts here
		valSpan, err := findValueSpan(rest, 0)
		if err != nil {
			return nil, err
		}

		if key == targetKey {
			return rest[:valSpan], nil
		}

		rest = rest[valSpan:]
	}
	return nil, fmt.Errorf("key %q not found in bencoded dictionary", targetKey)
}
