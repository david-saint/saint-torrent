package storage

import (
	"strings"
	"testing"
)

// TestFactoriesReturnUntypedNilOnError: a factory that returned its failed
// (*FileStorage)(nil) boxed a nil pointer in a non-nil Storage. A magnet whose
// metadata named a reserved or over-long file then got past the caller's
// st == nil check and crashed the process on the first storage call.
func TestFactoriesReturnUntypedNilOnError(t *testing.T) {
	factories := map[string]Factory{"NewStorage": NewStorage}
	for _, backend := range []Backend{BackendFile, BackendMMap, BackendMemory} {
		factory, err := FactoryForBackend(backend)
		if err != nil {
			continue // mmap is unsupported on Windows
		}
		factories[string(backend)] = factory
	}
	bad := map[string][]FileInfo{
		"reserved name": {{Path: ".dht_nodes", Length: 1}},
		"duplicate":     {{Path: "a", Length: 1}, {Path: "A", Length: 1}},
		"too long":      {{Path: strings.Repeat("x", 300), Length: 1}},
	}
	for name, factory := range factories {
		for input, files := range bad {
			st, err := factory(t.TempDir(), files, 16)
			if err == nil {
				if input == "too long" {
					// The mem backend never touches the disk, so a long name is fine there.
					_ = st.Close()
					continue
				}
				t.Fatalf("%s(%s): succeeded, want an error", name, input)
			}
			if st != nil {
				t.Fatalf("%s(%s): returned %T(%v) with error %v, want an untyped nil Storage", name, input, st, st, err)
			}
		}
	}
	st, err := NewStorageWithBackend(BackendFile, t.TempDir(), bad["reserved name"], 16)
	if err == nil || st != nil {
		t.Fatalf("NewStorageWithBackend = %T(%v), %v; want untyped nil and an error", st, st, err)
	}
}
