package logging

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// goupnp logs malformed SSDP replies with the raw header line; a LAN host can
// put terminal escape sequences there. Through StdLogWriter they must land in
// the debug log stripped of control characters.
func TestStdLogWriterForwardsSanitizedLinesToDebugLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "debug.log")
	if err := Configure(Config{Path: path, Level: LevelDebug}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	defer Close()

	std := log.New(StdLogWriter(), "", 0)
	std.Printf("httpu: error while parsing response: malformed MIME header line: X-Evil: \x1b]0;PWNED\x07\x1b[2J\u202e")
	if err := Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var entry struct {
		Level  string            `json:"level"`
		Event  string            `json:"event"`
		Fields map[string]string `json:"fields"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, data)
	}
	if entry.Event != "stdlog" || entry.Level != "warn" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	msg := entry.Fields["message"]
	if !strings.Contains(msg, "X-Evil:") || !strings.Contains(msg, "]0;PWNED") {
		t.Fatalf("message lost its content: %q", msg)
	}
	for _, r := range msg {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			t.Fatalf("message kept control character %U: %q", r, msg)
		}
	}
}

func TestStdLogWriterDiscardsWhenLoggingDisabled(t *testing.T) {
	if err := Configure(Config{}); err != nil {
		t.Fatal(err)
	}
	line := []byte("httpu: \x1b[2J\n")
	n, err := StdLogWriter().Write(line)
	if n != len(line) || err != nil {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(line))
	}
}
