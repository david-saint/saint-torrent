package logging

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestConfigureFromEnvDisabledByDefault(t *testing.T) {
	t.Setenv("SAINTTORRENT_LOG", "")
	if err := ConfigureFromEnv(); err != nil {
		t.Fatalf("ConfigureFromEnv: %v", err)
	}
	defer Close()
	if Enabled() {
		t.Fatal("logger should be disabled without SAINTTORRENT_LOG")
	}
}

func TestConfigFromEnvRejectsZeroBackups(t *testing.T) {
	t.Setenv("SAINTTORRENT_LOG", filepath.Join(t.TempDir(), "debug.log"))
	t.Setenv("SAINTTORRENT_LOG_MAX_BACKUPS", "0")
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("expected zero max backups to be rejected")
	}
}

func TestLoggerWritesStructuredJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "debug.log")
	logger, err := New(Config{Path: path, Level: LevelDebug})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := logger.Log(LevelInfo, "tracker_announce", String("tracker", "http://tracker.example/announce"), Int("peers", 7)); err != nil {
		t.Fatalf("Log: %v", err)
	}
	if err := logger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var entry struct {
		Level  string         `json:"level"`
		Event  string         `json:"event"`
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, data)
	}
	if entry.Level != "info" || entry.Event != "tracker_announce" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if entry.Fields["tracker"] != "http://tracker.example/announce" || entry.Fields["peers"].(float64) != 7 {
		t.Fatalf("unexpected fields: %+v", entry.Fields)
	}
}

func TestLoggerCreatesPrivateLogFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix owner-only mode bits through os.FileMode")
	}

	path := filepath.Join(t.TempDir(), "debug.log")
	logger, err := New(Config{Path: path, Level: LevelDebug})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := logger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != logFileMode {
		t.Fatalf("log file mode = %o; want %o", got, logFileMode)
	}
}

func TestLoggerRotatesFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "debug.log")
	logger, err := New(Config{Path: path, Level: LevelDebug, MaxSizeBytes: 240, MaxBackups: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < 8; i++ {
		if err := logger.Log(LevelDebug, "rotation_test", Int("seq", i), String("payload", strings.Repeat("x", 60))); err != nil {
			t.Fatalf("Log %d: %v", i, err)
		}
	}
	if err := logger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("current log missing: %v", err)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("rotated log missing: %v", err)
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("expected only two backups, stat .3 err=%v", err)
	}

	files := []string{path, path + ".1", path + ".2"}
	lines := 0
	for _, file := range files {
		f, err := os.Open(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("open %s: %v", file, err)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lines++
			var entry map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				t.Fatalf("%s contains invalid JSON: %v", file, err)
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", file, err)
		}
		f.Close()
	}
	if lines == 0 {
		t.Fatal("expected retained log lines after rotation")
	}

	if runtime.GOOS != "windows" {
		for _, file := range files {
			info, err := os.Stat(file)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				t.Fatalf("stat %s: %v", file, err)
			}
			if got := info.Mode().Perm(); got != logFileMode {
				t.Fatalf("%s mode = %o; want %o", file, got, logFileMode)
			}
		}
	}
}

func TestGlobalLoggerHonorsLevel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "debug.log")
	if err := Configure(Config{Path: path, Level: LevelWarn}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	defer Close()
	if !Enabled() {
		t.Fatal("logger should be enabled")
	}
	Debug("suppressed")
	Warn("written", String("component", "test"))
	if err := Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(data)
	if strings.Contains(text, "suppressed") {
		t.Fatalf("debug line should have been suppressed:\n%s", text)
	}
	if !strings.Contains(text, `"event":"written"`) {
		t.Fatalf("warning line missing:\n%s", text)
	}
}
