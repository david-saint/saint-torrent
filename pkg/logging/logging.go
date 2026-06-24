// Package logging provides opt-in structured debug logging for field diagnostics.
package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Level is the minimum severity a logger writes.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

const (
	defaultMaxSizeBytes = 10 * 1024 * 1024
	defaultMaxBackups   = 3
)

var levelNames = map[Level]string{
	LevelDebug: "debug",
	LevelInfo:  "info",
	LevelWarn:  "warn",
	LevelError: "error",
}

// Config controls a rotating JSON-lines logger. An empty Path disables logging.
type Config struct {
	Path         string
	Level        Level
	MaxSizeBytes int64
	MaxBackups   int
}

// Field is one structured key/value pair attached to a log event.
type Field struct {
	Key   string
	Value any
}

// Logger writes structured JSON-line events to a rotating file.
type Logger struct {
	mu           sync.Mutex
	path         string
	level        Level
	maxSizeBytes int64
	maxBackups   int
	file         *os.File
	size         int64
	now          func() time.Time
}

var defaultLogger atomic.Pointer[Logger]

// ParseLevel parses debug, info, warn, warning, or error.
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "debug":
		return LevelDebug, nil
	case "info":
		return LevelInfo, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	default:
		return LevelDebug, fmt.Errorf("invalid log level %q", s)
	}
}

func (l Level) String() string {
	if name, ok := levelNames[l]; ok {
		return name
	}
	return "debug"
}

// ConfigFromEnv reads SAINTTORRENT_LOG, SAINTTORRENT_LOG_LEVEL,
// SAINTTORRENT_LOG_MAX_SIZE, and SAINTTORRENT_LOG_MAX_BACKUPS.
func ConfigFromEnv() (Config, error) {
	path := strings.TrimSpace(os.Getenv("SAINTTORRENT_LOG"))
	if path == "" {
		return Config{}, nil
	}

	level, err := ParseLevel(os.Getenv("SAINTTORRENT_LOG_LEVEL"))
	if err != nil {
		return Config{}, err
	}

	cfg := Config{Path: path, Level: level}
	if raw := strings.TrimSpace(os.Getenv("SAINTTORRENT_LOG_MAX_SIZE")); raw != "" {
		size, err := parseSize(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SAINTTORRENT_LOG_MAX_SIZE: %w", err)
		}
		cfg.MaxSizeBytes = size
	}
	if raw := strings.TrimSpace(os.Getenv("SAINTTORRENT_LOG_MAX_BACKUPS")); raw != "" {
		backups, err := strconv.Atoi(raw)
		if err != nil || backups < 0 {
			return Config{}, fmt.Errorf("invalid SAINTTORRENT_LOG_MAX_BACKUPS %q", raw)
		}
		cfg.MaxBackups = backups
	}
	return cfg, nil
}

// ConfigureFromEnv configures the default logger from environment variables.
func ConfigureFromEnv() error {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return err
	}
	return Configure(cfg)
}

// Configure replaces the process-wide default logger. Passing an empty Path
// disables logging and closes the previous logger.
func Configure(cfg Config) error {
	var next *Logger
	var err error
	if strings.TrimSpace(cfg.Path) != "" {
		next, err = New(cfg)
		if err != nil {
			return err
		}
	}
	old := defaultLogger.Swap(next)
	if old != nil {
		_ = old.Close()
	}
	return nil
}

// Close closes and disables the process-wide default logger.
func Close() error {
	old := defaultLogger.Swap(nil)
	if old == nil {
		return nil
	}
	return old.Close()
}

// Enabled reports whether the default logger is configured.
func Enabled() bool {
	return defaultLogger.Load() != nil
}

// EnabledFor reports whether the default logger is configured for level.
func EnabledFor(level Level) bool {
	l := defaultLogger.Load()
	return l != nil && l.Enabled(level)
}

// Debug writes a debug event to the default logger.
func Debug(event string, fields ...Field) {
	Log(LevelDebug, event, fields...)
}

// Info writes an info event to the default logger.
func Info(event string, fields ...Field) {
	Log(LevelInfo, event, fields...)
}

// Warn writes a warning event to the default logger.
func Warn(event string, fields ...Field) {
	Log(LevelWarn, event, fields...)
}

// Error writes an error event to the default logger.
func Error(event string, fields ...Field) {
	Log(LevelError, event, fields...)
}

// Log writes an event to the default logger.
func Log(level Level, event string, fields ...Field) {
	l := defaultLogger.Load()
	if l == nil {
		return
	}
	_ = l.Log(level, event, fields...)
}

// New creates a file logger. The caller owns Close.
func New(cfg Config) (*Logger, error) {
	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		return nil, fmt.Errorf("log path is required")
	}
	if cfg.MaxSizeBytes <= 0 {
		cfg.MaxSizeBytes = defaultMaxSizeBytes
	}
	if cfg.MaxBackups == 0 {
		cfg.MaxBackups = defaultMaxBackups
	}
	if cfg.MaxBackups < 0 {
		return nil, fmt.Errorf("max backups must be non-negative")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Logger{
		path:         path,
		level:        cfg.Level,
		maxSizeBytes: cfg.MaxSizeBytes,
		maxBackups:   cfg.MaxBackups,
		file:         f,
		size:         stat.Size(),
		now:          time.Now,
	}, nil
}

// Enabled reports whether this logger writes level.
func (l *Logger) Enabled(level Level) bool {
	return l != nil && level >= l.level
}

// Close closes this logger.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

// Log writes one structured JSON-line event.
func (l *Logger) Log(level Level, event string, fields ...Field) error {
	if !l.Enabled(level) {
		return nil
	}
	entry := struct {
		TS     string         `json:"ts"`
		Level  string         `json:"level"`
		Event  string         `json:"event"`
		Fields map[string]any `json:"fields,omitempty"`
	}{
		TS:    l.now().UTC().Format(time.RFC3339Nano),
		Level: level.String(),
		Event: event,
	}
	if len(fields) > 0 {
		entry.Fields = make(map[string]any, len(fields))
		for _, f := range fields {
			if f.Key == "" {
				continue
			}
			entry.Fields[f.Key] = f.Value
		}
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return fmt.Errorf("logger is closed")
	}
	if l.maxBackups > 0 && l.size > 0 && l.size+int64(len(line)) > l.maxSizeBytes {
		if err := l.rotateLocked(); err != nil {
			return err
		}
	}
	n, err := l.file.Write(line)
	l.size += int64(n)
	return err
}

func (l *Logger) rotateLocked() error {
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return err
		}
		l.file = nil
	}

	oldest := fmt.Sprintf("%s.%d", l.path, l.maxBackups)
	_ = os.Remove(oldest)
	for i := l.maxBackups - 1; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", l.path, i)
		dst := fmt.Sprintf("%s.%d", l.path, i+1)
		if _, err := os.Stat(src); err == nil {
			if err := os.Rename(src, dst); err != nil {
				return err
			}
		}
	}
	if _, err := os.Stat(l.path); err == nil {
		if err := os.Rename(l.path, l.path+".1"); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	l.file = f
	l.size = 0
	return nil
}

func parseSize(s string) (int64, error) {
	raw := strings.TrimSpace(strings.ToLower(s))
	if raw == "" {
		return 0, fmt.Errorf("empty size")
	}
	multiplier := int64(1)
	for _, suffix := range []struct {
		text string
		mul  int64
	}{
		{"kib", 1024},
		{"kb", 1024},
		{"k", 1024},
		{"mib", 1024 * 1024},
		{"mb", 1024 * 1024},
		{"m", 1024 * 1024},
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1024 * 1024 * 1024},
		{"g", 1024 * 1024 * 1024},
	} {
		if strings.HasSuffix(raw, suffix.text) {
			multiplier = suffix.mul
			raw = strings.TrimSpace(strings.TrimSuffix(raw, suffix.text))
			break
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return value * multiplier, nil
}

// String adds a string field.
func String(key, value string) Field { return Field{Key: key, Value: value} }

// Int adds an int field.
func Int(key string, value int) Field { return Field{Key: key, Value: value} }

// Int64 adds an int64 field.
func Int64(key string, value int64) Field { return Field{Key: key, Value: value} }

// Uint16 adds a uint16 field.
func Uint16(key string, value uint16) Field { return Field{Key: key, Value: value} }

// Bool adds a bool field.
func Bool(key string, value bool) Field { return Field{Key: key, Value: value} }

// Duration adds a duration field in milliseconds.
func Duration(key string, value time.Duration) Field {
	return Field{Key: key + "_ms", Value: value.Milliseconds()}
}

// Err adds an error field when err is non-nil.
func Err(err error) Field {
	if err == nil {
		return Field{}
	}
	return Field{Key: "error", Value: err.Error()}
}
