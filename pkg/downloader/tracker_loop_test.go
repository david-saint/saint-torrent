package downloader

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestTrackerLogIDRedactsPathAndQuery(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "http passkey path and query",
			raw:  "https://tracker.example.com/abc123passkey/announce?token=secret",
			want: "https://tracker.example.com",
		},
		{
			name: "host port retained",
			raw:  "http://tracker.example.com:8080/announce?passkey=secret",
			want: "http://tracker.example.com:8080",
		},
		{
			name: "udp tracker",
			raw:  "udp://tracker.example.com:6969/announce",
			want: "udp://tracker.example.com:6969",
		},
		{
			name: "invalid",
			raw:  "://bad tracker",
			want: "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trackerLogID(tt.raw); got != tt.want {
				t.Fatalf("trackerLogID(%q) = %q; want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestTrackerLogErrRedactsURLErrorURL(t *testing.T) {
	err := trackerLogErr(&url.Error{
		Op:  "Get",
		URL: "https://tracker.example.com/passkey/announce?token=secret",
		Err: errors.New("connection refused"),
	})
	got := err.Error()
	if strings.Contains(got, "passkey") || strings.Contains(got, "token=secret") {
		t.Fatalf("trackerLogErr leaked sensitive URL detail: %q", got)
	}
	if !strings.Contains(got, "connection refused") {
		t.Fatalf("trackerLogErr lost useful error detail: %q", got)
	}
}
