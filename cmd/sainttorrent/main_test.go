package main

import (
	"strings"
	"testing"
)

func TestGetSpaceActionHelp(t *testing.T) {
	tests := []struct {
		isPaused    bool
		isCompleted bool
		want        string
	}{
		{isPaused: false, isCompleted: false, want: "Pause"},
		{isPaused: true, isCompleted: false, want: "Resume"},
		{isPaused: false, isCompleted: true, want: "Stop Seeding"},
		{isPaused: true, isCompleted: true, want: "Start Seeding"},
	}

	for _, tt := range tests {
		got := getSpaceActionHelp(tt.isPaused, tt.isCompleted)
		if got != tt.want {
			t.Errorf("getSpaceActionHelp(paused=%v, completed=%v) = %q; want %q",
				tt.isPaused, tt.isCompleted, got, tt.want)
		}
	}
}

func TestGetIndicator(t *testing.T) {
	tests := []struct {
		isPaused    bool
		isCompleted bool
		want        string
	}{
		{isPaused: false, isCompleted: false, want: "▶"},
		{isPaused: true, isCompleted: false, want: "⏸"},
		{isPaused: false, isCompleted: true, want: "▶"},
		{isPaused: true, isCompleted: true, want: "⏹"},
	}

	for _, tt := range tests {
		got := getIndicator(tt.isPaused, tt.isCompleted)
		if got != tt.want {
			t.Errorf("getIndicator(paused=%v, completed=%v) = %q; want %q",
				tt.isPaused, tt.isCompleted, got, tt.want)
		}
	}
}

func TestGetSpeedStr(t *testing.T) {
	tests := []struct {
		isPaused    bool
		isCompleted bool
		speed       float64
		want        string
	}{
		{isPaused: true, isCompleted: false, speed: 0, want: "paused"},
		{isPaused: true, isCompleted: true, speed: 0, want: "stopped"},
		{isPaused: false, isCompleted: false, speed: 0, want: "↓ 0 B/s"},
		{isPaused: false, isCompleted: false, speed: 1024, want: "↓ 1.0 KB/s"},
		{isPaused: false, isCompleted: true, speed: 50 * 1024, want: "↓ 50.0 KB/s"},
	}

	for _, tt := range tests {
		got := getSpeedStr(tt.isPaused, tt.isCompleted, tt.speed)
		// Strip extra spaces or check strings.Contains to deal with alignment formatting
		if tt.isPaused {
			if got != tt.want {
				t.Errorf("getSpeedStr(paused=%v, completed=%v, speed=%f) = %q; want %q",
					tt.isPaused, tt.isCompleted, tt.speed, got, tt.want)
			}
		} else {
			if !strings.Contains(got, "↓") || !strings.Contains(got, strings.TrimPrefix(tt.want, "↓ ")) {
				t.Errorf("getSpeedStr(paused=%v, completed=%v, speed=%f) = %q; want containing %q",
					tt.isPaused, tt.isCompleted, tt.speed, got, tt.want)
			}
		}
	}
}
