package main

import (
	"fmt"
	"time"

	"sainttorrent/pkg/downloader"
)

func getSpaceActionHelp(isPaused, isCompleted bool) string {
	if isCompleted {
		if isPaused {
			return "Start Seeding"
		}
		return "Stop Seeding"
	}
	if isPaused {
		return "Resume"
	}
	return "Pause"
}

func getIndicator(isPaused, isCompleted bool) string {
	if isPaused {
		if isCompleted {
			return "⏹"
		}
		return "⏸"
	}
	return "▶"
}

func getSpeedStr(isPaused, isCompleted bool, speed float64) string {
	if isPaused {
		if isCompleted {
			return "stopped"
		}
		return "paused"
	}
	if isCompleted {
		return fmt.Sprintf("↑ %-6s", formatSpeed(speed))
	}
	return fmt.Sprintf("↓ %-6s", formatSpeed(speed))
}

func currentTransferSpeed(s *downloader.Session) float64 {
	if s.IsCompleted() {
		return s.CurrentUploadSpeed()
	}
	return s.CurrentSpeed()
}

// sessionETA estimates the time to finish the wanted data from the current download
// speed, returning a compact "~1h 2m" / "~3m 4s" / "~5s" string, or "—" when complete,
// stalled (no speed), or otherwise unknown.
func sessionETA(s *downloader.Session) string {
	if s.IsCompleted() {
		return "—"
	}
	speed := s.CurrentSpeed()
	if speed <= 0 {
		return "—"
	}
	size := s.TotalSize()
	downloaded := int64(s.PercentComplete() / 100.0 * float64(size))
	remaining := size - downloaded
	if remaining <= 0 {
		return "—"
	}
	d := time.Duration(float64(remaining)/speed) * time.Second
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("~%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("~%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("~%ds", int(d.Seconds()))
	}
}
