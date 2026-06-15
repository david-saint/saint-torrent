package main

import (
	"fmt"

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
