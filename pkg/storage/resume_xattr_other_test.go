//go:build !windows && !darwin && !linux

package storage

import "testing"

func setLastUsedXattr(*testing.T, string) {}
