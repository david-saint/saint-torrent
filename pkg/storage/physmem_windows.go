package storage

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

var procGlobalMemoryStatusEx = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")

// memoryStatusEx mirrors MEMORYSTATUSEX; only the total physical memory is read.
type memoryStatusEx struct {
	length    uint32
	_         uint32
	totalPhys uint64
	_         [6]uint64
}

// physicalMemory reports the machine's total RAM in bytes.
func physicalMemory() (uint64, bool) {
	status := memoryStatusEx{length: uint32(unsafe.Sizeof(memoryStatusEx{}))}
	if ok, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status))); ok == 0 || status.totalPhys == 0 {
		return 0, false
	}
	return status.totalPhys, true
}
