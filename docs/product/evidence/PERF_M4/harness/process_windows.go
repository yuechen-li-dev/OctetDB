//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

type filetime struct{ Low, High uint32 }
type memoryCounters struct {
	CB                                                                                                                                 uint32
	PageFaultCount                                                                                                                     uint32
	PeakWorkingSetSize, WorkingSetSize                                                                                                 uintptr
	QuotaPeakPagedPoolUsage, QuotaPagedPoolUsage, QuotaPeakNonPagedPoolUsage, QuotaNonPagedPoolUsage, PagefileUsage, PeakPagefileUsage uintptr
}

func readProcessMetrics() processMetrics {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	psapi := syscall.NewLazyDLL("psapi.dll")
	handle, _, _ := kernel32.NewProc("GetCurrentProcess").Call()
	var created, exited, kernel, user filetime
	kernel32.NewProc("GetProcessTimes").Call(handle, uintptr(unsafe.Pointer(&created)), uintptr(unsafe.Pointer(&exited)), uintptr(unsafe.Pointer(&kernel)), uintptr(unsafe.Pointer(&user)))
	var memory memoryCounters
	memory.CB = uint32(unsafe.Sizeof(memory))
	psapi.NewProc("GetProcessMemoryInfo").Call(handle, uintptr(unsafe.Pointer(&memory)), uintptr(memory.CB))
	toNS := func(value filetime) int64 { return int64(uint64(value.High)<<32|uint64(value.Low)) * 100 }
	return processMetrics{UserNS: toNS(user), KernelNS: toNS(kernel), RSSBytes: uint64(memory.WorkingSetSize)}
}
