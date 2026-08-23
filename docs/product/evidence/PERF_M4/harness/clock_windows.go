//go:build windows

package main

import "syscall"

var (
	performanceCounter   = syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceCounter")
	performanceFrequency = func() int64 {
		var value int64
		syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceFrequency").Call(uintptr(unsafePointer(&value)))
		return value
	}()
)

func benchNowNS() int64 {
	var value int64
	performanceCounter.Call(uintptr(unsafePointer(&value)))
	return value * 1_000_000_000 / performanceFrequency
}
