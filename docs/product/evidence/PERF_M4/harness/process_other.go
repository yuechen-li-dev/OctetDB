//go:build !windows

package main

import "syscall"

func readProcessMetrics() processMetrics {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return processMetrics{UserNS: usage.Utime.Nano(), KernelNS: usage.Stime.Nano(), RSSBytes: uint64(usage.Maxrss) * 1024}
}
