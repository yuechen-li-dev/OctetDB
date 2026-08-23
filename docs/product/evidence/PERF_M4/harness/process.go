package main

type processMetrics struct {
	UserNS, KernelNS int64
	RSSBytes         uint64
}
