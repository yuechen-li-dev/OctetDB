//go:build !windows

package main

import "time"

func benchNowNS() int64 { return time.Now().UnixNano() }
