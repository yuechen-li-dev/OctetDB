//go:build windows

package main

import "unsafe"

func unsafePointer(value *int64) unsafe.Pointer { return unsafe.Pointer(value) }
