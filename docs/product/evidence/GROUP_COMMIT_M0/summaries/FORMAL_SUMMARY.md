# Formal summary

The authoritative narrative and tables are in [`GROUP_COMMIT_M0.md`](../../../GROUP_COMMIT_M0.md). The matched primary WSL/ext4 c8 gains versus pinned v0.2.0 are:

| Workload | gain | new p99 |
| --- | ---: | ---: |
| W1 | 3.64x | 2.65 ms |
| W2 | 3.63x | 2.57 ms |
| W3 | 3.37x | 2.45 ms |
| W4 | 3.53x | 2.60 ms |

All formal correctness flags and `go test -race ./...` passed. The normal dev harness completed in 12.069 seconds on WSL/ext4 and 3.874 seconds on Windows/NTFS.
