# PERF-M4 environment

Primary system matrix:

- Date: 2026-08-23, America/Los_Angeles.
- Host: AMD Ryzen 7 7700X, 8 cores / 16 logical processors, 32 GiB RAM.
- Physical storage: SHPP41-2000GM, 2 TB NVMe SSD.
- Host OS: Windows 11 Pro 10.0.26200 build 26200.
- Primary guest: WSL2 Arch Linux, kernel
  `6.6.87.2-microsoft-standard-WSL2`, `/dev/sdf` ext4, ordered data mode.
- Go: `go1.27.0-X:nodwarf5 linux/amd64`, `GOAMD64=v1`, default
  `GOMAXPROCS=16`.
- OctetDB: public `github.com/yuechen-li-dev/octetdb@v0.2.0`, tag commit
  `76a0659ae9125c8c9b689fe155f2ff30a4b30fc9`; independent module resolution
  in `harness/go.mod`.
- Oct: `ca22ab8dfc20ac6d6c59dd34976789cd2c84ad2e` (2026-08-22), generated
  source records this provenance in result metadata.
- PostgreSQL: native WSL PostgreSQL 18.6, data checksums enabled,
  `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`; loopback TCP;
  no replica.
- TigerBeetle: 0.17.9, commit
  `cc1c06a924e49b11089c521b2209d34c92caaf18`, ReleaseSafe Zig 0.14.1,
  Direct I/O, one production-mode replica, 1 GiB cache budget, 10 GiB
  storage limit, WSL loopback TCP.
- Build: `go build -trimpath`; safe Go, GC enabled, no unsafe/cgo/manual
  allocator in either OctetDB lane. The Go runtime may use normal OS cgo
  shims internally on Windows; the product does not import cgo.
- No CPU affinity or governor control was applied. Linux hardware counters
  were available (`perf_event_paranoid=2`) and collected in separately labeled
  diagnostic runs.

Cross-check matrix:

- Go `go1.26.2 windows/amd64`, `GOAMD64=v1`, NTFS on the same NVMe.
- PostgreSQL 17.11 in Docker Desktop 29.1.3, published on loopback port 54329,
  durability settings on, checksums off.
- The Windows durable sync path showed two reproducible latency regimes.
  Contemporaneous S0 controls moved together, so Windows is retained as a
  portability/outlier study rather than the primary product comparison.

The primary WSL PostgreSQL resource sidecars report the sum of process RSS,
which double-counts shared pages and is therefore an upper bound. The harness
JSON reports Go client RSS separately. OctetDB RSS is the single embedded
process. TigerBeetle reports its server and client separately.

