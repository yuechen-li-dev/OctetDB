# M9 evidence

- `evidence/storage-100k.json`: full-checkpoint/delta WAL and JSON/compact-dedupe independent ablations, per-scenario FLOW sizes, recovery allocations, and canonical hashes.
- `evidence/recovery-100k.json`: 100k snapshot and 10k-tail recovery.
- `evidence/recovery-1m.json`: 1M snapshot and 10k-tail recovery.
- `evidence/performance.json`: Oct and Go controls for memory, fsync-each, and group-16/wait-0 across independent, hot-key, and transfer workloads.
- `summary.json`: normalized milestone result.

Crash, corruption, determinism, dedupe-horizon, and exact-checkpoint equivalence evidence is executable in `internal/m7write/m1_test.go` and `internal/m7write/m2_test.go`; giant raw WAL/snapshot files are intentionally not retained.
