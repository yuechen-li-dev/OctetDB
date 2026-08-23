# Recovery and storage layout

An OctetDB database is one application-owned directory:

| File | Role |
| --- | --- |
| `FORMAT` | Text format/model/provenance marker; created and synced before the first open |
| `wal.oct` | Versioned binary WAL with a checksummed header and checksummed batch frames |
| `snapshot.oct` | Optional versioned binary snapshot with a SHA-256 payload digest |
| `snapshot.oct.tmp` | Temporary snapshot output; not authoritative until renamed |

Format version 1 uses the fixed `accounts-v1` product model and the safe-Go
engine. There are no segments, metadata database, or lock file in v0.1. The
application must ensure only one process/handle opens a directory at a time.

On `Open`, OctetDB validates `FORMAT`, loads and validates `snapshot.oct` when
present, opens and validates `wal.oct`, discards an incomplete final append, and
replays complete records whose sequence follows the snapshot. Sequence gaps,
checksum failures, impossible recovered state, malformed records, and malformed
snapshots fail closed with `ErrorCorruption`. Unsupported marker, WAL, snapshot,
or model versions fail with `ErrorIncompatible`. Ordinary filesystem failures
return `ErrorStorage`.

Pre-1.0 formats may change. There is deliberately no migration machinery in
M0; a future incompatible release must reject this format or provide an
explicit offline upgrade tool. Copy the whole directory for backup only while
the DB is closed, or after an application-coordinated snapshot and quiescence.
