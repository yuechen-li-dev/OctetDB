# Recovery and storage layout

An OctetDB database is one application-owned directory. The v0.1 account model
uses:

| File | Role |
| --- | --- |
| `FORMAT` | Text format/model/provenance marker; created and synced before the first open |
| `wal.oct` | Versioned binary WAL with a checksummed header and checksummed batch frames |
| `snapshot.oct` | Optional versioned binary snapshot with a SHA-256 payload digest |
| `snapshot.oct.tmp` | Temporary snapshot output; not authoritative until renamed |

Format version 1 uses the fixed `accounts-v1` product model and the safe-Go
engine. There are no segments, metadata database, or lock file in v0.1. The
application must ensure only one process/handle opens a directory at a time.

The candidate v0.2 catalog/keyed model has a distinct `keyed-json-v1` FORMAT
marker and these backend-owned files:

| File | Role |
| --- | --- |
| `FORMAT` | Text model marker, distinct from `accounts-v1` |
| `catalog` | Checksummed durable Database/Bucket/Dataset topology and stable identities |
| `catalog.tmp` | Interrupted catalog write; never authoritative |
| `wal` | Checksummed JSON decision frames containing backend-encoded dataset/key identities |
| `snapshot` | Optional checksummed deterministic keyed-state snapshot |
| `snapshot.tmp` | Interrupted snapshot output; never authoritative |

This layout is not a direct materialization of the logical tree. Buckets and
datasets do not create directories, and records do not create files. A private
compact key encoding maps `(DatasetID, RecordKey)` into the current keyed
backend; it is not semantic identity and is not a public storage address.

`OpenCatalog` first recovers the keyed snapshot and WAL, then validates the
catalog checksum and topology. Every recovered record must carry a dataset ID
present in the catalog. A missing catalog beside existing keyed decisions,
unknown dataset IDs, duplicate dataset IDs, or malformed topology fails closed.
Catalog creation/update is synchronized before callers can submit records to a
new dataset, so recovery cannot silently reinterpret a record after topology
loss.

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

`OpenKeyed` remains able to open PRODUCT-M2 global-key directories. It does not
silently upgrade them to a catalog database; `OpenCatalog` rejects keyed data
that has no durable catalog identity. The released v0.1 account directory and
API remain unchanged.
