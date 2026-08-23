# Durability contract

For the canonical catalog database, a successful `Database.Mutate` means one
complete checksummed WAL frame containing the command decision and every
cross-dataset write was written and synchronized. Concurrent mutations may
share one internal storage flush; each keeps its individual durable
acknowledgement, database-wide order, and exact result semantics. Staged state
is visible only to later serialized callbacks in the same group until the
shared synchronization succeeds. A durable rejection is a decision too;
duplicate retry of a retained ID appends nothing.

Catalog structure is synchronized separately before `Bucket` or `Dataset`
returns. OctetDB writes a checksummed complete catalog to a temporary file,
synchronizes it, atomically renames it, and synchronizes the containing
directory where supported. Catalog operations are administrative and cannot
run inside mutations.

## Failure behavior

- **Process crash:** a synchronized complete decision is replayed even if the
  process crashed before applying it in memory or replying.
- **OS crash or power loss:** durability depends on the operating system,
  filesystem, drive, and controller honoring Go file `Sync`.
- **Incomplete final WAL append:** recovery truncates a short final header,
  payload, or checksum to the last complete frame.
- **Corrupt complete frame:** checksum, sequence, JSON, dataset identity, or
  bound violations fail open with `ErrorCorruption` or `ErrorCapacity` as
  applicable.
- **Write or sync failure:** no affected group member is acknowledged; the
  handle is poisoned and all later operations return `ErrorPoisoned` until
  close/reopen, preventing undurable staged state from being observed or used.
- **Duplicate retry:** exact accepted or rejected decisions survive snapshot and
  restart while retained inside `DedupeHorizon`. An expired ID may apply again.

## Snapshots and close

`Database.Snapshot` and `Close` write and synchronize a deterministic temporary
snapshot, rename it over the authoritative snapshot, sync the directory on
POSIX, then truncate and synchronize the WAL. Snapshot data includes every
dataset record, the sequence frontier, and retained command decisions. Catalog
topology is in its separately synchronized catalog file.

Recovery safely skips WAL decisions already covered by the snapshot. Windows
does not provide the same directory-sync step, so power loss around snapshot
rename has weaker metadata persistence and may cause a fail-closed open.

Queries have no persistence path. `Dataset.Scan` and `ScanDataset` are read-only
and do not append to the WAL, advance sequence, or change dedupe state. Their
results survive restart because the underlying catalog and records survive.

## Compatibility paths

The v0.1 account `Submit`/`SubmitBatch` API retains its published equivalent
WAL rule. Deprecated pre-v0.2 `SubmitKeyed` retains the same rule for its
distinct global-key format. The formats are not interchangeable.

Cancellation is honored while waiting for admission. Once a mutation is
admitted, it runs through synchronization and returns a definitive outcome. If
a caller loses the reply, it retries the same command ID.

These are single-process, single-replica guarantees. OctetDB does not protect
against loss of the database directory or provide replication/failover.
