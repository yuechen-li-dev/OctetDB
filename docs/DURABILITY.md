# Durability contract

OctetDB v0.1 has one public durability mode. `Submit` and `SubmitBatch` return
success only after all new decisions in the offered batch have been written as
one checksummed WAL frame and the WAL file's `Sync` call has succeeded. State is
applied in memory only after that synchronization. Rejected domain decisions
are durable decisions too. A duplicate whose retained result is already durable
does not append another record.

## Failure behavior

- **Process crash:** a synchronized complete frame is replayed on `Open`, even
  if the process crashed before applying it in memory or replying to the caller.
- **OS crash or power loss:** OctetDB uses Go's file `Sync`; the guarantee is
  conditional on the operating system, filesystem, drive, and controller
  honoring that request. OctetDB cannot protect against hardware that falsely
  reports persistence.
- **Incomplete WAL tail:** recovery treats a short final length, payload, or
  checksum as an interrupted append, ignores it, and truncates the WAL to the
  last complete frame. A structurally complete frame with a bad checksum fails
  open as corruption.
- **Write or sync failure:** the write returns `ErrorStorage`. The handle is
  poisoned and rejects later submissions with `ErrorPoisoned`; close it and
  diagnose the storage before reopening.
- **Snapshot:** OctetDB writes and syncs `snapshot.oct.tmp`, atomically renames it
  to `snapshot.oct`, syncs the containing directory on POSIX, then resets and
  syncs `wal.oct`. A crash before WAL reset is safe because replay skips records
  already covered by the snapshot. Windows lacks the same directory-sync step,
  so power loss around snapshot rename has a weaker metadata-persistence
  guarantee and may cause a fail-closed recovery.
- **Restart:** `Open` validates the format marker and snapshot, restores the
  snapshot, validates the WAL, and replays complete records after the snapshot
  sequence.
- **Duplicate retry:** command IDs are exact within the last `DedupeHorizon`
  unique decisions. A retained ID returns its original sequence and decision
  with `Result.Duplicate=true`, including after snapshot and restart. Once an ID
  ages out of that explicitly bounded horizon, it may be applied again.

Cancellation is honored while waiting for local admission. Once admitted, a
submission runs through synchronization and returns its definitive result. If a
caller disconnects after admission and cannot observe the reply, it must retry
the same command ID.

These are single-process, single-replica durability guarantees. OctetDB v0.1
does not provide replication, failover availability, or protection from loss of
the database directory.
