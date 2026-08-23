# Changelog

All notable public product changes are recorded here. OctetDB follows semantic
versioning. Before 1.0, a minor release may change APIs or formats; incompatible
directories fail closed.

## v0.2.0 - 2026-08-22

- Add the canonical `OpenCatalog` → `Database` → `Bucket` → `Dataset` Go API.
- Add ordinary-struct point reads and one database-wide `Mutate`/`Tx` boundary
  for atomic cross-dataset writes and durable accepted/rejected decisions.
- Add durable catalog identity, Dataset-scoped keys, metadata enumeration,
  type/kind/bound compatibility checks, and close-time snapshots.
- Add deterministic read-only `Dataset.Scan` and typed `ScanDataset[T]` with
  ascending key order, stable serialized snapshot semantics, detached values,
  cancellation, and synchronous early stop.
- Keep the v0.1 account API and format supported unchanged.
- Deprecate the separate pre-v0.2 `OpenKeyed` global-key candidate path; its
  format remains readable only through that compatibility API and is never
  reinterpreted as a catalog database.
- Document optional Oct `filter`/`map`/`take` query specialization separately;
  the Go package has no Oct runtime/compiler dependency.

Known limitation: scans serialize mutations for their duration. v0.2 adds no
SQL, indexes, joins, MVCC, replication, migration, directory lock, or online
backup.

See [candidate release notes](docs/product/V0.2.0_RELEASE_NOTES.md).

## v0.1.0 - 2026-08-22

First public release.

- Added the public bounded account/transfer API.
- Added synchronized WAL durability, snapshots, recovery, and bounded exact
  command-ID deduplication.
- Added typed public error categories and documented cancellation semantics.
- Added format metadata and clean external-module verification.

See [v0.1.0 release notes](docs/product/V0.1.0_RELEASE_NOTES.md).
