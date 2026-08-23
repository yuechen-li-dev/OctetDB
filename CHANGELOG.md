# Changelog

## Unreleased — PRODUCT-M2B

- Add a durable shallow Database/Bucket/Dataset catalog with stable database and
  dataset identities, catalog enumeration, dataset-scoped keys, compatibility
  checks, semantic dataset bounds, and fail-closed catalog recovery.
- Add public single-dataset and atomic cross-dataset mutation surfaces while
  retaining database-wide command idempotency.
- Move the four candidate v0.2 golden applications to logical datasets without
  changing their service or HTTP layers. Preserve the released v0.1 account
  path and PRODUCT-M2 `OpenKeyed` compatibility surface with distinct format
  markers.
- Add QUERY-M0 `Dataset.Scan` and typed `ScanDataset` over KeyedJSON with
  deterministic key order, stable serialized read semantics, per-record
  cancellation, detached raw values, and synchronous early stop.
- Add ready-job and low-stock product queries without SQL, a fluent query
  builder, secondary indexes, goroutines, or a second iterator runtime.

## Unreleased

- Add an application-defined keyed-state workflow with bounded defaults,
  atomic validated JSON mutations, exact durable command decisions, point reads,
  explicit snapshots, and close-time snapshots.
- Add four conventional Go golden integrations and progressive adoption docs.
- Preserve the v0.1.0 account/transfer API unchanged; the additive behavior is a
  candidate v0.2.0 release and is not tagged by PRODUCT-M2.

All notable public-product changes are recorded here. OctetDB follows semantic
versioning for its Go module. Before 1.0, minor releases may contain breaking API
or database-format changes; incompatible formats are detected and rejected.

## v0.1.0 - 2026-08-22

First public release.

- Added the public `octetdb` package for a bounded account/transfer domain.
- Added synchronized WAL durability, snapshots, recovery, and bounded exact
  command-ID deduplication.
- Added typed public error categories and documented cancellation semantics.
- Added format metadata (`FORMAT`, `wal.oct`, and optional `snapshot.oct`).
- Added clean external-module lifecycle and idempotency verification.

See [the v0.1.0 release notes](docs/product/V0.1.0_RELEASE_NOTES.md) for scope,
installation, limitations, and performance-evidence context.
