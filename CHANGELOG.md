# Changelog

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
