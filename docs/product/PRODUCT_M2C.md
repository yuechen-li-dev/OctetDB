# OCTETDB-PRODUCT-M2C — v0.2 consolidation and release candidate

## 1. Verdict

Success

The unreleased keyed, catalog, and scan work is one product surface with one
front door. No database feature was added; the only code-surface change was
consolidation around one database-wide transaction type plus release-contract
tests and benchmarks.

## 2. Product state entering M2C

`v0.1.0` publicly exposed the fixed account/transfer `Open`/`DB` API. Main had
three additive development layers: `OpenKeyed` global JSON keys, `OpenCatalog`
catalog-scoped datasets, and Dataset scans plus optional Oct query syntax. The
behavior was correct, but `CatalogDB`, `CatalogTx`, `DatasetTx`, and
`Dataset.Mutate` made catalog and transaction history visible as competing
concepts. Primary docs also contained milestone names.

## 3. Candidate API inventory

Every package identifier added after v0.1 is classified below. Methods are
grouped under their receiver.

| Classification | Identifiers |
| --- | --- |
| keep: canonical | `OpenCatalog`, `Database`, `Database.Bucket`, `ListBuckets`, `Catalog`, `DatabaseID`, `Mutate`, `Snapshot`, `Close`; `Bucket`, `Bucket.Dataset`, `ListDatasets`; `Dataset`, `Get`, `Info`, `Scan`; `Tx`, `Tx.Get`, `Put`, `Delete`; `Mutation` |
| keep: typed command/value ergonomics | `KeyedOptions`, `DefaultKeyedOptions`, `KeyedCommand`, `KeyedDecision`, `DecodeResult`, `KeyedRejection`, `Reject`, `RejectWithResult` |
| keep: topology metadata | `Catalog`, `DatabaseInfo`, `BucketInfo`, `DatasetInfo`, `DatasetBounds`, `DatasetOptions`, `DefaultDatasetOptions`, `DatasetKind`, `KeyedJSON`, `DatasetOrigin`, `GoCatalog` |
| keep: scan | `ScanAction`, `ScanContinue`, `ScanStop`, `DatasetRecord`, `DatasetRecord.Decode`, `ScanDataset[T]` |
| keep: deprecated compatibility | `OpenKeyed`, `KeyedDB`, `KeyedTx`, `KeyedMutation`, `SubmitKeyed`, `GetKeyed`, `SnapshotKeyed`, and `KeyedTx.Get`/`Put`/`Delete`/`KeyedDB.Close` |
| rename before v0.2 | `CatalogDB` → `Database`; `CatalogTx` → `Tx`; `CatalogMutation` → `Mutation` |
| merge with another concept | `Dataset.Mutate` and `DatasetMutation` merged into `Database.Mutate`/`Mutation` |
| make internal/remove | `DatasetTx` removed; Dataset-scoped transaction methods are expressed as `Tx` methods taking a Dataset |
| documentation clarification | `KeyedOptions`/`KeyedCommand`/`KeyedDecision` remain named because v0.1 already owns `Options`, `Command`, and `Result`; they serve canonical v0.2 and the deprecated keyed compatibility path |

API overlap result: `DB` is protected v0.1 API; `Database` is canonical v0.2;
`KeyedDB` is deprecated pre-v0.2 compatibility. `Tx` is canonical v0.2 and
`KeyedTx` is deprecated compatibility. `CatalogDB` and `CatalogTx` no longer
exist. Dataset is the canonical leaf handle. The unavoidable legacy names are
subordinate and marked deprecated in `go doc`.

## 4. Canonical onboarding decision

`OpenCatalog` is the only v0.2 onboarding path. It returns `*Database`, owns one
user-selected directory, and leads directly to Bucket/Dataset handles.

Documentation hierarchy is explicit:

1. canonical: catalog Dataset Go API;
2. compatibility: v0.1 account API and deprecated pre-v0.2 global-key API;
3. advanced: optional Oct query/specialization.

## 5. OpenKeyed relationship

Choice C2: separate legacy candidate API. `OpenKeyed` is retained for developers
who created `keyed-json-v1` directories before v0.2, marked deprecated, and not
taught as a new-application option. It keeps its distinct format. It is not an
implicit Bucket/Dataset, does not open catalog directories, and is never
silently upgraded.

## 6. Database/Bucket/Dataset contract

The frozen topology is Database → Bucket → Dataset → Records. Record identity
is `(Dataset, key)`. Command identity and dedupe are database-wide. One mutation
may cross Datasets. Catalog entries and stable IDs are durable semantic
topology; backend key encodings and files are physical implementation details.

`Dataset.Info` exposes name, Bucket, stable ID, kind, origin, type identity, and
bounds. Catalog names reject path-like nesting.

## 7. Mutation/transaction model

One `Database.Mutate` call accepts one stable `KeyedCommand`, invokes one `Tx`
callback, validates Dataset and transaction bounds, and writes one atomic WAL
decision. `Tx.Get`/`Put`/`Delete` always take a Dataset, making cross-Dataset
scope explicit. A nil error accepts; `Reject`/`RejectWithResult` durably rejects
and discards writes; another error aborts without consuming the command ID.
Escaped transaction handles fail.

## 8. Dataset read/query model

`Dataset.Get`, `Tx.Get`, and `Tx.Put` use ordinary Go destinations/values.
`ScanDataset[T]` is the typed scan path. JSON is visible only in the advanced
raw `DatasetRecord` surface and durable decision representation.

Scan guarantees are now package and method documentation: read-only; ascending
record-key order; stable logical snapshot; detached raw/typed values; context
cancellation between records; synchronous early stop; and no WAL, sequence, or
dedupe mutation. This is deterministic Dataset enumeration, not a planner or
optimizer.

## 9. Oct optional specialization boundary

Go Dataset scans do not require Oct. The separate advanced Oct example shows
`query`, `filter`, `map`, `take`, and `Query.First`/`Any`/`Count`; the parser
lowers that syntax to Oct FLOW. It compiled and passed with zero interpreted
fallback at Oct revision
`ca22ab8dfc20ac6d6c59dd34976789cd2c84ad2e`
(`v1.0.0-61-gca22ab8d`, execution identity `gooct-cli`). OctetDB has no runtime,
compiler, MIR, or FLOW import. Generated host-facade ABI is revision-scoped and
has no cross-revision promise.

## 10. Storage-format consolidation

Three fail-closed model markers remain:

- `accounts-v1`: released v0.1 public model;
- `keyed-json-v1`: pre-v0.2 global-key candidate compatibility;
- `catalog-keyed-json-v1`: canonical v0.2 model.

Normal v0.2 docs create only the catalog format. Each opener requires its exact
marker. No automatic migration or reinterpretation was added.

## 11. Compatibility matrix

| Source | v0.2 status | Opener | Policy |
| --- | --- | --- | --- |
| v0.1 account database | supported | `Open` | published API/format preserved |
| pre-v0.2 keyed candidate | deprecated compatibility | `OpenKeyed` | distinct format, manual export/recreate if moving |
| v0.2 catalog database | canonical | `OpenCatalog` | new-user format |

## 12. Golden-app consolidation

Inventory, webhook, order lifecycle, and job worker all use `OpenCatalog`,
`Database`, Dataset handles, and `Database.Mutate`/`Tx`; no service or HTTP code
changed. Ready-job and low-stock discovery use `ScanDataset`. The separate
module has no internal imports or Oct dependency; race tests pass.

Direct lines naming `octetdb.` are inventory 20, webhook 7, order 11, and job
19. PRODUCT-M2 had 14/5/9/11; the increase is durable Bucket/Dataset declaration
plus the two requested scans, not transitional adapters. Compared with M2B
(16/7/11/13), only inventory/job query code grew. Required concepts are now
OpenCatalog, Bucket/Dataset, stable command, Database.Mutate/Tx, typed point
read/result, rejection, and typed scan.

## 13. Long-query behavior

On Windows/amd64, Ryzen 7 7700X, the representative 100,000-record contract test
measured a 26.6245 ms serialized scan while a concurrent mutation waited
29.1562 ms. The mutation could not enter until all 100,000 callbacks completed.
This blocking behavior is public and documented; M2C did not add MVCC.

## 14. Corruption/recovery

Canonical product tests cover catalog checksum corruption, snapshot corruption,
complete WAL corruption, incomplete final WAL append truncation, format
mismatch in both directions, and Dataset kind/type/bound mismatch. Public
categories are `ErrorCorruption`, `ErrorIncompatible`, and `ErrorCapacity` as
appropriate.

A close/reopen integration test proves catalog topology, both Datasets,
cross-Dataset state, retained dedupe, point reads, and logical query results.
Queries have no persistence mechanism because they are read-only.

## 15. Defaults

Database defaults are 100,000 live records, 100,000 retained decisions, 1 MiB
per encoded value/result, and 4 MiB of writes per command. Dataset record/value
bounds inherit database values unless lowered. Keys and command IDs have fixed
4 KiB limits; rejection codes 1 KiB. First use chooses only a directory. There
are no query knobs.

## 16. Public docs and quickstart

README, Getting Started, package docs, Durability, Recovery, Changelog, release
notes, and examples now tell one v0.2 story without milestone archaeology. The
canonical quickstart demonstrates OpenCatalog, Bucket/Dataset, an ordinary
struct, atomic mutation, stable ID, point read, typed scan, and close/reopen.
`go doc` opens with the canonical topology and transaction/query contract;
compatibility APIs are deprecated. The Oct example is separate and advanced.

## 17. Fresh-module result

Pass. `TestCandidateExternalModule` creates an unrelated temporary module,
requires v0.2.0, and uses a local `replace` solely as candidate wiring. Its
exported-API-only program proves two Datasets, cross-Dataset atomic mutation,
restart, exact duplicate retry, point reads, and low-stock typed scan. `go list
-deps` contains no research engine, TigerBeetle, PostgreSQL/pgx, benchmark
harness, Database Scheduler, or Oct compiler internals.

## 18. Fresh-agent result

Pass without correction. Given only candidate README, package docs, and Getting
Started, a clean-context coding agent produced a compiling unrelated-module
program with OpenCatalog, one Bucket/two Datasets, database-wide stable IDs,
cross-Dataset Tx access, durable rejection, and deterministic typed scan. It did
not invent SQL, select OpenKeyed, treat paths as Datasets, inspect internals, or
require Oct. Exact source and ledger are retained in the M2C evidence file.

## 19. Performance sanity

Windows/amd64 Ryzen 7 7700X candidate sanity measurements:

| Path | Result |
| --- | ---: |
| Dataset point read | 16.403 µs/op, 246 B, 5 allocs |
| durable atomic mutation | 355.077 µs/op, 2,227 B, 23 allocs |
| typed filter + Take(10), 100k source | 17.729–21.065 µs/op, 10,360 B, 259 allocs |
| raw full 100k scan | 9.031 ms/op, 4.8 MB, 100,000 allocs |
| typed full 100k filter scan | 63.259 ms/op, 28.0 MB, 700,002 allocs |

Historical QUERY-M0 evidence measured about 18 µs Take(10), 6.70 ms raw Count,
and 60.7 ms typed filter at 100k. The current range is directionally consistent;
no severe consolidation regression or optimization work was found/attempted.

## 20. Release gate table

| Gate | Status |
| --- | --- |
| canonical API chosen | PASS — OpenCatalog/Database/Tx |
| v0.1 compatibility | PASS — account tests and format retained |
| catalog recovery | PASS |
| cross-dataset atomicity | PASS |
| global idempotency | PASS |
| deterministic scans | PASS |
| ready-job query | PASS |
| low-stock query | PASS |
| corruption handling | PASS |
| long-scan behavior documented | PASS |
| golden apps | PASS — root and separate-module race lanes |
| fresh external module | PASS — local replace recorded |
| fresh coding agent | PASS without correction |
| `go test -race ./...` | PASS |
| `go vet ./...` | PASS |
| `git diff --check` | PASS |

## 21. Tag/release result

`v0.2.0` was created from the clean validated release commit and published as a
GitHub release using the candidate release notes. The tag was not moved or
rewritten.

## 22. Post-tag proof

Pass. With `GOPROXY=https://proxy.golang.org` and
`OCTETDB_EXTERNAL_VERSION=v0.2.0`, the retained `TestExternalModule` created a
clean unrelated module, downloaded the tag with no `replace`, and proved
restart, dedupe, catalog, cross-Dataset mutation, scan, and dependency
direction.

## 23. pkg.go.dev status

The public Go proxy resolves `github.com/yuechen-li-dev/octetdb@v0.2.0` and its
module checksum. The immediate pkg.go.dev page check had not indexed the new tag
and showed its normal request-to-add state. Package comments and `go doc` are
ready; crawler/indexing timing does not block Success.

## 24. Known v0.2 limitations

Single process/open handle/replica; no directory locking, SQL, secondary
indexes, joins, grouping/sorting, MVCC, replication, migration, online backup,
schema reflection, or large-blob path. Values are JSON and memory-resident.
Long scans serialize writes. Dedupe is exact only inside its horizon.
Cancellation stops waiting but not admitted durable work. Windows snapshot
rename has weaker power-loss metadata guarantees than POSIX directory sync.
Oct generated host ABI remains revision-scoped.

## 25. Required product decision

1. Ready to release v0.2.0 and proceed to PRODUCT-M3

## 26. Exactly one next recommendation

**PRODUCT-M3 — human/LLM integration benchmark against the released v0.2 public surface.**
