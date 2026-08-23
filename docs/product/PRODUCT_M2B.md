# OCTETDB-PRODUCT-M2B — Structured Catalog and Dataset Tree

## 1. Verdict

Success

## 2. Research question

Can OctetDB replace PRODUCT-M2's application-maintained global key prefixes
with a durable, shallow, typed logical database tree without requiring Oct,
adding record queries or indexes, changing physical record layout, or damaging
the released v0.1 account model?

Yes. `OpenCatalog` adds one product-owned semantic catalog, sane-default Go
declaration, dataset-scoped record identity, deterministic enumeration, and
database-wide atomic mutations. It reuses the proven keyed JSON WAL/snapshot
engine as a private backend and gives catalog databases their own format marker.

## 3. Existing keyed namespace

PRODUCT-M2 exposes `OpenKeyed`, `KeyedDB`, `GetKeyed`, and `SubmitKeyed` over one
`map[string][]byte`. WAL mutations store a global string `Key`; snapshots store
the same global string map. The product directory contains `FORMAT`, `wal`, and
an optional `snapshot`.

The golden apps therefore encoded topology as application conventions:
`items/widget`, escaped `events/<id>`, `orders/<id>`, and `jobs/<id>`. Nothing in
OctetDB knew whether `jobs/123` meant bucket, dataset, record kind, or merely a
string. Prefix spelling and escaping belonged to each adapter, and identical
record IDs in different domains were distinct only because the application
remembered to prefix them.

That API remains present for PRODUCT-M2 candidate compatibility. It is not
silently upgraded because existing keys have no recoverable dataset identity.

## 4. Catalog model

The implemented product model is deliberately minimal:

```text
Catalog
  DatabaseInfo { ID }
  BucketInfo { Name }
  DatasetInfo {
    ID, BucketName, Name, Kind, Origin, TypeIdentity, Bounds
  }
```

The durable internal state additionally carries `NextDatasetID` and the
bucket/dataset maps needed for stable allocation and lookup. There are no
descriptions, timestamps, arbitrary attributes, storage driver names, paths,
index definitions, or unused ordering fields.

Only `KeyedJSON` is implemented. `OctagonRecord` and `OctagonRecordTable` are
not product-ready storage kinds in the current Go engine, so M2B does not claim
them. `Origin` is currently `GoCatalog`; it reserves the explicit provenance
needed for a later compiled Oct-authored value without creating a plugin system.

## 5. Database/Bucket/Dataset semantics

```text
Database
└── Bucket
    └── Dataset
        └── Records
```

`OpenCatalog` creates or recovers one database identity. `Bucket` creates or
opens exactly one first-level grouping. `Dataset` creates or opens a leaf
storage object. Only datasets contain records. There is no recursive child API,
so `bucket/dataset/subdataset` cannot be expressed.

Bucket was not ceremonial in the evidence: the four unchanged service domains
map naturally to inventory, event ingress, commerce, and worker ownership. It
adds predictable grouping and enumeration without leaking filesystem layout.

## 6. Why `Catalog`, not `Index`

The new structure describes what data exists and how it is named. It does not
accelerate lookup. Calling it an index would spend the term needed later for
primary, secondary, sorted, dense, and derived indexes and would confuse
topology with access paths. `Manifest` remains available for package/build
metadata. The durable file is simply `catalog`; `Catalog.oct` is reserved for a
future authored source and is not engine state.

## 7. Go sane-default API

The additive public path is:

```go
db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
inventory, err := db.Bucket(ctx, "inventory")
items, err := inventory.Dataset(ctx, "items", octetdb.DatasetOptions{
    TypeIdentity: "example.Item/v1",
})

decision, err := items.Mutate(ctx, octetdb.KeyedCommand{ID: commandID},
    func(tx *octetdb.DatasetTx) (any, error) {
        return item, tx.Put(item.ID, item)
    })
```

`Dataset.Get` provides point reads. `Dataset.Mutate` is the small single-leaf
path. `CatalogDB.Mutate` plus `CatalogTx.Get/Put/Delete` is the cross-dataset
path. `DecodeResult`, `Reject`, `RejectWithResult`, `KeyedCommand`, and
`KeyedDecision` remain shared because their durable command semantics did not
change. This is not an ORM or fluent query surface.

Normal Go users author no Oct and no catalog file. Opening buckets/datasets is
idempotent and writes the catalog automatically.

## 8. Advanced Oct/Catalog.oct path

M2B does not implement textual `Catalog.oct`. Current Oct integration proves
typed Octagon publication and compiler specialization in research paths, but it
does not expose a clean product import that can reconcile an authored catalog
with live Go structural updates. Adding syntax or a second authority would be
misleading.

The bounded future form is ordinary Oct records/record tables producing the
same semantic `Catalog` value, validated by existing Oct tooling, then installed
through an explicit catalog import/compile step. It requires no `catalog`
language keyword. A source `Catalog.oct` would remain authored input; the
checksummed backend `catalog` file would remain compiled durable state.

## 9. Source-of-truth decision

The checksummed semantic catalog value is authoritative. In M2B, the Go API is
its only writer and synchronously creates equivalent entries. There is no
generated side metadata and no second Oct authority. A later Oct path must
compile into this same value and obey the same compatibility checks before it
can become a supported origin.

## 10. Filesystem/materialization boundary

Catalog databases use a distinct `catalog-keyed-json-v1` `FORMAT` marker plus:

```text
FORMAT
catalog
wal
snapshot          (optional)
```

Temporary atomic-install files may appear as `catalog.tmp` and `snapshot.tmp`.
The logical tree creates no bucket/dataset directories and no per-record files.
A private nine-byte dataset prefix maps `(DatasetID, RecordKey)` into the
existing compact keyed WAL/snapshot representation. This is explicitly a
backend encoding, not semantic concatenation or a public address. One hundred
thousand records remain entries in compact WAL/snapshot files, not one hundred
thousand files.

## 11. Octagon relationship

Octagon remains a typed value/publication unit. A future dataset kind may hold
one nominal Octagon record or one Octagon record table as a dataset value. M2B
does not make Octagon a page, segment, record-per-file convention, or recovery
storage engine. Because only keyed JSON is product-ready, no unsupported
Octagon kind is exposed today.

## 12. Persistence/recovery

Initial open creates and synchronizes the database catalog. Bucket/dataset
creation writes a complete JSON catalog payload inside a magic/version/SHA-256
envelope, synchronizes `catalog.tmp`, atomically installs `catalog`, and syncs
the directory where supported. Only then is the handle returned. A structural
write ambiguity poisons subsequent writes.

Data decisions continue through checksummed, synchronized keyed WAL frames.
Recovery loads the data snapshot/WAL and then validates catalog checksum,
database identity, names, unique monotonic dataset IDs, supported kind/origin,
bounds, and every recovered record's dataset reference. Existing data with no
catalog, records with unknown dataset IDs, and corrupt catalog payloads fail
closed. Tests simulate process loss after catalog and WAL synchronization, then
prove bucket, dataset, database ID, and records survive.

## 13. Dataset compatibility

Reopening compares dataset kind, optional application `TypeIdentity`,
`MaxRecords`, and `MaxValueBytes`. A mismatch returns `ErrorIncompatible`; a
dataset never silently changes representation or promise.

For v0.2 JSON, an empty type identity means intentionally opaque JSON with
application-owned compatibility. A non-empty identity is an exact application
label such as `example.Item/v1`. OctetDB does not use reflection and does not
pretend that a Go type name or runtime shape provides schema migration.

## 14. Identity and bounds

Database identity is a generated durable 128-bit random value. Bucket and
dataset names are stable catalog names. Dataset IDs are durable monotonic
identities. Public record identity is `(DatasetID, RecordKey)`, so record key
`123` in two datasets cannot collide. Physical map keys and slots are never
exposed.

Dataset `MaxRecords` and `MaxValueBytes` are serialized hard semantic bounds.
Zero options inherit normalized database bounds. The database retains its
global live-record, value, transaction, and dedupe bounds. None of these values
is backend preallocation, and no capacity hint is serialized as a promise.

## 15. Multi-dataset transaction semantics

One `CatalogDB.Mutate` callback may read and write any previously opened
datasets belonging to that database. All writes become one existing keyed WAL
decision and are applied atomically after synchronization. Rejection discards
every dataset write. A dataset from another database is rejected.

Command IDs remain database-wide. This is required because one mutation may
span inventory and reservations (or any other pair), and its exact accepted or
rejected result must deduplicate as one decision. Structural catalog calls are
administrative; callers must open structure before entering a data callback.
There is no transaction-local bucket/dataset creation.

## 16. Catalog enumeration

`CatalogDB.ListBuckets` returns deterministic name-sorted `BucketInfo` values.
`Bucket.ListDatasets` returns deterministic name-sorted `DatasetInfo` values.
`CatalogDB.Catalog` returns a detached deterministic topology snapshot. These
are metadata enumeration only. M2B adds no record scan, prefix scan, filter,
projection, planner, or query language.

## 17. Golden-app migration

| Application | Bucket | Dataset | Direct `octetdb.` lines after M2B |
| --- | --- | --- | ---: |
| inventory | `inventory` | `items` | 16 |
| webhook | `events` | `webhooks` | 7 |
| orders | `commerce` | `orders` | 11 |
| jobs | `workers` | `jobs` | 13 |

All four stores now use logical record IDs without topology prefixes. Across
the four adapter files the mechanical migration added 93 lines and removed 48
(net +45), principally explicit open-time bucket/dataset declaration and error
cleanup. The service and HTTP packages had zero source changes. Their restart,
domain rejection, and exact retry tests pass through public APIs in the separate
golden module.

## 18. LLM legibility sanity check

The bounded fresh-agent result is recorded in
[`evidence/PRODUCT_M2B_LLM_CHECK.md`](evidence/PRODUCT_M2B_LLM_CHECK.md). The
agent received only README, GETTING_STARTED, package/exported documentation, and
an inventory-plus-reservations requirement. It was forbidden to inspect
implementation, repository tests, golden adapters, or this report.

The result demonstrates whether the agent understood Bucket/Dataset, avoided
inventing SQL/collections and filesystem nesting, used a database-wide command
ID for cross-dataset retry, and completed without human correction.

## 19. Backward compatibility / v0.2 impact

The released v0.1 `Open`, `DB`, account API, `accounts-v1` marker, WAL, snapshot,
and directory behavior are unchanged. PRODUCT-M2's unreleased `OpenKeyed`,
`KeyedDB`, `GetKeyed`, and `SubmitKeyed` remain available with their global key
namespace and `keyed-json-v1` marker.

New catalog databases use `catalog-keyed-json-v1`. `OpenKeyed` rejects them and
`OpenCatalog` rejects global keyed directories containing data, preventing a
silent loss or reinterpretation of dataset identity. Before a v0.2 tag, new
applications and examples should move to `OpenCatalog`; automatic migration is
not promised.

## 20. Rejected features

- arbitrary nesting or recursive child datasets;
- record-per-file materialization;
- SQL schemas, ORM builders, or reflected Go schema claims;
- secondary, sorted, dense, derived, or other lookup indexes;
- record filtering, scanning, or query language;
- rename, destructive delete, or implicit migration;
- universal pluggable dataset/storage kinds;
- an unsupported `Catalog.oct` parser or `catalog` keyword;
- Octagon as a page/storage-engine representation;
- catalog mutation inside ordinary data callbacks;
- database-scoped capacity values presented as preallocation hints.

## 21. Final product decision

1. Catalog/tree is useful and becomes the v0.2 product structure

The bucket level carried clear product grouping in all four integrations and
made topology explicit without affecting service logic. The implementation and
fresh-user surface do not support a finding that Bucket is merely ceremonial.

## 22. Exactly one next recommendation

**OCT-QUERY-M0 — FLOW-backed read-only query algebra over datasets/record
tables.**

M2B now gives a query system a durable leaf identity and catalog enumeration
without pre-spending query or index semantics. The next bounded product question
is read-only dataset/record-table algebra, not more topology or storage layout.

## Required evidence

| Scenario | Result |
| --- | --- |
| bucket survives restart | pass — catalog crash/reopen test |
| dataset survives restart | pass — stable dataset ID and metadata after crash/reopen |
| same key in two datasets | pass — key `123` recovered independently in jobs/orders |
| multi-dataset atomic mutation | pass — inventory plus order commit; rejected pair leaves neither partial write |
| duplicate retry | pass — same database-wide ID through another dataset returns original without callback |
| catalog corruption detection | pass — damaged catalog fails with `ErrorCorruption` |
| public-only golden apps | pass — four external-module store suites; no internal imports |
| fresh-agent integration | pass — see bounded evidence file |

