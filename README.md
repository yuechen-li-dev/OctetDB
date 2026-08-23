# OctetDB

OctetDB is an embeddable specialized OLTP engine with a conventional Go on-ramp.
Applications can start with a bounded, durable Database/Bucket/Dataset catalog,
application-defined keyed JSON records, and atomic typed mutation functions. The original specialized account/transfer
model remains available, and deeper Oct-defined specialization is an advanced
adoption step rather than an onboarding requirement.

It is not a PostgreSQL replacement, general SQL database, network daemon,
TigerBeetle clone, cache, or ORM.

## Status

The first public release is `v0.1.0`. The keyed-state API documented below is a
candidate additive v0.2 API in the current source tree and is not in v0.1.0.
OctetDB is intentionally pre-1.0: APIs and database formats may change between
minor releases, incompatible data is detected and rejected, and automatic
migration is not promised.

## Install

```sh
go get github.com/yuechen-li-dev/octetdb@v0.1.0
```

That command installs the released specialized account API. The keyed workflow
below is implemented on the current main branch and should be consumed as a
versioned dependency after the proposed v0.2.0 release.

OctetDB requires Go 1.23 or newer. The release is tested on Windows and Linux
under WSL2; macOS is not yet verified. Oct and PostgreSQL are not required to
build or use the public package.

## Default Go workflow

```go
ctx := context.Background()
db, err := octetdb.OpenCatalog(ctx, "./data/myapp", octetdb.DefaultKeyedOptions())
if err != nil {
    log.Fatal(err)
}
defer db.Close()

usersBucket, err := db.Bucket(ctx, "identity")
if err != nil { log.Fatal(err) }
users, err := usersBucket.Dataset(ctx, "users", octetdb.DatasetOptions{TypeIdentity: "example.User/v1"})
if err != nil { log.Fatal(err) }

decision, err := users.Mutate(ctx, octetdb.KeyedCommand{ID: "create-user-42"}, func(tx *octetdb.DatasetTx) (any, error) {
    user := User{ID: "42", Name: "Ada"}
    return user, tx.Put("42", user)
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(decision.Applied)
```

`Mutate` serializes one ordinary Go mutation callback, atomically applies
all of its record writes, synchronizes the durable decision, and retains the
exact decision for stable command-ID retries. Domain rejections use `Reject` or
`RejectWithResult`; unexpected callback errors are not recorded and may be
retried. Values and results use `encoding/json`.

The logical structure is deliberately shallow:

```text
Database
└── Bucket
    └── Dataset
        └── Records
```

This is logical structure; OctetDB may materialize it differently for
performance. Buckets and datasets are catalog entries, not filesystem paths.
Record keys are scoped to their dataset, so key `123` in `identity/users` is
distinct from key `123` in `commerce/orders`.

Start with [Getting started](docs/GETTING_STARTED.md). Runnable examples cover
the [keyed default](examples/keyed/main.go), [v0.1 minimal account lifecycle](examples/minimal/main.go),
and [account batch idempotency across restart](examples/restart/main.go).

## Catalog and keyed default

`OpenCatalog` owns the supplied directory and creates a durable database
identity and catalog automatically. `Bucket`, `Dataset`, `ListBuckets`, and
`ListDatasets` are structural metadata operations; they are not record queries.
M2B supports one dataset kind, `KeyedJSON`. An optional `TypeIdentity` is an
application-owned compatibility label, not a reflected Go schema or migration
system. Reopening a dataset with a different kind, type identity, or bound fails
with `ErrorIncompatible`.

The default bounds are 100,000 live
records, 100,000 retained command decisions, 1 MiB per JSON value or command result, and 4 MiB of
writes per command. A normal application chooses only its directory. Close
installs a deterministic snapshot, while `CatalogDB.Snapshot` is available for
explicit maintenance. QUERY-M0 adds a deterministic Dataset scan; it does not
add a dynamic query language, planner, or secondary index.

Use `db.Mutate` with `CatalogTx` when one command must atomically read or write
several datasets. Command IDs remain database-wide, so retry identity is stable
even for cross-dataset invariants. Catalog creation is administrative and does
not occur inside data mutation callbacks. M2B has no rename or destructive
catalog deletion.

`OpenKeyed`, `KeyedDB`, `GetKeyed`, and `SubmitKeyed` remain available as the
PRODUCT-M2 candidate compatibility surface. Their keys occupy one global
application-defined namespace; new v0.2 code should prefer `OpenCatalog`.

Keys and command IDs are limited to 4 KiB and rejection codes to 1 KiB without
additional configuration.

## Dataset queries

`Dataset.Scan` visits detached logical KeyedJSON records in record-key ascending
order. `ScanDataset[T]` is the typed convenience: it decodes one value at a
time, creates no intermediate result slice, and lets the callback return
`ScanStop` for `Take`, `First`, or `Any` behavior.

```go
var ready []Job
err := octetdb.ScanDataset(ctx, jobs, func(_ string, job Job) (octetdb.ScanAction, error) {
    if job.Status != Ready {
        return octetdb.ScanContinue, nil
    }
    ready = append(ready, job)
    if len(ready) == 10 {
        return octetdb.ScanStop, nil
    }
    return octetdb.ScanContinue, nil
})
```

Every scan is scoped through an opened Database → Bucket → Dataset. A scan
holds the database admission boundary for one stable committed state, so writes
do not interleave and a long scan blocks mutations. The callback runs
synchronously and must not call an OctetDB mutation; keep it deterministic,
local, and free of external side effects. Context cancellation is checked for
each record. One decode or callback error fails the scan, though callbacks may
already have observed earlier records.

Queries scan today. An in-memory primary-key cursor provides reproducible base
order and genuine early stop, but it is not a predicate index. Future indexes
may accelerate the same filter semantics without changing source identity,
results, or order.

Stable command IDs are part of application correctness. For HTTP, pass the
client's `Idempotency-Key` through as `KeyedCommand.ID`; do not generate a new ID
on each retry.

## Specialized account model

One `DB` owns one directory and authoritative keyed account state. Commands
carry stable IDs. `SubmitBatch` evaluates commands serially in offered order,
including overlapping hot keys, and writes all new decisions in one WAL frame.
Accepted and rejected decisions receive a sequence. Duplicate IDs within the
configured exact bounded horizon return the original decision without applying
it again.

`Options` exposes only product bounds: path, maximum accounts, dedupe horizon,
and maximum offered batch size. Defaults are 100,000 accounts, 100,000 retained
command results, and 512 commands per batch.

## Durability

A successful submission means its complete WAL frame has been written and
synchronized. Recovery validates an optional snapshot, validates and replays
the complete WAL tail, and truncates an incomplete final append. Corruption and
incompatible formats fail closed. See [the precise durability contract](docs/DURABILITY.md)
and [recovery/storage layout](docs/RECOVERY.md).

## Format compatibility

`FormatVersion` identifies the database format independently of the Go module
version. Before 1.0, the format may change between minor releases. OctetDB does
not yet promise automatic migration, but it does promise that incompatible data
is detected and rejected rather than silently interpreted as the current
format.

## Limitations

- Single process and single replica; there is no directory lock, replication, or failover.
- No SQL, query planner, network server, secondary indexes, migrations, or online backup API.
- Keyed values use JSON and are loaded into memory; there is no schema migration
  framework, compare-and-swap API, cross-process writer coordination, or large-blob path.
- Idempotency is exact only inside `DedupeHorizon`; expired IDs may apply again.
- Cancellation is honored before admission, not after durable processing begins.
- Snapshot rename power-loss guarantees are weaker on Windows than POSIX.

## When not to use OctetDB

A conventional database is usually a better choice when you need ad-hoc SQL,
rapidly changing arbitrary schemas, many dynamic query shapes, minimal upfront
modeling, or general multi-tenant relational workloads. OctetDB asks the
application to state its mutations and invariants explicitly in exchange for a
bounded path toward deeper semantic specialization.

## Performance evidence

In a bounded single-replica local OLTP experiment, the specialized safe-Go
engine that became the v0.1 production engine exceeded the measured
single-replica TigerBeetle control in the tested configurations. The systems
were not guarantee-equivalent, and this is neither a general database benchmark
nor a claim that OctetDB is generally faster than TigerBeetle. See the
[TigerCompareM0 report](docs/experiments/TIGER_COMPARE_M0.md) for methodology,
environment, and limitations.

## License

OctetDB is licensed under the [GNU General Public License v3.0](LICENSE).

## Development and experiments

```sh
go test ./...
go vet ./...
```

The root package and `internal/core` are production OctetDB. `cmd/`, the other
`internal/` packages, `postgres/`, and `experiments/` retain benchmark controls,
fixtures, generated artifacts, and historical research. Start with the
[experiment archive index](docs/experiments/README.md), not those packages, when
looking for historical evidence.
