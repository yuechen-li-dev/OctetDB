# OctetDB

OctetDB is a boring embedded Go OLTP database by default: Database → Bucket →
Dataset, ordinary Go structs, durable atomic mutations, exact bounded
idempotency, deterministic scans, and restart safety. Oct is optional and
provides deeper semantic/query specialization when desired.

## Status

`v0.2.0` is the current public release. OctetDB is pre-1.0: incompatible
formats are rejected, and automatic migration is not promised.

```sh
go get github.com/yuechen-li-dev/octetdb@v0.2.0
```

Go 1.23 or newer is required. Oct, PostgreSQL, and TigerBeetle are not required
to build or use the public package.

## Quickstart

```go
type Item struct {
    SKU   string `json:"sku"`
    Stock int    `json:"stock"`
}

ctx := context.Background()
db, err := octetdb.OpenCatalog(ctx, "./data/shop", octetdb.DefaultKeyedOptions())
if err != nil { return err }
defer db.Close()

inventory, err := db.Bucket(ctx, "inventory")
if err != nil { return err }
items, err := inventory.Dataset(ctx, "items", octetdb.DatasetOptions{
    TypeIdentity: "shop.Item/v1",
})
if err != nil { return err }

command := octetdb.KeyedCommand{ID: "receive-widget-001"}
decision, err := db.Mutate(ctx, command, func(tx *octetdb.Tx) (any, error) {
    item := Item{SKU: "widget", Stock: 8}
    return item, tx.Put(items, item.SKU, item)
})
if err != nil { return err }

var item Item
found, err := items.Get(ctx, "widget", &item)
```

The caller owns one directory; OctetDB owns all files beneath it. Buckets and
datasets are durable logical catalog entries, not paths:

```text
Database
└── Bucket
    └── Dataset
        └── Records
```

Record identity is `(Dataset, key)`. The same key can exist independently in
two datasets. Command identity is database-wide.

The complete runnable [quickstart](examples/quickstart/main.go) also proves
close/reopen, duplicate retry, point read, and typed scan. See
[Getting started](docs/GETTING_STARTED.md) for the progressive walkthrough.

## Atomic mutation and retry

`Database.Mutate` is the only catalog transaction boundary. One callback gets
one `Tx` and may read or write any previously opened Dataset:

```go
decision, err := db.Mutate(ctx, octetdb.KeyedCommand{ID: orderCommandID},
    func(tx *octetdb.Tx) (any, error) {
        if err := tx.Put(orders, order.ID, order); err != nil { return nil, err }
        if err := tx.Put(items, item.SKU, item); err != nil { return nil, err }
        return order, nil
    })
```

All writes become visible together after the accepted decision is written and
synchronized. `Reject` and `RejectWithResult` record an exact durable rejection
and discard writes. Other callback errors abort without recording the command
ID. Retrying a retained ID returns the original decision without rerunning the
callback. Keep callbacks deterministic and local; do network or irreversible
work outside them.

## Deterministic dataset scans

Go users query without Oct. `Dataset.Scan` exposes detached JSON records;
`ScanDataset[T]` is the ordinary typed path:

```go
low := make([]Item, 0, 10)
err := octetdb.ScanDataset(ctx, items,
    func(_ string, item Item) (octetdb.ScanAction, error) {
        if item.Stock <= 5 { low = append(low, item) }
        if len(low) == 10 { return octetdb.ScanStop, nil }
        return octetdb.ScanContinue, nil
    })
```

A scan is read-only, visits ascending record keys, observes one stable logical
snapshot, returns detached values, checks context cancellation between records,
and stops synchronously on `ScanStop`. It does not change the WAL, sequence, or
dedupe state. The current serialized snapshot implementation blocks mutations
for the scan duration. This is deterministic enumeration, not a query planner
or predicate index.

## API hierarchy

- **Canonical:** `OpenCatalog`, `Database`, `Bucket`, `Dataset`, `Tx`, `Get`,
  `Mutate`, `Scan`, and `ScanDataset[T]`.
- **Compatibility:** the v0.1 `Open`/`DB` account API remains supported.
  `OpenKeyed`/`KeyedDB` retain the distinct unreleased pre-v0.2 global-key
  format and are deprecated; they are not taught as a new-application model.
- **Advanced and optional:** Oct query syntax and specialized domain/compiler
  paths. OctetDB has no runtime dependency on Oct.

`DB` cannot be renamed because it is v0.1 public API. `Database` is the v0.2
catalog type. `KeyedDB` and `KeyedTx` exist only for compatibility; canonical
code uses `Database` and `Tx`. There is no `CatalogDB` or `CatalogTx` in v0.2.

## Defaults

A normal application supplies only a directory. Zero options select:

| Bound | Default |
| --- | ---: |
| live records, database-wide | 100,000 |
| retained exact command decisions | 100,000 |
| one encoded value or decision result | 1 MiB |
| encoded writes in one command | 4 MiB |
| dataset live records | inherited from database |
| dataset value size | inherited from database |

Record keys and command IDs have a fixed 4 KiB limit; rejection codes have a
fixed 1 KiB limit. Dataset-specific bounds may be lower. There are no query
tuning knobs.

## Optional Oct specialization

Oct's separate query syntax expresses `filter`/`map`/`take` and
`Query.First`/`Any`/`Count`, lowering to Oct FLOW state machines. It can make
composable query behavior and compiler specialization more ergonomic, but it is
not required for Dataset scans. The [advanced example](examples/oct-query/README.md)
pins the verified Oct revision. Beginner code does not expose FLOW or compiler
IR concepts.

## Durability, formats, and recovery

A successful mutation decision means its checksummed WAL frame was written and
synchronized. `Close` installs a deterministic snapshot. Recovery validates
the catalog, snapshot, and WAL; replays complete decisions; truncates an
incomplete final append; and fails closed on corruption or incompatible
formats. See [Durability](docs/DURABILITY.md) and [Recovery](docs/RECOVERY.md).

Compatibility summary:

- v0.1 `accounts-v1`: supported by `Open`/`DB`.
- pre-v0.2 `keyed-json-v1`: deprecated compatibility through `OpenKeyed` only.
- v0.2 `catalog-keyed-json-v1`: canonical for new databases through
  `OpenCatalog`.

There is no automatic migration, and one opener never silently interprets
another model's directory.

## Limitations

- Single process, single open handle, and single replica; no directory lock,
  replication, failover, or online backup API.
- No SQL, joins, secondary indexes, query planner, MVCC, migrations, or schema
  reflection.
- JSON values are held in memory; no large-blob path.
- Long scans serialize mutations.
- Idempotency is exact only inside `DedupeHorizon`; an expired ID is new again.
- Cancellation is honored before mutation admission, not after durable
  processing begins.
- Snapshot rename power-loss guarantees are weaker on Windows than POSIX.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

The production public import graph is the root package plus `internal/core` and
the Go standard library. Research and benchmark packages remain elsewhere in
the repository and are not imported by the v0.2 package.

OctetDB is licensed under [GPL-3.0](LICENSE).
