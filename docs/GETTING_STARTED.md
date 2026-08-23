# Getting started with OctetDB v0.2

The default Go path needs one directory, ordinary structs, and stable command
IDs. It does not need Oct, generated code, SQL, or knowledge of storage files.

## 1. Open the database and catalog

```go
db, err := octetdb.OpenCatalog(ctx, "./data/shop", octetdb.DefaultKeyedOptions())
if err != nil { return err }
defer db.Close()

inventory, err := db.Bucket(ctx, "inventory")
if err != nil { return err }
items, err := inventory.Dataset(ctx, "items", octetdb.DatasetOptions{
    TypeIdentity: "shop.Item/v1",
})
if err != nil { return err }
```

OctetDB owns files beneath `./data/shop`. The durable topology is exactly:

```text
Database
└── Bucket
    └── Dataset
        └── Records
```

The topology is semantic, not physical. A bucket or dataset name is not a file
path. Record identity is Dataset plus key.

## 2. Mutate ordinary Go state atomically

```go
type Item struct {
    SKU   string `json:"sku"`
    Stock int    `json:"stock"`
}

command := octetdb.KeyedCommand{ID: "receive-widget-001"}
decision, err := db.Mutate(ctx, command, func(tx *octetdb.Tx) (any, error) {
    item := Item{SKU: "widget", Stock: 8}
    return item, tx.Put(items, item.SKU, item)
})
```

`Database.Mutate` is the transaction boundary even when a command touches one
Dataset. A nil callback error accepts every write atomically. `Reject` or
`RejectWithResult` records a durable application rejection and discards writes.
Any other error aborts without recording the ID.

The callback is serialized and must be deterministic and local. Fetch network
input first; do not perform irreversible external work inside the callback.

## 3. Read one known record

```go
var item Item
found, err := items.Get(ctx, "widget", &item)
```

`Get`, `Tx.Get`, `Tx.Put`, and `ScanDataset[T]` encode or decode ordinary Go
values. Normal application code does not handle JSON bytes. `Dataset.Scan` is
available when a tool genuinely needs detached raw JSON.

## 4. Retry safely

Command IDs are database-wide correctness values. Reusing an ID within the
configured horizon returns the exact original decision without invoking the
callback again—including after close/reopen.

For HTTP, validate and pass the caller's `Idempotency-Key` unchanged. Never
generate a new ID for each retry. `DecodeResult` decodes the typed application
result from either the accepted or rejected decision.

```go
if item.Stock < requested {
    return item, octetdb.RejectWithResult("insufficient_stock", item)
}
```

## 5. Preserve invariants across datasets

Open every Dataset before entering the mutation. One `Tx` can access all of
them:

```go
decision, err := db.Mutate(ctx, octetdb.KeyedCommand{ID: "place-order-42"},
    func(tx *octetdb.Tx) (any, error) {
        var item Item
        found, err := tx.Get(items, sku, &item)
        if err != nil { return nil, err }
        if !found { return nil, octetdb.Reject("item_not_found") }
        if item.Stock < quantity { return item, octetdb.RejectWithResult("insufficient_stock", item) }
        item.Stock -= quantity
        if err := tx.Put(items, sku, item); err != nil { return nil, err }
        if err := tx.Put(orders, order.ID, order); err != nil { return nil, err }
        return order, nil
    })
```

The accepted/rejected decision and all writes use one database WAL boundary.
Catalog creation is administrative and is not allowed inside a mutation.

## 6. Scan a known dataset

```go
low := make([]Item, 0, 10)
err := octetdb.ScanDataset(ctx, items,
    func(_ string, item Item) (octetdb.ScanAction, error) {
        if item.Stock <= 5 { low = append(low, item) }
        if len(low) == 10 { return octetdb.ScanStop, nil }
        return octetdb.ScanContinue, nil
    })
```

Public scan guarantees:

- read-only: no WAL append, sequence change, or dedupe mutation;
- ascending record-key order;
- one stable logical committed snapshot;
- detached raw and typed values;
- context cancellation checked between records;
- synchronous early stop: no later record is decoded after `ScanStop`.

The callback runs inline. Do not mutate OctetDB from it. The current scan holds
the database admission boundary, so a long scan blocks mutations. This is a
known v0.2 limitation, not MVCC.

## 7. Close and reopen

`Close` installs a deterministic snapshot containing records and retained
command decisions. Catalog topology is already synchronized when created.
Reopen the same directory and the database identity, every Dataset, cross-
dataset state, dedupe decisions, and scan results survive.

Use `Database.Snapshot` only at an application-chosen maintenance boundary for
a long-running process. There is no background snapshot scheduler.

## Defaults and errors

Zero options select 100,000 live records, 100,000 retained decisions, 1 MiB per
encoded value/result, and 4 MiB of writes per command. Dataset bounds inherit
database bounds unless explicitly lowered. Keys and command IDs are limited to
4 KiB; rejection codes to 1 KiB.

Use `errors.As` to inspect `*octetdb.Error` and its stable `Kind`:
`ErrorInvalidInput`, `ErrorCapacity`, `ErrorStorage`, `ErrorCorruption`,
`ErrorIncompatible`, `ErrorClosed`, or `ErrorPoisoned`.

## Compatibility and advanced paths

New v0.2 code uses `OpenCatalog`. The v0.1 `Open`/`DB` account API remains
supported. Deprecated `OpenKeyed` supports only the separate pre-v0.2
global-key development format and is not a second onboarding choice.

Oct is optional. Its advanced `query` syntax provides `filter`, `map`, `take`,
and `Query.First`/`Any`/`Count` by lowering to FLOW. It is useful when Oct
authoring or compiler specialization is desired; it is not required for Go
Dataset scans. See the [separate advanced example](../examples/oct-query/README.md).

Run the [canonical Go quickstart](../examples/quickstart/main.go) next.
