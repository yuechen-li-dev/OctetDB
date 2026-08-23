# Getting started

OctetDB has a progressive adoption path. Stage 1 uses ordinary Go values and
does not require Oct, generated code, layout contracts, WAL filenames, snapshot
filenames, or benchmark tuning.

## 1. Open with defaults

```go
db, err := octetdb.OpenKeyed(ctx, "./data/myapp", octetdb.DefaultKeyedOptions())
if err != nil { return err }
defer db.Close()
```

OctetDB creates and owns its product files beneath `./data/myapp`. One process
may own that directory. The defaults bound live records, retained retry
decisions, value size, and writes per command. You do not need to tune them to
try the product.

## 2. Store and read ordinary Go state

```go
type Item struct {
    SKU   string `json:"sku"`
    Stock int    `json:"stock"`
}

decision, err := db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: "create-widget"}, func(tx *octetdb.KeyedTx) (any, error) {
    item := Item{SKU: "widget", Stock: 10}
    return item, tx.Put("items/widget", item)
})
```

After `SubmitKeyed` succeeds, the decision and all writes are durable. Read the
current value with a normal destination pointer:

```go
var item Item
found, err := db.GetKeyed(ctx, "items/widget", &item)
```

Keys are application-defined strings. Use stable, explicit prefixes such as
`items/`, `orders/`, and `jobs/`. M2 exposes point lookup only; model a known
projection explicitly in your application if you need one, and note that there
is no scan or query planner yet.

## 3. Add explicit idempotent commands

The command ID is a correctness value supplied by the caller. Reusing it returns
the original durable decision without running the callback again. For HTTP,
validate `Idempotency-Key` and pass it through unchanged:

```go
command := octetdb.KeyedCommand{ID: r.Header.Get("Idempotency-Key")}
```

Do not silently generate a different ID for each retry. IDs are retained exactly
inside the bounded dedupe horizon; an expired ID is new again.

Use a durable domain rejection for expected invalid intent:

```go
if item.Stock < quantity {
    return item, octetdb.RejectWithResult("insufficient_stock", item)
}
```

The returned decision has `Applied == false` and `Code == "insufficient_stock"`.
An exact retry gets the same decision. An ordinary callback error aborts without
recording the command ID and is suitable for operational failures.

## 4. Add atomic invariants and multi-record changes

The callback may read and write multiple records. Its writes become visible
together only after validation succeeds:

```go
decision, err := db.SubmitKeyed(ctx, command, func(tx *octetdb.KeyedTx) (any, error) {
    var item Item
    found, err := tx.Get("items/widget", &item)
    if err != nil { return nil, err }
    if !found { return nil, octetdb.Reject("not_found") }
    if item.Stock < quantity { return item, octetdb.RejectWithResult("insufficient_stock", item) }
    item.Stock -= quantity
    if err := tx.Put("items/widget", item); err != nil { return nil, err }
    return item, nil
})
```

Callbacks are serialized. Keep them deterministic and local: do not perform
network calls or other irreversible external effects inside them. Compute or
fetch external input first, then submit the intended state change.

## 5. Advanced specialization

The keyed workflow is an on-ramp, not OctetDB's entire architecture:

```text
default Go keyed state
→ explicit application command model
→ richer semantic bounds, batches, idempotency, and workflows
→ Oct-defined behavior and compiler specialization
```

Move deeper only when a stable domain and measured workload justify it. The
v0.1 account API is an example of a narrow specialized model. Advanced Oct and
layout research are not prerequisites for the keyed workflow.

## Bounds and maintenance

Defaults are 100,000 live records, 100,000 retained command decisions, 1 MiB
per JSON value or command result, and 4 MiB of writes per command. These are safe trial defaults,
not claims about every production domain. A true maximum population and retry
horizon are semantic production decisions; set `KeyedOptions` explicitly once
you know them.

Keys and command IDs have a fixed 4 KiB limit; rejection codes have a fixed
1 KiB limit. These mechanical safety bounds require no configuration.

`Close` installs a deterministic snapshot and resets the WAL. Call
`SnapshotKeyed` at a deliberate maintenance boundary if a long-running process
needs to bound recovery work. M2 adds no background scheduler.

## HTTP error mapping

Keep transport choices in the application adapter:

| Condition | Typical HTTP response |
| --- | --- |
| malformed input or missing idempotency key | 400 |
| domain rejection such as invalid transition | 409 |
| application record not found | 404 |
| `ErrorCapacity` | 507, or a documented 503 policy |
| storage, poisoned, or durability failure | 503 |
| cancelled context | stop writing the response |
| duplicate retry | return the original successful/rejected result |

OctetDB does not contain HTTP status codes.

## When not to use OctetDB

Prefer a conventional database when you need ad-hoc SQL, rapidly changing
arbitrary schemas, many dynamic query shapes, minimal upfront modeling, or
general multi-tenant relational workloads. The keyed API has point lookup, not
listing, indexes, joins, schema migrations, or online backup.
