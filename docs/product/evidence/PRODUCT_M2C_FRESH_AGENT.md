# PRODUCT-M2C fresh-agent check

The agent received only candidate `README.md`, `doc.go`, and
`docs/GETTING_STARTED.md`. It was forbidden to inspect implementation, tests,
examples, history, internal packages, or other docs. Its task was to atomically
place an order across orders and inventory datasets, then list low-stock items.

## Result

Pass without correction. The exact returned source is retained at
`testdata/fresh-agent/main.go` and compiled/run from a fresh temporary module
with only the candidate local `replace` wiring.

The agent independently selected:

- `OpenCatalog` and `DefaultKeyedOptions`;
- one Database, one Bucket, and two Datasets;
- stable database-wide `KeyedCommand` IDs;
- one `Database.Mutate` callback with `Tx.Get`/`Put` across both Datasets;
- durable domain rejection helpers;
- typed `ScanDataset` and ascending record-key order.

It did not invent SQL, choose `OpenKeyed`, use filesystem paths as Dataset
identity, search internals, or require Oct.

## Agent ledger

- `Dataset` is documented as create-or-open; there is no separate open-existing
  operation.
- It did not need to inspect mutation decisions for the requested happy path.
- It chose one `shop` Bucket containing two Datasets; separate Buckets would
  also preserve database-wide atomicity.
- It used SKU as record key so scan order gives deterministic low-stock output.
- It added stable-ID seeding so the standalone program succeeds on a fresh
  directory and remains retry-safe.
