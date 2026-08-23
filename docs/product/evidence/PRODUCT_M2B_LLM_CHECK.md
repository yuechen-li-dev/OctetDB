# PRODUCT-M2B fresh-agent legibility check

## Protocol

A fresh coding agent received this bounded instruction:

- read only repository `README.md`, `docs/GETTING_STARTED.md`, `doc.go`, and
  exported `go doc` output;
- do not inspect implementation, tests, golden apps, diffs, or product reports;
- work in a new temporary external module using only public APIs;
- create logical `inventory/items` and `inventory/reservations` datasets;
- seed inventory, atomically decrement it and create a reservation across the
  two datasets, retry the same command ID through a different dataset, and prove
  restart state;
- do not edit the OctetDB checkout.

Temporary module:
`C:\Users\yuech\AppData\Local\Temp\octetdb-m2b-legibility-20a22e04c514409b82488dffcd55e2b4`.

## Result

Pass. Final command result:

```text
ok octetdb-legibility-check 0.350s
```

The first `go test` requested `go mod tidy`. After module housekeeping, the
same source compiled and passed; no API or logic correction was required.

| Question | Observation |
| --- | --- |
| Did it understand Bucket/Dataset? | yes — used bucket `inventory`, datasets `items` and `reservations` |
| Did it invent SQL/collections? | no |
| Did it confuse filesystem paths with logical datasets? | no — one path only for `OpenCatalog` |
| Did it correctly use global command identity? | yes — retried the cross-dataset command ID through `reservations.Mutate` |
| Did retry rerun the callback? | no |
| Did retry create an unintended record? | no |
| Did close/reopen preserve both records? | yes — stock 7 and reservation quantity 3 |
| Human intervention? | none |
| Repository files edited by agent? | none |

This is a bounded API-legibility sanity check, not a query-language or PRODUCT-M3
benchmark.

