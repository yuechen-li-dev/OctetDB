# OCT-QUERY-M0 — FLOW-backed read-only query algebra

## 1. Verdict

Success

## 2. Problem

PRODUCT-M2B could mutate or point-read a known record but could not discover
arbitrary Ready jobs or low-stock inventory. The product needed one safe,
deterministic Dataset enumeration primitive without SQL, a LINQ clone, or a
second execution runtime.

## 3. Existing collection/FLOW model

OctetDB's sane-default engine stores detached JSON bytes in a bounded in-memory
key map backed by a synchronized WAL and snapshot. One admission boundary
serializes mutation and maintenance. It had Dataset-scoped point operations but
no record cursor. Oct separately had arrays, record tables, and Octomata
FLOW/yield; OctetDB does not require Oct at runtime or expose FLOW in its Go API.

## 4. Query semantic model

A query begins from an explicitly opened Database → Bucket → Dataset and walks
logical KeyedJSON records in record-key ascending order. Application Go tests
each decoded item and may project or materialize it. Returning `ScanStop` is the
terminal boundary for Take/First/Any. Query meaning is the selected ordered
records, not today's scan algorithm.

## 5. Operator set

The public Go primitive is callback scan, not a fluent operator API. Ordinary Go
implements Filter and Map. `ScanStop` implements Take, First, and Any without
later work. Count increments during a complete scan. A zero/negative product
limit returns an empty slice before entering the scan. This covers all M0
operators without parsing Go closures or adding expression objects.

## 6. FLOW lowering/runtime reuse

The Go scan is deliberately boring and synchronous; it adds no iterator
runtime, goroutine, channel, worker pool, lazy list, or task executor. In Oct,
the same operator semantics lower to the existing FLOW/yield runtime. OctetDB
does not embed Oct or duplicate its compiler state machine.

## 7. Authoring/API decision

`Dataset.Scan(ctx, func(DatasetRecord) (ScanAction, error))` is the detached raw
surface. `ScanDataset[T](ctx, dataset, func(key string, value T)
(ScanAction, error))` is the typed helper. The latter decodes one record at a
time and avoids forcing normal callers to handle `json.RawMessage`. There is no
reflection query parser, fluent builder, global relation namespace, or implicit
join.

## 8. Generated implementation

No code is generated in the Go product path. A non-durable sorted primary-key
cursor is reconstructed from committed Dataset records on open and maintained
inside the same admission boundary as record mutation. Scan walks it directly,
so early stop does not first enumerate or sort the whole backend map. The cursor
is base-order bookkeeping, not a predicate index, planner, or persisted query
structure.

## 9. Failure/order/short-circuit semantics

Raw records are cloned before exposure. Typed decode failure is
`ErrorCorruption`; invalid input/action and incompatible kind are typed errors;
closed databases fail `ErrorClosed`; context cancellation and callback errors
end the whole scan. Earlier callbacks may already have observed values and
cannot be revoked. Filter/map preserve ascending key order. `ScanStop` returns
immediately after the current callback; tests observe three records to obtain
two matches and compiled Oct observes exactly 37 to obtain ten one-in-four
matches at every scale.

## 10. Oct array/record-table proof

The companion Oct report records six interpreted and six compiled facts with
zero compiled fallback over arrays and record tables, plus four invalid
contracts. Its golden `Job` is a record-shaped Concept, which cleanly models
nominal job identity without changing Dataset or query semantics.

## 11. Catalog/Dataset integration

Only `DatasetKind = KeyedJSON` is queryable. A Dataset handle carries durable
catalog identity; callers cannot supply a map, WAL frame, snapshot, bucket, or
filesystem path as the record source. Identical logical record keys in separate
datasets remain isolated. Cursor maintenance parses only catalog backend keys
and preserves insert/update/delete behavior.

## 12. Query snapshot semantics

A scan holds the existing database admission boundary from entry through the
last callback. It observes one stable committed in-memory state; mutations do
not interleave and wait until the scan exits. Multiple readers are serialized
by the same boundary. Cancellation is checked before admission and once per
record. The callback must not call a mutation because that would wait on its
own scan boundary. This simple M0 rule is correct but long reads block writes;
there is no MVCC or background snapshot worker.

## 13. Ready-job product proof

The job store's `ListReady(ctx, limit)` scans `workers/jobs`, decodes `Job`,
keeps `Status == Ready`, and returns `ScanStop` at the limit. Service and HTTP
expose `GET /jobs/ready?limit=N`; requeue is an ordinary idempotent mutation at
`POST /jobs/{id}/requeue`. The golden test scrambles keys, excludes claimed,
completed, and failed jobs, includes a failed job after requeue, proves Take
order, and reproduces results after restart.

## 14. Second golden-app proof

Inventory's `ListLowStock(ctx, threshold, limit)` scans `inventory/items`, keeps
stock at or below the threshold, and stops at the limit. `GET
/items/low-stock?threshold=N&limit=M` is conventional HTTP/service plumbing.
The golden test proves deterministic take and restart without job-specific
database features.

## 15. Benchmarks/allocations

Windows/amd64, Ryzen 7 7700X, 30 ms Go benchmark lane:

| Records | Lane | Filter | Filter+Take(10) | Filter+Map | Count |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000 | handwritten decoded slice | 646 ns | 26 ns | 742 ns | 1 ns |
| 1,000 | public Dataset API | 489 µs | 17.8 µs | 505 µs | 77.0 µs |
| 10,000 | handwritten decoded slice | 6.54 µs | 25.9 ns | 7.48 µs | 1 ns |
| 10,000 | public Dataset API | 5.22 ms | 19.0 µs | 5.31 ms | 488 µs |
| 100,000 | handwritten decoded slice | 68.0 µs | 26.0 ns | 73.8 µs | 1 ns |
| 100,000 | public Dataset API | 60.7 ms | 18.0 µs | 67.0 ms | 6.70 ms |

The typed full scans allocate for JSON decode (100k Filter: 28,000,032 B and
700,000 allocations); raw Count clones detached JSON (4,800,000 B and 100,000
allocations). These costs are linear and explicit. Filter+Take remains roughly
18–19 µs, 10,360 B, and 259 allocations at every scale because it examines only
37 records. Compiled Oct's separate FLOW lane reports 0 allocations and is
documented in the companion report. No intermediate slice is allocated per
query stage; only application terminal collection allocates its final output.

## 16. WAL/read-only proof

The product test records WAL file size, in-memory durable sequence, and retained
dedupe IDs before and after a query and requires all three to be identical.
Scan has no WAL call or mutation transaction. Query cursor bookkeeping is
non-durable and changes only as part of an already durable record mutation.

## 17. LLM legibility

A fresh agent received only `README.md`, `docs/GETTING_STARTED.md`, `doc.go`, and
the ready-job requirement. It independently wrote the intended `OpenCatalog →
Bucket("workers") → Dataset("jobs") → ScanDataset → ScanStop at ten` path. It
invented no APIs or SQL, and correctly separated the owned database directory
from logical catalog names. Human correction was limited to application-owned
path, TypeIdentity, Job/Ready types, and shutdown policy. Full evidence is in
`docs/product/evidence/OCT_QUERY_M0_LLM_CHECK.md`.

## 18. Rejected SQL/LINQ/planner features

M0 adds no SQL, aggregation pipeline, joins, groups, arbitrary sorting,
distinct, general reducer, window, dynamic projection, fluent generic query
builder, Go-closure AST parser, cost model, relationship search, secondary
index, MVCC, background execution, worker pool, or new Dataset kind.

## 19. Required architecture decision

2. Thin query syntax/IR sugar is justified but lowers completely to FLOW

This is the Oct language decision. The Go product remains a callback scan and
does not expose the Oct syntax or an IR.

## 20. Required product decision

A. Scan-based Dataset query is sufficient for v0.2

The scans are correct, deterministic, cancellable, read-only, restart-stable,
and fast enough for the candidate product proof. Long-read blocking is measured
and documented rather than hidden; it does not make M0 unsafe.

## 21. Remaining limitations

Only KeyedJSON scans. Queries serialize with writes and other scans. JSON decode
dominates typed full-scan allocations. There is no index, planner, arbitrary
ordering, paging/resume token, terminal collection API, read-only callback
purity enforcement, query stats API, or MVCC. Database values remain bounded
in-memory state, and v0.2 is not tagged by this milestone.

## 22. Exactly one next recommendation

Ship the scan API in the candidate v0.2 surface and collect real workload data
before choosing whether one predicate index or a published read snapshot is the
next product investment.
