# DBSCHED-M5: compiled immutable read cache

## 1. Verdict

**Success**

M5 compiled a deterministic 10,000-row catalog through Oct into ordinary Go, verified it against JSON, compact-binary, and PostgreSQL controls, and measured both publication and runtime costs. The compiled D/E lanes perform no runtime decode or index construction, allocate nothing during queries, and begin querying at the empty-Go-process baseline. The rude follow-up is equally important: after a conventional cache is loaded and indexed, E does not make its query loops intrinsically faster. Its decisive advantages are removal of startup reconstruction and the ability to publish exact immutable projections; the present Oct artifact path costs about 93 seconds to emit a 10,000-row artifact and is the dominant tradeoff.

## 2. Phase 2 thesis

This is a **compiled immutable read cache, not a database replacement**. PostgreSQL remains a suitable mutable system of record. A complete publication epoch is transformed into typed canonical data, validated by Oct, emitted as Go, compiled, and then queried without a runtime database/cache loader.

The snapshot has one identity:

```text
SnapshotVersion = m5-seed-20260821-n10000
SnapshotHash    = 8314fe505eed49b57b8faf1cd72069cd6f5d35a4f17da24adda6f05755385d0e
SchemaVersion   = product-v1
RowCount        = 10000
```

## 3. Oct data-model reconnaissance

The Oct revision tested was `a997285fc006b29143c31f6391ca6f21a950adb2`.

Current record tables are nominal immutable column stores. Declared fields are cell types, storage adds one implicit array depth, construction checks shared column length, `Len(table)` returns row count, and `table[i]` projects a compiler-owned row. Tables have no `with`, append, delete, join, query, or compile-time derivation syntax. Ordinary records do support immutable `with`; M5 proves a publication metadata record can produce version 2 while version 1 remains unchanged.

`.octagon` is not JSON branding. It is a data-only Oct expression parsed into typed Oct values using an expected type. It supports scalars, arrays, records, enums, and—partially—record tables. The direct record-table result is currently inconsistent:

1. A compiled Fact successfully loads a primitive `Products` record-table Octagon and preserves nominal table/cell behavior.
2. The build-time-interpreted artifact lane rejects the same root because it expects a scalar cell where the table stores a column array.
3. The compiled loader panics for realistic record-table columns containing enums or refined concepts (`reflect.Value.SetInt on struct Value`).
4. An ordinary typed record containing column arrays works in both paths.

M5 therefore uses `CatalogData { ID: Int[], ... }` as the canonical Octagon root, constructs `Products` inside Oct, validates it in a compiled Fact, and evaluates it again in the artifact phase. This preserves typed data and record-table validation, but not direct record-table identity in the file. Integer codes plus publication Facts are used for category/status/region because nominal enum columns hit the compiled loader bug. Generated Go restores nominal Go types.

Ordinary compiled Oct reconstructs loaded Octagon values at runtime through its parser/materializer. M5 moves that work into the artifact phase; the application does not call `LoadOctagon`. The existing artifact infrastructure is reusable, but it renders Go with interpreted string loops and is very slow at this scale.

The current gaps between Oct and the intended architecture are therefore concrete:

- unify record-table Octagon materialization across interpreter and compiled backends;
- support enum and refined-concept table cells in compiled Octagon loading;
- make a nominal table Octagon a first-class artifact input;
- emit static backend data directly instead of building thousands of strings through `Artifact.WriteLines`;
- provide bounded compile-time table derivation/group/sort helpers and quantifiable uniqueness/index proofs;
- carry schema/snapshot identity as compiler-owned metadata rather than handwritten constants;
- expose deterministic generated-artifact hashing/freshness in the artifact command.

There is also a reference documentation inconsistency: `tooling/34-octagon.md` says package declarations are forbidden but one example includes `package Main`. The parser/contracts correctly enforce one data-only value.

## 4. Dataset

The deterministic generator uses seed `20260821` and emits 10,000 products with dense IDs, eight categories, three statuses, six regions, positive prices, ratings, and stable names. This scale is large enough to expose reconstruction, index, source, and binary costs while keeping the committed generated source reviewable. A 100,000-row run was not forced after the 10,000-row artifact phase already reached roughly 93 seconds.

The generator creates four equivalent publications:

| representation | bytes |
|---|---:|
| canonical `catalog.octagon` | 405,759 |
| JSON control | 1,115,612 |
| compact binary control | 280,012 |
| formatted generated Go | 1,386,361 |

JSON and binary payloads are reproducible ignored products. The Octagon, Oct source, generated Go, hashes, and normalized evidence are retained.

## 5. Query family

- Q1 `FindByID(id)`: exact dense-ID point lookup.
- Q2 `FindByCategory(category)`: cursor over all matching products.
- Q3 `ProductsWithPriceBetween(low, high)`: inclusive price range.
- Q4 `ActiveProductsInRegion(region)`: projected `ID + PriceCents` rows.

Queries return immutable cursors rather than mutable slices. Correctness digests consume every returned row; benchmark timings therefore include result traversal, not merely obtaining a handle.

## 6. Execution lanes

| lane | implementation |
|---|---|
| A | PostgreSQL 17.11 via ordinary pgx pool; primary key, category, price, and partial active-region indexes |
| B | read JSON, unmarshal Go rows, construct exact runtime indexes |
| C | read a bounded purpose-built binary encoding, decode rows, construct the same indexes |
| D | Oct-emitted fixed product array; point lookup plus scans for Q2-Q4 |
| E | D plus emitted category row IDs/offsets, price-sorted row IDs, and active-region projections/offsets |

B and C intentionally share the same post-load query implementation. This isolates text parsing from reconstruction. D and E intentionally share the same product data and point lookup. This isolates specialization from compilation alone.

## 7. Generated representation

Representative E data is ordinary Go:

```go
var products = [...]m5.Product{
    {ID: 1000001, Category: 5, Status: 2, PriceCents: 148459, RatingTenths: 28, Region: 4, Name: "Product-000001"},
    // ...
}

var categoryRows = [...]uint32{ /* row positions */ }
var priceRows = [...]uint32{ /* price-sorted row positions */ }
var activeProjections = [...]m5.Projection{ /* ID + price */ }
```

All backing stores are fixed arrays. Accessors form temporary slices over those arrays; they do not build maps or slices. The arrays are unexported. Public cursors have unexported storage and return product/projection values by copy, so callers cannot obtain or mutate the backing arrays. Code inside `internal/m5compiled` remains the technical ownership boundary; Go does not provide truly immutable package storage.

## 8. Startup-to-first-query

One warm-page-cache run from cache initialization entry:

| lane | read | decode | indexes | first Q1 | total ready-to-first |
|---|---:|---:|---:|---:|---:|
| A, already-running ready pgx pool | — | — | already present | 1.186 ms | 1.186 ms |
| B JSON | 0.129 ms sampled read | 9.309 ms | 1.546 ms | 1 ns loop cost | 11.371 ms |
| C binary | 0.516 ms sampled read | 0.515 ms | 2.052 ms | 2 ns loop cost | 3.082 ms |
| D compiled | none | none | none | 6 ns | 6 ns from entry |
| E compiled | none | none | none | 6 ns | 6 ns from entry |

The nanosecond entries are repeated accessor measurements because a single Windows timer sample can round to zero. The five-count benchmark gives 6.60-6.72 ns for D Q1 and 6.78-6.90 ns for E Q1.

Thirty rotated process launches include Windows process creation, Go runtime, loader, output, and application work:

| executable | p50 process ms | p95 process ms |
|---|---:|---:|
| empty Go control | 9.625 | 15.449 |
| D compiled | 10.127 | 11.421 |
| E compiled | 9.764 | 11.524 |
| C binary | 12.923 | 13.934 |
| B JSON | 21.199 | 27.405 |

D/E are indistinguishable from the empty executable at this noisy process scale. This is “no cache construction,” not literal zero process startup. A’s separate dataset publication (copy, indexes, analyze) took 85.778 ms in the final run and is not charged to reads from an already-published database.

## 9. Runtime allocation and memory

| lane | startup allocs | startup allocated bytes | query allocs/bytes |
|---|---:|---:|---:|
| B JSON | 40,251 | 3,271,416 | 0 / 0 for Q1-Q4 |
| C binary | 80,055 | 1,518,088 | 0 / 0 for Q1-Q4 |
| D compiled | 0 | 0 | 0 / 0 for Q1-Q4 |
| E compiled | 0 | 0 | 0 / 0 for Q1-Q4 |

The simple binary decoder allocates a byte buffer and string per name, so it performs more allocations than JSON despite allocating fewer bytes. This is acceptable for the control: its purpose is to remove text parsing, not win a serializer contest.

Median standalone private bytes were D 46.47 MB, E 46.57 MB, binary 48.05 MB, and JSON 51.31 MB. These Windows private-byte values include Go/runtime reservations and are not claims about resident physical pages. Static data does not appear in the Go heap; heap-only comparison would therefore undercount D/E.

## 10. Query results

Authoritative D/E values are medians of five `go test -benchmem` counts. A/B/C are normalized harness runs on the same host.

| lane | Q1 point | Q2 category | Q3 price | Q4 projection | allocs/op |
|---|---:|---:|---:|---:|---:|
| A PostgreSQL | 269.5 us | 583.4 us | 631.4 us | 409.1 us | 12 / 2602 / 2406 / 1140 |
| B JSON cache | ~5 ns | 10.08 us | 9.19 us | 531 ns | 0 |
| C binary cache | ~15 ns | 10.46 us | 9.29 us | 436 ns | 0 |
| D compiled scan | 6.64 ns | 14.85 us | 18.16 us | 10.47 us | 0 |
| E compiled indexed | 6.82 ns | 10.53 us | 9.64 us | 476 ns | 0 |

E corresponds to roughly 147 million point reads/s, 95 thousand complete category result traversals/s, 104 thousand price result traversals/s, and 2.1 million projection traversals/s in the dedicated benchmark. PostgreSQL remains three to five orders slower depending on result size and transport work. That is a service-boundary comparison, not evidence that PostgreSQL is deficient at its actual mutable/durable role.

The bounded mixed E workload scaled from 385k operations/s at one worker to 802k at eight workers. It remained deterministic and race-free, but scaling was only 2.08x because result traversal becomes CPU/cache-bandwidth work; immutability removes locks, not memory limits.

## 11. Specialized-index ablation

| query | D | E | E improvement | index verdict |
|---|---:|---:|---:|---|
| Q1 | 6.64 ns | 6.82 ns | none | dense direct lookup already exists in D |
| Q2 | 14.85 us | 10.53 us | 1.41x | category row IDs earn modest cost |
| Q3 | 18.16 us | 9.64 us | 1.88x | sorted row IDs and binary-search bounds earn cost |
| Q4 | 10.47 us | 0.476 us | 22.0x | prefiltered projection clearly earns cost |

The exact E index symbols occupy 107,376 bytes plus 144 bytes of offsets. The E executable is 109,056 bytes larger than D, closely matching those data symbols. Precomputed projection is the strongest specialization; generic-looking indexes are not automatically dramatic.

## 12. Publication/build cost

| step/artifact | result |
|---|---:|
| deterministic generator, warm | 0.162 s |
| Oct compiled validation median | 1.279 s |
| Oct artifact emission | 90.070-94.891 s |
| forced Go build, D | 3.906 s |
| forced Go build, E | 4.031 s |
| Oct source | 153,873 bytes |
| canonical Octagon | 405,759 bytes |
| raw/formatted generated Go | 1,212,657 / 1,386,361 bytes |
| empty/D/E executable | 2,691,072 / 3,212,288 / 3,321,344 bytes |

D adds 521,216 executable bytes over the empty Go control. E adds another 109,056 bytes. The formatted generated source is 3.42x the Octagon input. Artifact evaluation—not parsing, validation, Go build, or runtime—is overwhelmingly the current publication bottleneck.

## 13. Correctness and snapshot consistency

Tests compare every point lookup and representative Q2-Q4 result digests across canonical Go rows, B, C, D, E, and PostgreSQL. All available lanes match. D/E identity exactly matches the canonical hash/version/schema/count. Eight goroutines repeatedly query one E snapshot and produce deterministic results; `go test -race` passes. Cursors return values, no query mutates backing data, and every process observes one compiled epoch.

The full price index is validated sorted, category index covers exactly one row position per source row, projection columns have equal extents, IDs are dense/unique, prices are positive, code columns are range-checked, and generated/control row counts agree.

## 14. Compile-time validation

Four invalid sources are rejected before application runtime:

- negative refined price;
- invalid enum variant;
- mismatched record-table extents;
- explicit duplicate ID requirement.

A corrupted price index fails its compiled publication Fact. The valid 10,000-row snapshot Fact runs compiled with zero interpreted fallback. `Require` still cannot loop, index, inspect rows, sort, or quantify uniqueness, so whole-table properties are compiled Facts rather than typechecker proofs. Enum/refined table columns also cannot currently traverse the compiled Octagon path, as described above.

## 15. Go compiler/linker audit

1. **Static materialization:** `go tool nm -size` reports `products` as a 320,000-byte `D` data symbol. E additionally has 40,000-byte category IDs, 40,000-byte price IDs, 27,232-byte projections, and 144 bytes of offsets. There is no `m5compiled.init` symbol.
2. **Runtime initialization:** no generated constructor, map build, slice append, decode, reflection, or validation executes in the application. The OS loader maps the executable and applies normal relocations; Go runtime startup remains.
3. **Heap before first query:** the compiled cache itself records zero allocations/bytes from initialization entry. Products and indexes live outside the Go heap.
4. **Escape analysis:** query wrappers and constructors inline; the compiler reports no query-result escape. Benchmarks confirm 0 B/op and 0 allocs/op.
5. **Binary tradeoff:** D adds 521 KB over empty; E adds 109 KB over D. Larger datasets will scale data/source/link work roughly with rows and duplicated indexes.

The declaration uses `var [...]T`, not immutable memory. It is linker-materialized ordinary data, not a giant Go function that executes at startup.

## 16. Comparison with conventional caches

B versus C shows that JSON text work dominates conventional startup here: C reaches ready about 3.7x sooner and allocates about half the bytes, although the deliberately simple decoder allocates more objects.

C versus D shows what compilation removes: file open/read, binary validation, 10,000 row/string reconstructions, index arrays, sorting, bucket construction, and roughly 1.5 MB of startup allocation. What remains is OS executable loading, Go runtime startup, direct accessor/cursor code, and traversal of requested results.

D versus E shows that merely compiling rows does not create fast multi-row queries. The declared access pattern must also be materialized. B/C and E have near-identical post-ready query costs because they ultimately use equivalent concrete arrays. Compilation moves construction out of runtime; it does not imbue arrays with extra speed.

## 17. TigerBeetle comparison

| classification | finding |
|---|---|
| generalizable principle | narrow schema, bounded work, explicit identity, and predeclared access patterns remove machinery |
| more extreme specialization | E publishes exact read indexes/projections and performs no request parsing, storage access, or cache construction |
| not applicable | WAL, MVCC, transaction mutation, durability, replication, recovery, and runtime request ownership |
| tradeoff | M5 obtains a cheaper read path by giving up mutation and requiring a new build/publication for every epoch |

No “faster than TigerBeetle” claim is made because the operations and guarantees are not equivalent.

## 18. TableScript/Copeland interpretation

Classification: **works as expected**.

Current Copeland TSON is more complete than Octagon in schema identity: it models self-contained schema identity plus nominal record, enum, table, and column identities, validates rectangular tables, and canonicalizes table documents. TableScript also lowers bounded table/query plans into backend source. These are useful design precedents.

Its C# backend does not establish the M5 performance premise: authored tables are emitted through `Create()`, allocate one array per column, create column wrappers and a table object, then assign a static singleton. Derived tables additionally allocate result arrays and join dictionaries. Its ad-hoc query binder/generator is broader than M5’s declared-accessor design. Therefore old Copeland performance is not imported.

The empirical Oct/Go result validates the architectural concept but narrows it: native compiled data removes startup reconstruction exactly as expected, while steady-state query speed matches an equivalently indexed runtime cache. The strongest result is publication-time projection, not source-code storage by itself.

## 19. Intended workload fit

Good fit:

- versioned product/content/reference catalogs;
- pricing, routing, entitlement, and model manifests;
- game/scientific data published in complete epochs;
- schema-known data with a small stable query family;
- deployments where cold start, per-process heap, deterministic reads, or removal of a cache service matter.

Bad fit:

- carts, sessions, balances, counters, or frequently updated rows;
- ad-hoc analytics and user-defined predicates;
- very large snapshots where source/link/binary amplification dominates;
- data that must hot-swap without process/binary publication;
- workloads requiring transactions, durability, replication, or partial invalidation.

## 20. Limitations

The experiment uses one 10,000-row synthetic catalog, one Windows/AMD host, one Go/Oct revision, and one PostgreSQL container. Page-cache state is warm. Process launch timing is noisy. RSS evidence uses Windows private bytes rather than physical residency. PostgreSQL queries cross a local service boundary and return rows through pgx; they are not same-guarantee comparisons with in-process arrays.

The generator, not Oct, currently derives sorted/bucket/projection arrays and computes SHA-256. Oct validates the resulting structure. Category/status/region are integer-coded in canonical Octagon because current compiled record-table enum materialization panics. Direct record-table Octagon works in a small compiled probe but fails in the artifact interpreter. The artifact output is ordinary static Go but its 93-second string-emission path is not production-worthy. There is no hot swap, independent module, automatic SQL snapshot extractor, generated join, or multi-scale curve.

## 21. Exactly one next recommendation

**Implement a first-class Oct compiled-data publication backend that accepts a nominal record-table Octagon and emits static Go arrays plus declared indexes directly.** It should first unify interpreter/compiled table materialization (including enums/refined concepts), then replace interpreted `Artifact.WriteLines` loops with deterministic backend emission and compiler-owned snapshot identity. This single step attacks the measured 93-second bottleneck and removes the type-identity workarounds before M5 is scaled or given hot-swap machinery.
