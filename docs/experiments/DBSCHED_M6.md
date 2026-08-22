# DBSCHED-M6: first-class compiled-data read publication

## 1. Verdict

Success

M6 connects the M5 compiled immutable read cache to Oct's first-class compiled-data backend. A direct nominal `Products` Octagon with enum and refined cells is loaded, ordinary Oct derives the declared read models, `StaticAssert` rejects invalid publication, and `Artifact.WriteCompiledData` emits fixed Go arrays with compiler-owned identity. The M5 10,000-row publication path fell from a median of about 93.3 seconds to 2.33 seconds (about 40x). The same full path completed at 100,000 rows in 22.38 seconds, built in 2.21 seconds, performed no runtime reconstruction, and allocated zero bytes/objects on startup and Q1-Q4.

M6 also found and fixed one genuine Oct dogfood bug: interpreted `xs = Append(xs, x)` deep-copied the entire array inside `Append` and again on assignment. That made ordinary derivation quadratic. A narrow self-append assignment fast path reduced the 10k artifact from 21.65 seconds to 2.33 seconds without changing any of the seven generated artifact hashes. Targeted Oct interpreter, typechecker, and compiled-data tests pass.

The result is practical for bounded epoch publication, but not cheap: repeated Oct grouping/projection passes now dominate full publication. Go formatting dominates only the backend emitter itself; native build/link is secondary.

## 2. M5 → Compiled Data M0 → M6 lineage

M5 proved that a compiled immutable cache can eliminate runtime decode/index construction and preserve ordinary Go execution, but discovered nominal-table materialization gaps and a roughly 93-second interpreted `WriteLines` path. Compiled Data M0 fixed those gaps generally in Oct revision `71bcb4f73a15b9877c1d97123d7d0e0791df1366`. M6 external-dogfoods that revision in Database-Scheduler and retains PostgreSQL as a different-guarantee control, not an equivalent system.

## 3. Source-model cleanup

| M5 workaround | M6 disposition |
|---|---|
| `CatalogData` record of column arrays | Removed; the canonical Octagon root is nominal `Products` |
| integer-coded category/status/region | Removed; `Category`, `Status`, and `Region` are nominal enums |
| unrefined integer price | Removed; `PriceCents` is `PositivePrice` and admission runs during load |
| handwritten application hash/schema identity | Removed from the compiled artifact; compiler logical/schema hashes and row count are consumed directly |
| compiled `[Fact]` publication workaround | Replaced by ordinary validation helpers plus `StaticAssert` in the artifact phase |
| interpreted `Artifact.WriteLines` | Removed; all retained data/indexes use `Artifact.WriteCompiledData` |

No old emitter is retained in the M6 path. M5 evidence remains the historical control.

## 4. Canonical nominal snapshot

Representative schema:

```oct
enum Category { C0 C1 C2 C3 C4 C5 C6 C7 }
enum Status { Draft Active Retired }
enum Region { R0 R1 R2 R3 R4 R5 }
concept PositivePrice = Int { Require(Self > 0, "price must be positive") }

record table Products {
    ID: Int
    Category: Category
    Status: Status
    PriceCents: PositivePrice
    RatingTenths: Int
    Region: Region
    Name: String
}
```

Representative Octagon:

```octagon
Products {
    ID: [1000001, 1000002]
    Category: [Category.C5, Category.C0]
    Status: [Status.Retired, Status.Active]
    PriceCents: [49744, 114773]
    RatingTenths: [23, 34]
    Region: [Region.R2, Region.R2]
    Name: ["Product-000001", "Product-000002"]
}
```

The path is directly `canonical Octagon → nominal Products → static validation → compiled data`; there is no representational shim.

## 5. Static derivation/validation

Ordinary Oct derives category row IDs/offsets and the active-region ID/price projection/offsets. The host retains deterministic price sorting because Oct has no sort helper; the resulting nominal `PriceIndex` Octagon is loaded, coverage-checked, order-checked, and republished as typed static data. `Array.CrossSection` was evaluated but is only a range-copy operation; it cannot filter, group, or sort these models.

Publication asserts non-empty and exact extents, dense unique IDs, positive prices, valid row IDs, category coverage, price-index coverage sums, deterministic `(price,rowID)` order, projection extent agreement, and monotonic/bounded offsets. Six probes fail before publication: duplicate ID, invalid enum, refinement violation, extent mismatch, corrupted sorted index, and invalid row ID.

The bounded epoch probe uses:

```oct
let updated = source with { PriceCents: [100, 250, 300] }
```

It proves the source remains at 200, the result contains 250, both remain nominal three-row `Products`, and unchanged ID/enum columns retain their semantics.

## 6. Compiled-data publication pipeline

```text
Products Octagon + host-sorted nominal PriceIndex
  → typed nominal tables
  → ordinary Oct category/projection derivation
  → StaticAssert validation
  → WriteCompiledData (table + six index/projection arrays)
  → deterministic formatted Go
  → ordinary go build
```

The reproducible generator is `go run ./cmd/m6gen -rows <n> -output <ignored-work-dir>`. Large Octagon and generated Go products are deliberately ignored and are not retained.

## 7. Identity/freshness

| Probe | Logical hash | Schema hash | Rows |
|---|---|---|---:|
| source | `sha256:a605...3d9f` | `sha256:dd632...974fe` | 3 |
| identical source copy | `sha256:a605...3d9f` | `sha256:dd632...974fe` | 3 |
| price-only update | `sha256:f3d60...a3990` | `sha256:dd632...974fe` | 3 |
| added-label schema | `sha256:8c07f...de5d9` | `sha256:49e2e...d6cde` | 3 |

Thus identical logical values hash identically, a data-only update changes only logical identity, and a schema change changes schema identity. The 10k rerun reported all seven files `UNCHANGED`; staged-file SHA-256 values were stable. Runtime exposes `ProductsDataLogicalHash`, `ProductsDataSchemaHash`, and `ProductsDataRowCount` without parsing source. The human semantic label remains application-owned in the report.

## 8. Execution lanes

The conceptual lanes remain:

| Lane | Role in M6 |
|---|---|
| A PostgreSQL | M5 service/system-of-record baseline retained |
| B JSON indexed | M5 runtime-loaded control retained |
| C compact binary indexed | M5 runtime reconstruction control retained |
| D compiled rows | republished through first-class compiled data and remeasured at 10k/100k |
| E compiled specialized | republished category/price/active projection arrays and remeasured at 10k/100k |

A/B/C were not reimplemented because the logical dataset/query architecture is unchanged. M6's motivating comparison is the replacement publication path and D/E scale behavior; M5's ready-state B/C result remains the equivalent-array control.

## 9. Correctness

For every category and region plus the standard price range, D and E traverse results and produce identical normalized count/digest outcomes. Q1 validates dense ID lookup. The canonical generator is the unchanged deterministic M5 authority. Compiler row-count metadata equals the static array extent at both scales. All available paths preserve Q1-Q4 semantics.

## 10. 10k before/after publication

| Property | M5 interpreted path | M6 compiled-data path |
|---|---:|---:|
| publication | median ~93.3 s | 2.331 s |
| generated Go | 1,386,361 B | 1,602,663 B across seven typed units |
| canonical input | 405,759 B surrogate | 775,577 B nominal table + 58,917 B price index |
| runtime construction | none | none |
| model | surrogate/int-coded/manual identity | nominal enums/refinement/compiler identity |
| steady query allocation | 0 | 0 |

The source grew because M6 emits general `int`-backed nominal rows and separate compiler-owned units, but the 93-second pathology disappeared externally.

## 11. Scale characterization

| Metric | 10,000 | 100,000 | Observed scaling |
|---|---:|---:|---|
| canonical table Octagon | 775,577 B | 7,755,400 B | ~10.0x |
| price-index Octagon | 58,917 B | 688,917 B | ~11.7x from wider IDs |
| input generation + host price sort, median | 18.0 ms | 71.1 ms | 3.95x; mixed generation/I/O/sort |
| full Oct artifact | 2.331 s | 22.384 s | 9.60x |
| generated Go | 1,602,663 B | 16,191,141 B | 10.10x |
| Go build | 0.682 s | 2.207 s | 3.24x |
| E executable | 4,041,216 B | 13,174,784 B | 3.26x |
| process startup probe | 11.5 ms | 12.5 ms | effectively flat/noisy |

Two points do not establish formal complexity. The full artifact is consistent with linear row-pass scaling after the Oct fix. The host price sort is expected to include an `n log n` component, but rendering and file I/O prevent isolating it from these two points.

## 12. Publication cost breakdown

External isolation:

| Cost | 10k | 100k |
|---|---:|---:|
| nominal load + one row-count assert + row emission/staging | 0.287 s | 1.948 s |
| full load + derivation + validation + seven outputs | 2.331 s | 22.384 s |
| derivation/index-validation/index-output residual | ~2.044 s | ~20.436 s |
| Go build/link | 0.682 s | 2.207 s |
| publication through native executable | ~3.013 s | ~24.591 s |

The residual is an isolation, not a perfectly additive profiler: the rows-only run still includes its own load/hash/format/stage work.

Controlled Oct typed-emitter benchmark (simpler four-field table, three iterations):

| Rows | validate/hash | literal emission | `go/format` | `EmitGo` total | source |
|---:|---:|---:|---:|---:|---:|
| 10k | 6.119 ms | 6.119 ms | 55.097 ms | 67.064 ms | 697,234 B |
| 100k | 55.501 ms | 60.648 ms | 516.036 ms | 654.989 ms | 7,166,437 B |

`go/format` is about 79–82% of the isolated backend cost and scales near source size. It is not the full M6 bottleneck: ordinary Oct derivation is roughly 20 seconds at 100k. Rewriting formatting is therefore not the next move.

## 13. Startup/read results

| Scale | startup allocs/bytes | Go heap after ready | startup private bytes | startup working set | process probe |
|---|---:|---:|---:|---:|---:|
| 10k | 0 / 0 | ~313,200 B | ~13.9 MB | ~7.33 MB | ~11.5 ms |
| 100k | 0 / 0 | ~313,200 B | ~21.6 MB | ~7.34 MB | ~12.5 ms |

Static data is outside the Go heap. The flat startup working set is expected because a single Q1 does not fault every mapped page; it is not a claim that the full snapshot is resident.

All measured Q1-Q4 calls allocate 0 B / 0 objects. Representative latencies:

| Scale/lane | Q1 | Q2 category | Q3 price | Q4 projection |
|---|---:|---:|---:|---:|
| 10k D | ~1–10 ns | 4.71 µs | 6.75 µs | 9.87 µs |
| 10k E | ~1–10 ns | 1.72 µs | 1.52 µs | 0.334 µs |
| 100k D | ~1–10 ns | 77.97 µs | 102.00 µs | 356.06 µs |
| 100k E | ~1–10 ns | 18.78 µs | 20.05 µs | 2.01 µs |

Q1 is below stable wall-clock resolution in this harness and is reported as a range, not a false exact zero.

## 14. Specialized-index ablation

At 10k, E is ~2.7x faster for Q2, ~4.4x for Q3, and ~29.6x for Q4. At 100k, the observed ratios are ~4.2x, ~5.1x, and ~177x. Absolute ratios differ from M5 because the general backend emits `int`-backed 64-byte rows rather than M5's compact hand-authored Go representation. The conclusion reproduces: declared specialization wins over scanning, while compilation does not intrinsically make equivalent ready arrays faster.

## 15. Parallel reads

| Workers | 10k E ops/s | 100k E ops/s |
|---:|---:|---:|
| 1 | 2.22 M | 0.155 M |
| 2 | 1.97 M | 0.157 M |
| 4 | 2.67 M | 0.177 M |
| 8 | 5.00 M | 0.399 M |

There is no synchronization in the read model. Scaling is non-monotonic at small durations and improves most at eight workers. The 100k decline and modest 1→4 scaling are consistent with result traversal, cache locality, and memory bandwidth becoming more important than locks.

## 16. Go compiler/linker audit

Generated output contains fixed native arrays and nominal `Category`, `Status`, `Region`, `PositivePrice`, and `ProductsRow` declarations. Searches found no `init`, reflection, decoder, parser, Octagon loader, `make`, `append`, or map construction in generated files. `go tool nm -size` reports:

| Symbol | 10k | 100k |
|---|---:|---:|
| `ProductsData` | 640,000 B | 6,400,000 B |
| `CategoryRows` | 80,000 B | 800,000 B |
| `PriceRows` | 80,000 B | 800,000 B |
| `ActiveIDs` | 27,232 B | 268,288 B |
| `ActivePrices` | 27,232 B | 268,288 B |

Ordinary safe Go is the runtime substrate; there is no unsafe, cgo, custom allocator, application-side reconstruction, or Oct runtime dependency.

## 17. Artifact-size scaling

| Scale | total Octagon input | products Go | all Go | D binary | E binary | E−D binary |
|---|---:|---:|---:|---:|---:|---:|
| 10k | 834,494 B | 1,426,544 B | 1,602,663 B | 3,773,952 B | 4,041,216 B | 267,264 B |
| 100k | 8,444,317 B | 14,256,368 B | 16,191,141 B | 10,985,472 B | 13,174,784 B | 2,189,312 B |

Formatted Go is ~1.92x combined Octagon input at both scales, below M5's 3.42x surrogate-input ratio. The native row is 64 bytes, so the general nominal representation is less compact than M5's 32-byte hand-authored row. E pays unavoidable additional static bytes for exact read models.

## 18. OctetDB read-plane interpretation

1. **Is publication practical?** Yes for bounded immutable epochs: ~24.6 seconds from full 100k Oct artifact through native build on this host.
2. **Reasonable scale?** 100k is healthy in runtime and manageable in source/binary size. Larger scales were not justified before reducing repeated derivation passes.
3. **Dominant publication cost?** Ordinary interpreted derivation/validation (~20.4 seconds residual at 100k), not typed emission (~0.65-second controlled backend) or Go build (~2.21 seconds).
4. **Is native compilation worth it?** For snapshots reused across many process starts or requiring exact projections, yes. A runtime binary cache remains preferable when epochs change frequently enough that ~25-second publication and larger binaries dominate.
5. **What is eliminated?** Runtime parsing, decoding, index/projection construction, heap ownership of the snapshot, query allocation, and synchronization for immutable reads.
6. **What remains unavoidable?** Every epoch must derive, emit, compile/link, distribute, map, and traverse its static data; specialization consumes extra source/binary space.

The evidence supports only an OctetDB read-plane candidate. No write plane exists. PostgreSQL offers mutation, durability, concurrency control, and general querying; compiled cache wins by specializing those semantics away for one immutable epoch.

## 19. Oct feature findings

| Feature | Classification | Finding |
|---|---|---|
| nominal table Octagon | Useful | direct canonical root; no shim |
| enum/refined cells | Useful | natural model and prepublication failure |
| record-table `with` | Useful | bounded epoch probe preserves nominal schema/immutability |
| `StaticAssert` | Useful | clear publication failures; per-element calls should be aggregated for cost |
| `WriteCompiledData` | Useful | replaces 93-second text path with deterministic typed output |
| compiler hashes/row count | Useful | runtime identity and clean logical/schema separation |
| artifact freshness | Useful | seven unchanged outputs without rewriting |
| ordinary Oct derivation | Useful, needs more pressure | expressive enough; repeated passes dominate at 100k |
| `Array.CrossSection` | Neutral | correct range-copy primitive, orthogonal to grouping/filter/sort |

## 20. Remaining gaps

- No general sort/group/index-builder helper; price sorting remains host-owned.
- Repeated interpreted table passes still cost ~20 seconds at 100k even after linearizing self-append accumulation.
- General Go emission uses native `int` and a row-oriented 64-byte struct; compact/columnar mixed emission deserves evidence before design.
- Generated code does not embed an explicit compiler-version constant, so this report records the Oct SHA externally.
- Very large snapshots still pay formatted-source and Go compilation cost.
- Hot swap, mutable publication, write coordination, and multi-epoch lifecycle remain intentionally unbuilt.

## 21. Exactly one next recommendation

**Improve Oct's ordinary static table-derivation library around bounded grouping/index construction, then re-run the 100k publication without repeated full-table passes.**

The self-append fix removed the accidental quadratic cliff and made 100k viable. The remaining measured ~20-second derivation residual is now the largest tractable cost. A narrow grouping/index-builder abstraction would attack evidence from this external dogfood without prematurely adding a query DSL, write plane, hot swap system, or formatter rewrite.
