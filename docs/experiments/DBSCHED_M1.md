# DBSCHED-M1 — Ahead-of-time database execution plans

## 1. Verdict

**Success**

Oct moved the closed command model, statement catalog, compatibility relation, capacities, priorities, conflicts, transaction roles, and resource envelope out of runtime construction and into a committed fixed Go plan. All four lanes produced identical PostgreSQL state in three full runs. The defensible benefit is deterministic structure, compile-time rejection, and removal of runtime metadata allocation—not a material throughput improvement.

## 2. Thesis tested

M1 evaluated all three ideas separately:

- **Static derivation:** Oct derives the 4×4 compatibility matrix from one exhaustive typed relation and binds commands to a five-entry statement catalog.
- **Static representation:** D uses `[4]commandDescriptor`, `[4][4]bool`, and `[5]statementDescriptor`; C builds two maps and one slice.
- **Static capacity:** Oct owns queue capacity 128, maximum batch 8, write batch width 1, and worker count 8. `NewStatic` rejects a Go configuration that differs from the generated envelope.

The workload values, arrivals, occupancy, connection availability, rows, latency, and errors remain runtime data. Go GC, pgx, and PostgreSQL are unchanged.

## 3. Oct feature reconnaissance

| Classification | Capability | Evidence and decision |
|---|---|---|
| Used | Record tables | Natural columnar home for four typed commands and five statements; shared column extents are validated. |
| Used | Enums plus exhaustive `match` | Close command, statement, batch, priority, conflict, and transaction identities; all serializers and compatibility logic are exhaustive. |
| Used | Concepts and `Require` | Refined positive capacity/batch width and literal requirements reject invalid plans before artifact generation. |
| Used | Arrays and bounded iteration | Represent the closed inputs and derive all 16 compatibility cells. |
| Used | Artifact infrastructure | Serializes the typed plan into ordinary committed Go through an explicit, confined generation phase. This was the useful unexpected generalization: a scientific artifact lane cleanly emits systems configuration. |
| Used | Flow / Octomata | The M0 decision flow remains the common C/D runtime policy, avoiding a bundled policy change. |
| Not needed | Utility scoring | No evidence justified selecting among static candidates; M1 is not an adaptive controller. |
| Not needed | Units | Capacity and batch width are counts whose refinements were clearer than invented dimensions. |
| Not needed | Typed function values | Direct exhaustive functions are simpler for this closed plan. |
| Promising later | Flow topology specialization | The generated flow remains useful but its general helper baggage is larger than the static plan. Specializing it now would confound C vs D. |
| Blocked | General compile-time table queries | Current `Require` cannot loop, index, or inspect record fields. Literal uniqueness/envelope checks are compile-time; full symmetry is also a compiled Fact. No compiler change was needed. |
| Blocked | Native top-level constant table emission | The compiled backend constructs record-table slices in functions. The existing artifact facility was used instead of adding syntax or changing the compiler. |

The inventory was checked against `Language/reference`, the `Language/Types` positive/negative corpus, Octomata contracts, artifact contracts, compiler emission behavior, and relevant Concepts/OctGo reports in the current Oct repository.

## 4. Static execution plan

The Oct source is [`experiments/M1/static-plan/plan.octest`](../../experiments/M1/static-plan/plan.octest). Its core shapes are:

```oct
record table CommandDefinitions {
    Kind: CommandKind
    NumericID: Int
    Statement: StatementKind
    Batch: BatchClass
    Priority: PriorityClass
    Conflict: ConflictClass
    Transaction: TransactionRole
    QueueCapacity: PositiveCapacity
    MaxBatch: PositiveBatchWidth
}
```

`Compatible(left, right)` exhaustively matches `CommandKind`; the artifact evaluates it for all command pairs. The generated representation in [`static_plan.generated.go`](../../internal/scheduled/static_plan.generated.go) is:

```go
var staticExecutionPlan = executionPlan{
    Commands:      [4]commandDescriptor{...},
    Compatibility: [4][4]bool{...},
    Statements:    [5]statementDescriptor{...},
    QueueCapacity: 128, MaxBatch: 8, Workers: 8,
}
```

C constructs the equivalent maps/slice in `buildRuntimePlan`; D starts with the linker-materialized fixed value. The statement catalog includes stable identity, SQL, parameter shape, result shape, command binding, batch class, and transaction role. PostgreSQL preparation remains pgx-owned and identical: M1 does not contact PostgreSQL at generation time.

## 5. Compile-time invariants

The compiled Oct suite passes one positive Fact and six negative contracts with zero interpreted fallbacks. It rejects:

- duplicate numeric command identity;
- zero queue capacity;
- zero batch width;
- an unknown statement variant/binding;
- a queue smaller than one admitted maximum batch;
- asymmetric batch compatibility.

Enum closure also prevents unknown command/priority/conflict/transaction identities. Current Oct cannot express a general loop over record-table fields inside `Require`; therefore the positive full-matrix symmetry check is a compiled Fact rather than a `Require` proof.

## 6. Execution lanes

- **A — `conventional`:** one ordinary pgx operation per request, no scheduler.
- **B — `batch`:** conventional fixed same-kind read batching and a bounded ordinary Go queue; no Oct metadata lookup or decision flow.
- **C — `runtime`:** the M0 Oct flow plus idiomatic runtime construction of command maps, compatibility map, and statement slice.
- **D — `static`:** the same flow, admission bound, batching policy, workers, SQL path, and workload, but fixed Oct-generated metadata and capacities.

Aliases for M0 lane names remain for compatibility, but all M1 evidence uses the names above. A Go equivalence test compares every C/D command descriptor and all 16 compatibility cells.

## 7. Benchmark methodology

The seed, schema, dataset, PostgreSQL 17 container, pgx v5.7.6, eight-connection pool, warmup count, request timeout, and five arrival phases are unchanged from M0. Each lane resets, reseeds, `VACUUM ANALYZE`s, and checkpoints the same database. Each full lane executes 135,000 operations over approximately 62 seconds. Three independent full runs rotate lane order:

1. A, B, C, D;
2. D, A, C, B;
3. C, B, D, A.

Results below are medians; raw JSON, configs, environment identity, phase summaries, correctness snapshots, and memory/pool counters are retained under [`experiments/M1/evidence`](../../experiments/M1/evidence). The normalized summary is [`summary.json`](../../experiments/M1/evidence/summary.json).

## 8. Results

### Startup

| Lane | Metadata allocations | Metadata bytes | Whole scheduler init allocations | Whole scheduler init bytes |
|---|---:|---:|---:|---:|
| B fixed batch | 0 | 0 | 15 | 2,048 |
| C runtime | 8 | 1,464 | 23 | 3,512 |
| D static | 0 | 0 | 15 | 2,048 |

D removes 100% of C's measured metadata allocations and bytes. Construction and statement-catalog traversal usually measured below this Windows clock's observable tick and serialized as 0 µs; one whole-init sample was 509.8 µs. Allocation counters, source audit, and escape analysis are the reliable startup evidence; no sub-tick timing claim is made.

### Steady state and overload (median of three)

| Lane | ops/s | p50 ms | p95 ms | p99 ms | p99.9 ms | alloc/op | bytes/op | GC | avg batch | overload p99 ms | recovery s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A conventional | 2,177.56 | 0.0000 | 1.6263 | 2.1129 | 2.7784 | 23.8271 | 1,421.68 | 10 | 1.0000 | 2.1396 | 1 |
| B fixed batch | 2,177.43 | 0.5161 | 2.0630 | 2.5522 | 3.1520 | 33.1818 | 1,953.17 | 14 | 1.4502 | 2.1307 | 1 |
| C runtime plan | 2,177.46 | 0.5180 | 2.0735 | 2.5595 | 3.7768 | 38.7265 | 2,188.41 | 15 | 1.4486 | 2.1904 | 1 |
| D static plan | 2,177.45 | 0.5167 | 2.0691 | 2.5502 | 3.0110 | 38.6770 | 2,186.29 | 16 | 1.4524 | 2.1434 | 1 |

All lanes completed all operations with zero failures/rejections. C vs D throughput and p50/p95/p99 are neutral. D's median reduction of 0.0495 allocations/op and 2.12 bytes/op repeated directionally in all runs but is too small to treat as an application-level win. C's run-2 tail excursion did not reproduce. Median C/D queue p99 was 2.5271/2.5202 ms and database-service p99 was 2.1286/2.0973 ms. Median post-overload `normal_after` p99 was 2.7564/2.8158 ms, with peak scheduled queue depth 45–47 and recovery at 1 second throughout. p50 values of zero in A are clock-quantized samples, not literal zero-cost database calls.

Plain batching reduced median physical pool acquisitions from 135,000 to about 93,089 and aggregate pool acquire wait from 1,355.5 ms to 0.52 ms, but it added queue residence and application allocations. Throughput was arrival-rate limited.

Policy CPU timing was instrumented but below reliable attribution at this scale: C ranged 4.24–5.50 ms and D 1.49–11.64 ms accumulated over each approximately 62-second lane. The range is treated as timer noise, not a static-plan CPU result.

## 9. Ablation

- **Plain batching (A→B):** 31% fewer physical pgx acquisitions and almost no connection wait, but higher overall tail and allocation cost in this workload.
- **Bounded scheduling / Oct flow (B→C):** same batching and throughput; about 5.54 additional allocations/op and 235 bytes/op. No repeatable tail benefit over B in M1.
- **Static derivation and representation (C→D):** removes two maps, one slice, their population, 8 startup allocations, and 1,464 startup bytes. Steady-state impact is neutral/small.
- **Static capacity:** makes the resource envelope inspectable and rejects configuration drift, but does not change performance because both lanes use the same bound.
- **Statement catalog:** removes application-side discovery/construction in D, but actual pgx preparation and SQL execution are unchanged; no independent server-side speedup is attributed.

## 10. Generated-Go audit

The static artifact is 2,059 source bytes and contains four command descriptors, 16 compatibility booleans, five statement descriptors, and three global capacity values. Relative to C it removes:

- 2 runtime map constructions;
- 1 runtime slice construction plus its append;
- 2 metadata population loops (20 relationship insertions total: 4 command entries and 16 compatibility entries are no longer inserted);
- dynamic statement-catalog storage/population.

Escape analysis confirms both C maps, the C statement slice/append, and the runtime plan object escape to the heap. Static command and compatibility accessors inline; compatibility access does not escape. No static maps or initialization loops exist.

Runtime work that remains includes channels, request/result objects, batching slices, timers, goroutines, pgx values, actual SQL execution, and the M0 generated flow. The flow artifact is 9,679 source bytes and retains reflection clone/range/helper baggage; it is 4.7× the static-plan artifact and dominates generated support. Optimizing that helper surface was excluded because it would confound the metadata ablation.

## 11. TigerBeetle comparison

| Classification | Finding |
|---|---|
| TigerBeetle-specific implementation choice | Unmanaged/static memory disciplines and database-engine ownership are not transferred; Go GC and PostgreSQL remain ordinary. |
| General systems principle | Closed topology, identities, relationships, and capacity envelopes should be materialized before serving requests when practical. |
| Already matched by Oct | Exhaustive enums/match, bounded flow state, typed records, and compile-time requirements already model closed control structure. |
| Generalized by Oct | Record tables plus the artifact lane turn typed scientific data derivation into a conventional database execution-plan artifact. |
| Not useful for this workload | Further direct-index specialization did not materially improve rate-limited throughput or repeatable tails. |
| Gap remaining | `Require` cannot generally quantify over record-table rows, and compiled record tables do not emit native top-level fixed data without the artifact phase. |

## 12. Interpretation

| Idea | Classification | Interpretation |
|---|---|---|
| Fixed pgx batching | Useful | Reduces physical acquisitions/connection pressure, though latency/allocation tradeoffs are unfavorable here. |
| Static derivation | Useful | One typed source derives the complete compatibility relation and eliminates runtime reconstruction. |
| Static representation | Useful | Removes heap metadata and makes the closed plan directly inspectable; steady-state speed is neutral. |
| Static capacity | Useful | Enforces a single resource envelope and detects Go/plan drift before execution. |
| Statement catalog | Neutral | Better ownership/inspection, but this milestone did not alter actual pgx preparation. |
| Scheduler specialization | Neutral | Fixed indexing is slightly smaller in allocation counters but has no defensible throughput/tail win. |
| Compile-time validation | Useful | Six invalid plans fail before Go generation; enum closure removes stringly identities. |

## 13. Limitations

The benchmark is arrival-rate limited, uses one machine/PostgreSQL configuration and one workload mix, and has only three full runs. Windows timing quantization makes sub-millisecond startup timing unreliable; allocation evidence is stronger. B still uses a bounded ordinary Go queue, so A→B is batching plus queueing rather than a pure pgx protocol-only toggle. The statement catalog is application metadata and does not pre-contact PostgreSQL or independently benchmark explicit per-connection preparation. The flow helper baggage and request-level allocations dominate the tiny C/D difference. Compatibility is simple same-kind read batching; conflict and priority fields are modeled but do not yet change dispatch. No unmanaged memory, unsafe code, alternate GC, or runtime Oct dependency was introduced.

## 14. Exactly one next recommendation

**Pursue conflict-aware scheduling using the static plan.** The plan's typed conflict domains are now available without runtime discovery, while further metadata micro-specialization has no meaningful steady-state signal. The next experiment should make conflict information affect bounded dispatch and compare it against the same four controls.
