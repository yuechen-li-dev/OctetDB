# DBSCHED-M0

## 1. Verdict

**Success**

Across two normalized runs with rotated lane order, bounded admission plus
compatible read batching completed every attempted operation at the same useful
throughput as the baseline. Its overload p99 was stable at 2.58–2.59 ms while
the baseline measured 2.76 and 3.84 ms. This is a narrow control result, not a
general database speedup: scheduled steady-state p99 was about 0.7 ms higher,
and admission without batching was inconsistent near the tested capacity.

## 2. Architecture

```text
Lane A: seeded operations -> ordinary Go goroutines -> pgxpool (8) -> PostgreSQL

Lane B: seeded operations -> generated Oct policy -> bounded queue (128)
                                  -> compatible read batches (max 8)
                                  -> 8 Go workers -> pgxpool (8) -> PostgreSQL
```

The Oct flow owns admission and compatibility decisions. Its closed
`SchedulerOutcome` enum is projected to the Go-facing integer result by an
exhaustive Oct `match`; all terminal paths converge on one explicit `Complete`
state. Handwritten Go owns the bounded queue, workers, pgx calls, transactions,
timings, and reports. The generated file is committed, has no Oct runtime
dependency, and all memory remains ordinary GC-managed Go memory.

Oct's `batch` construct was not used: it is an all-or-nothing data-parallel
array map, not a database batching primitive. Oct chooses compatible groups and
pgx `SendBatch` executes the SQL. An Oct `[Benchmark]` was also omitted because
it would measure isolated policy dispatch, while every generated-policy call is
already included in the end-to-end request latency.

## 3. Correctness evidence

Before measurement, the runner resets the database and executes the same 500
operations through both lanes in bounded concurrent waves. Both retained runs
report exact equality for:

- 545 orders and 45 order items;
- all 45 unique generated order IDs;
- every inventory-row quantity and total inventory of 1,999,949;
- zero operation errors.

Unique deterministic order IDs turn duplicate writes into primary-key failures.
The final snapshots expose lost admitted writes. Order creation remains one pgx
transaction. Unit tests also cover deterministic workload generation, unique
write IDs, all generated policy outcomes, explicit state history, metrics
counts, and percentile aggregation.

## 4. Benchmark methodology

- PostgreSQL 17.11 in the committed Docker Compose setup on Windows/amd64;
  Go 1.26.2; pgx 5.7.6; 16 logical CPUs.
- Dataset: 100 customers, 20 inventory rows, 500 seeded orders.
- Seed: `20260821`; operation mix: 55% point read, 25% recent-order range
  read, 10% order transaction, 10% atomic inventory update.
- Fixed-rate open-loop arrivals: steady 15 s at 500/s; burst 2 s at 5,000/s;
  normal 15 s at 500/s; overload 10 s at 10,000/s; recovery 20 s at 500/s.
- 5,000 unmeasured warmup operations per lane, followed by a database reset,
  `VACUUM ANALYZE`, and `CHECKPOINT` to normalize statistics and dirty pages.
- Identical 8-connection pools, 30 s request timeouts, schema, seed, reset,
  durations, and dataset for every lane.
- Two retained normalized runs. Run 1 order was admission, scheduled, baseline;
  run 2 order was scheduled, baseline, admission.
- Recovery is the first of two consecutive one-second post-overload windows
  whose p95 is within 25% of pre-overload normal p95.

The baseline uses ordinary concurrent goroutines and pgxpool's normal wait
behavior. It is neither serialized nor given a smaller pool. Each run lasts
about 62 seconds per lane and attempts exactly 135,000 operations.

## 5. Results

### Whole-run outcomes

| Run | Lane | Completed / attempted | Rejected | Throughput/s | p50 ms | p95 ms | p99 ms | Recovery s | Avg batch |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | Baseline | 135,000 / 135,000 | 0 | 2,177.5 | < clock resolution | 2.12 | 2.72 | 1 | 1.00 |
| 1 | Admission only | 128,429 / 135,000 | 6,571 | 2,071.5 | 0.51 | 17.20 | 20.57 | 1 | 1.00 |
| 1 | Scheduled | 135,000 / 135,000 | 0 | 2,177.5 | 0.53 | 2.16 | 2.63 | 1 | 1.46 |
| 2 | Baseline | 135,000 / 135,000 | 0 | 2,177.5 | < clock resolution | 2.24 | 3.64 | 1 | 1.00 |
| 2 | Admission only | 135,000 / 135,000 | 0 | 2,177.5 | < clock resolution | 2.17 | 2.86 | 1 | 1.00 |
| 2 | Scheduled | 135,000 / 135,000 | 0 | 2,177.5 | 0.54 | 2.17 | 2.64 | 1 | 1.46 |

Sub-millisecond zeros reflect the Windows timer resolution and are reported as
such rather than interpreted as zero-cost operations.

### Primary overload comparison

| Run | Baseline overload p99 | Scheduled overload p99 | Change | Scheduled rejects |
|---|---:|---:|---:|---:|
| 1 | 2.76 ms | 2.58 ms | 6.7% lower | 0 |
| 2 | 3.84 ms | 2.59 ms | 32.7% lower | 0 |

Scheduled steady-state p99 was 2.84/2.77 ms versus baseline 2.09/2.01 ms, a
roughly 0.7 ms batching/control tax. The scheduled lane removed pgx-pool
contention (0.5–1.0 ms total acquire wait versus 9.2–19.9 seconds baseline)
by feeding at most eight worker calls into the eight-connection pool.

Scheduled allocation was 281 MB in both runs versus 187–188 MB baseline, with
15–16 GC cycles versus 10. Heap-system high-water readings were noisy (51–75 MB
scheduled, 88–96 MB baseline). The scheduler improved overload control but did
not reduce total Go allocation; normal Go GC remained enabled throughout.

## 6. Ablation

Admission-only used the same generated policy and 128-operation bound but a
maximum batch size of one. It was neutral in run 2, but in run 1 it crossed the
capacity threshold, rejected 6,571 overload requests, lost 4.9% useful
throughput, and reached 21.09 ms overload p99. Full batching averaged 1.46
requests per physical dispatch and completed all work in both runs at stable
2.58–2.59 ms overload p99. In this experiment, batching/worker shaping—not
rejection by itself—explains the useful result.

## 7. Interpretation

| Idea | Classification | Evidence |
|---|---|---|
| Bounded admission | Inconclusive | Correct and bounded, but admission-only was neutral once and harmful once near capacity. |
| Explicit scheduler state | Neutral | Deterministic and inspectable; no independent performance ablation. |
| Batching | Useful | Stable overload p99 and full completion in both runs; admission-only was less stable. |
| Bounded queue capacity | Useful | Prevented work from entering an unbounded execution path; full lane needed no rejection while keeping pool wait negligible. |

## 8. Limitations

This is a synthetic, single-host, PostgreSQL-only workload with two retained
runs. Pilot runs exposed substantial host-state noise, which motivated warmup,
reset normalization, and rotated order; more repetitions remain desirable.
The fixed 2 ms batching window adds steady-state latency. Batch fill is low
(1.46 of 8), and there is no fixed-pgx-batch baseline yet. CPU utilization was
not sampled. There is no ORM, adaptive scheduling, HSFM, Smith prediction,
custom storage, direct I/O, custom allocator, or unmanaged memory. Generated
Oct names and runtime baggage remain compiler-oriented.

## 9. Next recommendation

Add a fixed pgx-batching baseline with the same 2 ms/8-request policy, so the
next experiment can separate ordinary batching/worker shaping from value added
by generated admission and explicit control state.

