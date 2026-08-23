# PERF-M4 lab notebook — the fun version

> Preliminary, August 23, 2026. This is the notebook, not the verdict. The
> rotated WSL2/ext4 matrix is still finishing its 100k-record repetitions.
> Numbers below are medians unless I explicitly say otherwise.

## The one-sentence plot twist

Default OctetDB is not waiting for Oct to rescue it on durable bounded OLTP.
It already runs in PostgreSQL's performance neighborhood—and sometimes ahead—
but its scan/query path falls off a JSON cliff as populations grow. Oct then
turns that cliff into a racetrack, at the cost of materializing a second,
read-optimized representation whose lifecycle is not yet a boring product
feature.

That is a much more interesting result than “compiler makes number go up.”

## Default mode did not embarrass itself

The first controlled Windows primary pass, 1k records and eight clients:

| Workload | PostgreSQL | OctetDB default | Default / PG | Default p99 | PG p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| transfer | 3,051 ops/s | 2,996 ops/s | 0.98× | 3.38 ms | 3.64 ms |
| inventory | 3,667 ops/s | 2,942 ops/s | 0.80× | 3.29 ms | 2.85 ms |
| jobs + ready query | 4,117 ops/s | 3,712 ops/s | 0.90× | 2.66 ms | 3.23 ms |
| webhook, old pre-audit lane | 3,847 ops/s | 3,513 ops/s | 0.91× | 2.94 ms | 3.30 ms |
| mixed 70/20/10, early disk regime | 10,546 ops/s | 14,326 ops/s | 1.36× | 3.08 ms | 2.67 ms |

The webhook row above is intentionally labeled old: the audit caught that the
PostgreSQL adapter suppressed a duplicate but did not fetch and validate the
original durable result. After fixing both adapters, under the later/slower
Windows sync regime, PostgreSQL measured 2,104 ops/s and OctetDB 734 ops/s.
That is why the formal report will use the rotated WSL matrix rather than mash
those two disk regimes together.

Still, the original four mutation rows answer an important fear: released
default mode was not 10× behind and waiting for a compiler miracle. It was
0.80–0.98× PostgreSQL throughput with comparable tails.

## W1 profiling says: please stop optimizing the arithmetic

In the durable default transfer CPU profile, `runtime.cgocall`—the Windows
filesystem synchronization boundary—accounted for about **91.6%** of samples.
`Database.Mutate` accumulated 97.6%; JSON and map work were scraps beside the
sync.

This is the cleanest negative specialization result in the milestone:

- The transfer state transition is not the hot cost in primary durable mode.
- Compiling subtraction and invariant checks harder would be theater.
- W1 therefore stays **S0**. The “specialized” control deliberately calls the
  exact default backend.
- Rotated same-time controls put S0 default/specialized ratios around
  0.96–1.01× across uniform, hot-set, and hot-key cases, as they should.

The compiler did not lose here. It correctly had nothing valuable to buy.

## Then W5 walked in carrying a flamethrower

At 10k records, one client, 25% selectivity:

| Query | PostgreSQL | Default | Oct S1 | S1 / default |
| --- | ---: | ---: | ---: | ---: |
| point lookup | 3,556/s | 854,555/s | 884,486/s | 1.04× |
| filter | 1,335/s | 167/s | 22,166/s | 132× |
| filter + map | 1,129/s | 160/s | 22,141/s | 138× |
| count | 2,068/s | 1,897/s | 11,862/s | 6.25× |
| filter + `Take(10)` | 3,514/s | 49,845/s | 2,309,469/s | 46.3× |

Read that point-lookup row twice. The embedded product needs no compiler help
when it already knows the key: about **0.85 million lookups/s**, roughly 240×
the ordinary TCP PostgreSQL call in this topology. Oct adds essentially
nothing. This is exactly what progressive specialization should look like:
leave the good path alone.

The full typed scan is the opposite. Default `ScanDataset` validates and
decodes JSON record by record. The allocation profile attributed about:

- 1.48 GiB to `encoding/json.Unmarshal`;
- 838 MiB directly to the typed scan callback wrapper;
- 3.12 GiB / 99.3% cumulatively to the Dataset scan across the profiled run.

The generated Oct lane's CPU profile is wonderfully boring: about **79%** in
one generated `FilterOnly.__octStep`, with no iterator framework, goroutine,
channel, SQL parser, or planner hiding underneath it.

## Selectivity produced comedy-sized numbers—but honest comedy

For `Take(10)` over 10k records:

| Shape | Records examined/query | Default | Oct S1 | PostgreSQL |
| --- | ---: | ---: | ---: | ---: |
| first ten match | 10 | 189,897/s | 3,203,075/s | 3,419/s |
| 100% match | 10 | 210,944/s | 2,161,695/s | 3,431/s |
| 50% | 19 | 107,183/s | 2,962,085/s | 3,436/s |
| 25% | 37 | 49,845/s | 2,309,469/s | 3,514/s |
| 10% | 91 | 21,939/s | 1,311,303/s | 3,349/s |
| 1% | 901 | 2,062/s | 290,901/s | 3,479/s |
| no match | 10,000 | 177/s | 29,148/s | 3,575/s |

Both OctetDB lanes examined exactly the same upstream record count. The speedup
is not a fake early-stop difference. PostgreSQL uses the obvious
`(status, id)` index, so it examines/result-materializes only the matching rows
and stays near 3.4k calls/s across selectivity. It wins the no-match default
comparison by about 20×; compiled Oct beats it by about 8× in this embedded vs
TCP topology.

These are not universal database claims. They are a very sharp picture of
fixed per-call network/SQL machinery versus an in-process compiled loop.

## Scaling is the most thesis-shaped graph even without drawing it

Full filter, 25% selectivity:

| Population | PostgreSQL | Default | Oct S1 | S1/default |
| ---: | ---: | ---: | ---: | ---: |
| 1k | 2,923/s | 2,067/s | 214,005/s | 104× |
| 10k | 1,335/s | 167/s | 22,166/s | 132× |
| 100k | 195/s | 14.9/s | 2,121/s | 142× |

The multiplier grows with source size, just as H4 predicts. So does the
integration awkwardness: public v0.2 has no bulk-load path, and the honest
specialized representation is built only after 100k individually durable
default writes. On this setup, that takes minutes per fresh repetition.

Fast steady state, expensive runway.

## The machinery question: what vanished, what merely moved?

Preliminary inventory for the new research question:

### Actually disappears from the application/deployment

- SQL strings and their second type/expression system;
- schema migration SQL for these bounded document-shaped workloads;
- driver-to-server transaction choreography;
- connection pool sizing as an application performance variable;
- a database service, port, credentials, health check, and startup order;
- network serialization and loopback round trips;
- translating Go domain records into relational rows for point-shaped access.

### Does not disappear; OctetDB absorbs or re-expresses it

- WAL framing, checksums, sync, snapshots, and recovery;
- atomic admission and conflict serialization;
- command dedupe retention;
- schema/type identity (now `TypeIdentity` plus Go structs rather than DDL);
- bounds/capacity decisions;
- operational responsibility for the owned data directory.

### Becomes harder without SQL

- secondary indexes that PostgreSQL can express in one ordinary DDL line;
- joins, relationship traversal, sorting, pagination, and aggregation;
- ad-hoc inspection with `psql`;
- bulk ingest and data repair workflows;
- query-plan introspection and mature database observability;
- updating a specialized materialization while preserving transactional query
  semantics under concurrent writes.

That last bullet is why W6 remains S0. I could make a fast benchmark by keeping
a detached dense mirror and updating it “soon after” commits. I did not. A
query racing between durable commit and mirror update would observe stale state.
Fixing that honestly needs a product-owned publication/index contract, not a
clever benchmark adapter.

## TigerBeetle was hilariously slow at batch 1, and that proves almost nothing

Current WSL2, one production-mode replica, one transfer per client request:

- **106 transfers/s** median;
- **9.22 ms p50**, **15.6 ms p99**;
- about **3.09 GiB** server RSS;
- all value conserved, duplicate transfer ID suppressed.

OctetDB default's earlier Windows W1 was ~3k transfers/s. Do not write “OctetDB
is faster than TigerBeetle” on a billboard. The honest statement is: at batch
1, single replica, and these different Windows/WSL storage paths, the fixed
TigerBeetle client/server + consensus/storage cadence dominates. Historical
TIGER-COMPARE-M0 already showed TigerBeetle reversing the result spectacularly
at batch 512. PERF-M4's public default API has no comparable batch transaction
surface, so batch 1 is the only current common application shape.

## We caught our own benchmark cheating twice

Not intentional cheating, but exactly the sort that turns into benchmark lore
if nobody audits it:

1. I launched CPU profiles while a dimension run was still active. Thirty-four
   overlapping files were moved to
   `raw/invalidated_concurrent_profile_20260822`; none are summarized.
2. The first PostgreSQL resource sampler used the wrong shell syntax for
   positional fields 14/15, then missed CPU from backend processes that exited
   before the final sample. Both bad sets are quarantined. The fixed sampler
   tracks the peak cumulative tick count while the client is alive.
3. The W4 durable-result gap described above invalidated the old W4 primary
   cells. They are quarantined and replaced, not quietly edited.

Honestly, this may be my favorite result. The benchmark is capable of saying
“our number is invalid” and throwing it out with a receipt.

## Hardware counters: fewer misses, many more tiny branches

Separately labeled WSL diagnostic runs (not primary throughput):

- default W5: ~70.3B instructions / 17.8B cycles, 31.0M cache misses, 14.9M
  branch misses across 1,000 full filters;
- specialized W5: ~101.8B instructions / 24.3B cycles, 2.75M cache misses,
  1.20M branch misses across **100,000** full filters.

Normalize per query and the representation change is enormous: the generated
lane performs 100× as many queries in the run while recording fewer total cache
and branch misses. Do not overread absolute IPC—the run lengths differ—but the
direction agrees strongly with the CPU/allocation profiles.

## Current betting line

If the rotated WSL primary matrix lands near the Windows controls, my current
provisional choices are:

- **A2** — default is competitive in some workload classes, not scans;
- **B2** — specialization is extremely useful but workload-dependent;
- **C1, narrowly** — “boring Go database with an unreasonable performance
  ceiling” is supported for bounded stable/read-mostly query data, while the
  publication/update story is unfinished;
- **D1** — compelling bounded niche; PostgreSQL remains the rich relational,
  indexed, ad-hoc default;
- likely **E3** — add one product-owned compiled read-publication/index feature
  so W5's speed does not require an application-managed detached materialization.

The formal report may downgrade C1 to C2 if the final integration-cost accounting
shows that the speed is too detached from normal mutation lifecycle. That is
the live question now—not whether compiled loops are fast. They are absurdly
fast. The question is whether the product can make them boring.

## Scoreboard after the buzzer

The rotated WSL2/ext4 primary matrix did change the call. It confirmed **B2**
but moved the final decisions to **A3, C3, D2, and E1**: released default mode
has meaningful concurrent-mutation deficits versus PostgreSQL; specialization
is spectacular but confined to the read-query shape we could specialize
honestly; and the next job is improving the default runtime rather than adding
more specialization surface. That reversal is exactly why this notebook is
kept as preliminary evidence instead of being polished into hindsight.
