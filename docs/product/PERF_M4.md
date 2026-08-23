# OCTETDB-PERF-M4 — Default-vs-Specialized Positioning Benchmark

## 1. Verdict

**Success**

PERF-M4 created fair, correct, durable lanes for six workload families; pinned
the released default product; integrated generated Oct code only after
profiling; ran current TigerBeetle only for transfers; preserved negative
results and invalidated runs; and reached decisions A–E.

The product result is mixed and useful. On the primary WSL2/ext4 environment,
default OctetDB was only 0.12–0.18× PostgreSQL throughput for the four
concurrent durable mutation workloads and 0.14× for the 70/20/10 mix. It was
exceptional for embedded point reads. Compiled W5 queries were 63–92× default
for focused full filters and 220× default in the primary mixed query lane, but
no honest mutation specialization was justified. The benchmark succeeds; the
broad default-competitiveness hypothesis does not.

## 2. Research questions

1. How competitive is released OctetDB v0.2.0 without Oct for bounded embedded
   OLTP?
2. How much additional ceiling does minimum, profile-directed Oct
   specialization provide without changing application semantics?
3. Does specialization remain progressive and localized?
4. Which bounded semantics create advantage, and where does conventional
   database machinery win?
5. **What engineering machinery disappears, remains, or becomes harder when
   the application/database boundary is expressed entirely through ordinary
   Go instead of Go + SQL?**

## 3. Hypotheses

| Hypothesis | Result | Evidence |
| --- | --- | --- |
| H1 — default competitiveness | **Falsified broadly; supported narrowly** | WSL concurrent durable default was 0.12–0.18× PostgreSQL on W1–W4 and 0.14× on W6. Point lookup was 67× PostgreSQL; Windows mutation controls were 0.80–0.98× PostgreSQL. |
| H2 — specialization multiplier | **Supported for read-only W5, not generally** | W5 full filters improved 63–92× across 1k–100k; primary W5 improved 220×. W1–W4/W6 remained S0. |
| H3 — progressive specialization | **Partially supported** | Domain structs, durable store, and operation interface remained. One localized adapter and 26 nonblank Oct LOC sufficed, but building/cohering the materialized read form is not product-owned. |
| H4 — bounded-system advantage | **Supported** | W5 multiplier increased with population; early-stop examined the same 10–10,000 records in both OctetDB lanes while compiled execution removed JSON work. |
| H5 — conventional-database advantage | **Supported** | PostgreSQL group commit, secondary indexes, no-match queries, external tooling, and relational/ad-hoc capability materially outperformed or simplified the corresponding work. |

## 4. Systems and guarantee table

This table precedes comparative conclusions intentionally.

| Property | PostgreSQL | OctetDB default | OctetDB + Oct | TigerBeetle reference |
| --- | --- | --- | --- | --- |
| topology | external native WSL service; pgx pool over loopback TCP | embedded/in-process Go | same embedded Database; generated query package in-process | external WSL service; Go client over loopback TCP |
| replicas | one; no replication | one; replication unavailable | same | one production-mode replica; normal multi-replica repair/HA absent |
| durable boundary | transaction commit; `synchronous_commit=on`, `fsync=on`, `full_page_writes=on` | accepted decision WAL frame written and synchronized before apply/ack | identical for durable state; W5 generated queries are read-only | quorum-prepared durable write; quorum is one in this run |
| transaction model | read committed SQL transactions; row locks/conditional updates | database-wide serialized `Database.Mutate` callback and atomic multi-Dataset writes | identical; no generated mutation backend | fixed ledger state machine and immutable transfers |
| idempotency | retained unique command/event row; unbounded for run | exact database-wide command result within 100k default dedupe horizon | identical | immutable u128 transfer ID |
| conflicts | row locks, unique constraints, predicates; hot locks queue | serialized admission; callback sees current committed value | identical | ordered single state machine; domain validation |
| ordering | SQL order only with explicit `ORDER BY` | ascending record key scans; ordered mutation authority | generated array source order equals Dataset materialization order | ordered ledger events/IDs under TigerBeetle semantics |
| query/index model | primary keys plus obvious `(status,id)` and `(available,id)` indexes | point key and deterministic full Dataset scan; no predicate index | W5 S1 fused FLOW over materialized typed array; no durable index | fixed account/transfer lookup/filter domain only |
| batching | one logical command/transaction in primary; PostgreSQL may group WAL flushes internally | one public mutation; no public group/batch API | same | one transfer per client request in current common batch-1 lane |
| sync behavior | server may group concurrent commit flushes without weakening individual commit durability | one synchronized WAL append per accepted command | same | TigerBeetle journal/storage cadence |
| recovery | PostgreSQL WAL; checksums on; not restart-timed here | checksummed snapshot plus WAL tail; corruption fails closed | same durable recovery; W5 materialization rebuilt after open | VSR data-file recovery; peer repair absent with one replica |
| replication | supported, disabled | unavailable | unavailable | supported, disabled for closest local comparison |
| safe-Go requirement | Go client; PostgreSQL native server | safe Go, GC, no unsafe/cgo/manual allocator | generated safe Go, GC, no unsafe/cgo/manual allocator | native TigerBeetle server plus Go client |

Guarantees are close enough for durable local W1–W6 comparisons but not
identical. Embedded calls and TCP calls are different topology; PostgreSQL
earns throughput from ordinary group commit; TigerBeetle's one-replica lane
does not include its normal HA/repair guarantee.

## 5. Environment

Primary: AMD Ryzen 7 7700X (8C/16T), 32 GiB, 2 TB SHPP41 NVMe, WSL2 Linux
6.6.87.2, ext4, Go `1.27.0-X:nodwarf5`, `GOMAXPROCS=16`. PostgreSQL 18.6 was
native WSL with checksums and all durability switches on. OctetDB was the
independent module dependency `v0.2.0` at commit
`76a0659ae9125c8c9b689fe155f2ff30a4b30fc9`. Oct was pinned to
`ca22ab8dfc20ac6d6c59dd34976789cd2c84ad2e`. TigerBeetle was 0.17.9 commit
`cc1c06a...`, ReleaseSafe, Direct I/O.

Windows/NTFS and Docker PostgreSQL 17.11 form a same-physical-host cross-check,
not the primary matrix. Exact capture is in `evidence/PERF_M4/environment/`.

## 6. Benchmark methodology

- Independent Go module pinned to public v0.2.0; no `replace`, internal import,
  researchengine, or current-main default path.
- One prebuilt `-trimpath` binary per suite; systems run separately.
- Three repetitions, lane order rotated on WSL; median reported with raw
  min/max retained in `summaries/summary-wsl.json`.
- Warmup was 10% capped at 100 operations; setup excluded.
- Primary population: 1k for W1–W4/W6, 10k for W5; eight clients.
- Primary measured operations: 1k W1–W5 and 5k W6.
- W5 focused runs: 500 queries, one client; 1k/10k/100k populations and all
  required selectivities.
- Latencies use a high-resolution monotonic timer (`QueryPerformanceCounter`
  on Windows); p50/p95/p99/max are per logical operation.
- Process CPU, RSS, Go heap/allocation/GC, storage, WAL position/bytes, records
  examined, and correctness are emitted in every primary JSON. PostgreSQL
  server CPU/RSS sidecars are separate because the service is external.
- Invalid runs are quarantined with reasons. No statistical outlier was
  deleted.

The user-provided ROI definitions were fixed before results: Excellent = large
gain/small localized work; Good = meaningful gain/moderate localized work;
Marginal = small gain relative to complexity; Poor = complex or unavailable
specialization for little practical gain; Negative = regression/maintenance
damage without compensation.

## 7. Workload definitions

| Workload | Model and measured actions | Invariants |
| --- | --- | --- |
| W1 transfer | two accounts; debit/credit of one; stable command ID | conserved total, no negative balance, atomic pair, retry suppressed |
| W2 inventory | Reserve, Release, Restock | nonnegative available/reserved, conditional reserve, atomic update, retry suppressed |
| W3 jobs | durable seed/Create; Claim, Complete, Fail, Requeue, `ListReady(10)` | guarded state transition, one claim wins serialized state, deterministic ready order, retry suppressed |
| W4 webhook | process external ID; duplicate returns exact stored result | one record/result per external ID; exact duplicate result validated |
| W5 query | point, filter, filter+take, filter+map, count | identical filtered order/result; exact examined count for take |
| W6 mixed | 70/20/10 and 50/40/10 read/write/discovery | point/read result valid, inventory invariants, discovery order |

## 8. PostgreSQL implementations

pgx pooling used a normal external service. Tables had primary keys, check
constraints, retained command identities, and obvious `(status,id)` and
`(available,id)` indexes. W1 locks the two rows in key order before update. W2
uses conditional updates. W3 uses predicate-guarded transitions and indexed
ready selection. W4 uses `INSERT ... ON CONFLICT`, then selects and validates
the original result on duplicate. W5 uses indexed `ORDER BY ... LIMIT` and
`count(*)`. No ORM, disabled durability, missing obvious index, or exotic
tuning was used.

## 9. Default OctetDB implementations

Every default lane uses only `OpenCatalog → Database → Bucket → Dataset`,
`Database.Mutate → Tx`, `Dataset.Get`, `Dataset.Scan`, and `ScanDataset[T]`,
with `DefaultKeyedOptions()`. JSON Go structs are durable records. Commands are
database-wide stable IDs. W5 uses the public deterministic scan and
`ScanStop`. There is no private layout, generated code, researchengine, or Oct
compiler in Lane B.

## 10. Specialization process

1. Default applications ran correctly.
2. W1 and W5 CPU/allocation profiles were collected.
3. W1 showed sync as 91.6% of Windows sampled CPU; W1–W4 stayed S0.
4. W5 showed JSON validation/decode/allocation dominance.
5. Twenty-six nonblank Oct LOC described `Job` plus filter, take, map, and
   count queries.
6. Existing `EmitGoSource` generated one typed safe-Go FLOW package; no
   compiler change or hand repair.
7. The default W5 Dataset remained durability authority. At initialization it
   was scanned once into the generated typed array. Default and compiled filter
   order/results were compared before and after measurement.

W6 stayed S0. Updating a detached mirror after commit would create a stale-read
window; locking the whole application or bypassing the product would violate
the same-application/fairness intent.

## 11. TigerBeetle scope

TigerBeetle ran W1 only. W2–W6 are **N/A**, not zero and not ranked. Current
W1 uses batch 1 because the released default OctetDB application has no honest
equivalent to TigerBeetle's homogeneous batch request. Historical batch-64/512
data is context only and is not substituted into current tables.

## 12. W1 transfer results

Primary WSL, 1k accounts, eight clients:

| Lane | ops/s | p99 | client/embedded RSS | WAL/logical op |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL | 5,493 | 2.39 ms | 17.1 MiB client; server summed-RSS upper bound 467 MiB | 631 B |
| OctetDB default | 977 | 9.63 ms | 15.1 MiB | 377 B |
| OctetDB + Oct (S0) | 975 | 9.10 ms | 15.6 MiB | 377 B |
| TigerBeetle, WSL batch 1 | 106 | 15.59 ms | 31 MiB client; 2.94 GiB server | N/A; 1.14 GiB allocated data file after setup/run |

Default competitiveness is 0.18× PostgreSQL throughput; S0 multiplier is
1.00×. TigerBeetle qualification: in this one-replica, batch-1, loopback WSL
transfer workload, TigerBeetle measured 106/s. This does not generalize to its
intended batching/replication regime.

## 13. W2 inventory results

| Lane | ops/s | p99 | RSS | WAL/op |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL | 6,756 | 2.24 ms | 14.0 MiB client; server summed-RSS ≤473 MiB | 625 B |
| Default | 957 | 9.61 ms | 14.1 MiB | 295 B |
| Specialized S0 | 974 | 9.72 ms | 13.8 MiB | 295 B |

Default/PostgreSQL is 0.14×. No compiler-owned hot path was justified.

## 14. W3 job results

| Lane | ops/s | p99 | RSS | WAL/op |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL | 2,617 | 6.22 ms | 13.8 MiB client; server summed-RSS ≤440 MiB | 422 B |
| Default | 324 | 30.43 ms | 14.7 MiB | 198 B |
| Specialized S0 | 330 | 28.51 ms | 14.1 MiB | 198 B |

The scan inside the mixed transition workload serializes with mutations and
amplifies the default durability bottleneck. Default/PostgreSQL is 0.12×.

## 15. W4 webhook results

| Lane | ops/s | p99 | RSS | WAL/op |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL | 2,265 | 4.89 ms | 13.4 MiB client; server summed-RSS ≤413 MiB | 407 B |
| Default | 284 | 39.81 ms | 13.5 MiB | 290 B |
| Specialized S0 | 287 | 35.40 ms | 13.6 MiB | 290 B |

These are corrected runs in which every duplicate retrieves/decodes and
validates the original durable result. The pre-audit lane is quarantined.
Default/PostgreSQL is 0.13×.

## 16. W5 query results

Primary 10k mixed-query lane:

| Lane | ops/s | p99 | RSS | allocation/op |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL | 17,036 | 1.56 ms | 18.2 MiB client; server summed-RSS ≤434 MiB | 24.8 KiB |
| Default | 775 | 15.19 ms | 22.4 MiB | 397 KiB |
| Specialized S1 | 170,279 | 0.15 ms | 19.4 MiB | 87 B |

Default/PostgreSQL is 0.05×; specialized/default is 219.7×.

Focused WSL, 10k, one client, 25% selectivity:

| Query | PostgreSQL | Default | Specialized | S1/default |
| --- | ---: | ---: | ---: | ---: |
| point | 12,837/s | 864,756/s | 1,249,304/s | 1.44× (point remains default path) |
| filter | 2,814/s | 315/s | 21,478/s | 68.1× |
| filter + map | 1,912/s | 314/s | 20,133/s | 64.1× |
| count | 3,502/s | 1,993/s | 12,438/s | 6.24× |
| filter + Take(10) | 13,277/s | 95,069/s | 2,710,453/s | 28.5× |

## 17. W6 mixed-workload results

70/20/10 primary:

| Lane | ops/s | p99 | RSS | WAL/op |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL | 9,847 | 4.28 ms | 15.8 MiB client; server summed-RSS ≤469 MiB | 128 B |
| Default | 1,340 | 30.29 ms | 14.5 MiB | 69 B |
| Specialized S0 | 1,435 | 29.07 ms | 14.7 MiB | 69 B |

At 50/40/10: PostgreSQL 15,148/s, default 1,857/s, S0 control 2,509/s.
The S0 differences are run variance, not compiler effects. Scans serialize
with writes and there is no coherent specialized read publication.

## 18. Optional relationship-heavy result

No misleading W7 performance number was fabricated. The orders/customers/
line-items/inventory shape is structurally PostgreSQL-favored: foreign keys,
join, indexed order/sort, pagination, and aggregate fit ordinary SQL. OctetDB
would require multiple scans, application-side relationships, and explicit
sorting/paging. This is sufficient boundary evidence, not a claim that a
nonexistent OctetDB join was benchmarked.

## 19. Contention results

W1 WSL current controls:

| Shape | PostgreSQL | Default | S0 control | Default p99 | PG p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| uniform, c1 | 544/s | 292/s | 292/s | 4.79 ms | 3.29 ms |
| hot set, c8 | 946/s | 292/s | 268/s | 32.65 ms | 25.17 ms |
| one hot source, c32 | 443/s | 264/s | 268/s | 131.72 ms | 289.88 ms |

PostgreSQL is 1.7–3.2× faster, but its p99 becomes worse at the single hot row.
OctetDB's serialized design limits throughput early but bounds conflict logic.

## 20. Population scaling

W5 25%-selectivity full filter:

| Population | PostgreSQL | Default | Specialized | S1/default | Default RSS | S1 RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1k | 8,131/s | 3,649/s | 231,381/s | 63.4× | 15.5 MiB | 13.6 MiB |
| 10k | 2,814/s | 315/s | 21,478/s | 68.1× | 22.5 MiB | 19.0 MiB |
| 100k | 317/s | 24.7/s | 2,278/s | 92.3× | 89.6 MiB | 80.8 MiB |

One million is N/A: released default `MaxRecords` is 100k. Raising the option
would no longer represent the default product and was not done.

## 21. Batch scaling

Primary application batch size is one. PostgreSQL may group concurrent durable
flushes internally; OctetDB v0.2 exposes no public mutation batch/group-commit
API; TigerBeetle's request batching would change the application interface.
Therefore 64/512 are N/A for the current common lane rather than fake rankings.
Historical TIGER-COMPARE-M0 batch sweeps remain context and show why batch-1
TigerBeetle must not be generalized.

## 22. Memory/GC analysis

Primary Go-client/embedded measurements:

| Workload | Default alloc/op | Specialized alloc/op | Default GC cycles | Specialized GC cycles |
| --- | ---: | ---: | ---: | ---: |
| W1 | 8.5 KiB | 8.5 KiB | 3 | 3 |
| W2 | 2.5 KiB | 2.5 KiB | 0 | 0 |
| W3 | 2.6 KiB | 2.5 KiB | 1 | 1 |
| W4 | 2.8 KiB | 2.8 KiB | 0 | 0 |
| W5 | 397 KiB | 87 B | 83 | 0 |
| W6 | 715 B | 715 B | 1 | 1 |

Default W5 profile accumulated 3.12 GiB/99.3% of allocation space through the
typed Dataset scan; specialized W5 removed query-time JSON decode. GC is an
effect of representation here, not a standalone pathology.

## 23. CPU/cache/profile analysis

- W1 default Windows CPU: 91.6% flat in filesystem sync `cgocall`; transaction
  arithmetic was not worth compiling.
- W5 default CPU: JSON validation/object decode plus allocation/runtime work.
- W5 specialized CPU: 79.0% flat in one generated FLOW step and 97.6%
  cumulative in its typed facade.
- WSL `perf`: default W5 recorded 70.3B instructions, 17.8B cycles, 31.0M cache
  misses, 14.9M branch misses for 1k filters. Specialized recorded 101.8B,
  24.3B, 2.75M, and 1.20M for 100k filters. Per query, generated execution is
  orders of magnitude lower. Different run counts prohibit raw-total IPC
  generalization.

## 24. Storage/WAL analysis

At 1k primary populations, logical JSON payload was 26.9 KiB W1, 42.9 KiB W2,
37.3 KiB W3, and 60.9 KiB W6. W5 payload was 35.5 KiB/365.5 KiB/3.76 MiB at
1k/10k/100k.

Primary measured database/WAL footprints:

| Workload | Default storage | PostgreSQL storage | Default WAL/op | PG WAL/op |
| --- | ---: | ---: | ---: | ---: |
| W1 | 0.63 MiB | 0.48 MiB | 377 B | 631 B |
| W2 | 0.59 MiB | 0.65 MiB | 295 B | 625 B |
| W3 | 0.47 MiB | 0.51 MiB | 198 B | 422 B |
| W4 | 0.31 MiB | 0.30 MiB | 290 B | 407 B |
| W5 read | 2.67 MiB | 2.07 MiB | 0 B | effectively 0 B |
| W6 | 0.67 MiB | 0.54 MiB | 69 B | 128 B |

PostgreSQL includes indexes/metadata; OctetDB includes retained decisions in
its WAL. TigerBeetle's fixed data file allocated ~1.14 GiB after W1 setup/run,
so per-operation storage is not a meaningful comparison at this tiny scale.

## 25. Recovery analysis

WSL, released default, 10k records, three repetitions:

| Case | median open | range | source bytes | correctness |
| --- | ---: | ---: | ---: | --- |
| empty cold catalog | 15.43 ms | 5.79–16.17 | empty | pass |
| W1 snapshot | 23.67 ms | 23.56–23.93 | 2,617,864 snapshot | pass |
| W1 WAL tail | 24.06 ms | 23.36–24.51 | 2,987,704 WAL | pass |
| W3 snapshot | 23.33 ms | 23.32–24.33 | 2,617,864 snapshot | pass |
| W3 WAL tail | 24.87 ms | 24.56–24.91 | 2,987,704 WAL | pass |

WAL preparation intentionally exited without `Close`; every frame had already
been synchronized. Recovered counts, balance conservation, and job status were
exact. PostgreSQL/TigerBeetle restart timing is N/A in current M4 and is a
limitation, not inferred from normal connection startup.

## 26. Specialization multiplier table

| Workload | Default | Specialized | Multiplier | Oct LOC | Go LOC changed | Level | ROI |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| W1 | 977/s | 975/s | 1.00× | 0 | 0 | S0 | Poor — no relevant compiler-owned bottleneck |
| W2 | 957/s | 974/s | 1.02× | 0 | 0 | S0 | Poor — no justified path |
| W3 | 324/s | 330/s | 1.02× | 0 | 0 | S0 | Poor — sync/scan architecture dominates |
| W4 | 284/s | 287/s | 1.01× | 0 | 0 | S0 | Poor — fixed durable overhead dominates |
| W5 | 775/s | 170,279/s | 219.7× | 26 nonblank | 199 nonblank adapter | S1 | Good — huge gain, moderate localized lifecycle work |
| W6 | 1,340/s | 1,435/s | 1.07× measured noise | 0 | 0 | S0 | Poor — coherent mixed specialization unavailable |

“Poor” for S0 means no specialization ROI exists, not that unused Oct code
made the lane slower.

## 27. Specialization effort/ROI

| Metric | W5 S1 |
| --- | ---: |
| agent implementation sessions/turns | 1 continuous session |
| changed integration files | 4: main lane selection, adapter, Oct source, generated file |
| handwritten Oct | 34 physical / 26 nonblank LOC |
| handwritten Go added | 208 physical / 199 nonblank LOC |
| generated Go | 1,473 physical / 1,339 nonblank LOC; 46,598 bytes |
| new concepts | typed materialized read snapshot; generated FLOW facade |
| build steps | generate Go; build ordinary Go module |
| first measured generator build | 448.69 ms; warm-cache 172–205 ms |
| time from generation to first correct integrated smoke | approximately 3 minutes in the evidence session |
| runtime initialization | 0.44 ms/1k; 3.89 ms/10k; 45.72 ms/100k |

ROI is **Good**, not Excellent: runtime gain is enormous and code localized,
but an application-owned materialization/rebuild step is a meaningful product
lifecycle cost.

## 28. Application-code-diff analysis

Domain `record`, workload selection, durability store, command semantics, and
benchmark transport remained shared. Lane C embeds the Lane B backend. W5 adds
only initialization materialization and query dispatch. W1–W4/W6 call the
unchanged default backend. No generated mutation logic, generated database,
unsafe memory, or handwritten generated repair exists.

The important failure is also visible in the diff: the read materialization is
owned by the adapter, not by a transactional product index/publication API.
Read-only W5 is safe; applying it casually to W6 would not be.

## 29. Complexity comparison

Measured nonblank evidence-implementation LOC (descriptive, not quality score):

| Lane | shared Go | lane Go | SQL lines | Oct | generated Go | processes | setup steps | DB-specific concepts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| PostgreSQL | 298 | 284 | 30 | 0 | 0 | 2 | 4 (service/schema/pool/app) | 8 |
| Default | 298 | 413 | 0 | 0 | 0 | 1 | 2 (directory/app) | 6 |
| Specialized W5 | 298 | 413 + 199 | 0 | 26 | 1,339 | 1 | 4 (Oct/generate/build/app) | 9 |
| TigerBeetle W1 | historical shared harness | adapter in prior command | 0 | 0 | 0 | 2 | 5 (format/start/client/app/stop) | fixed ledger domain |

PostgreSQL's SQL file/strings are shorter because relational machinery is
expressive. OctetDB removes a process and SQL but places bounded schema,
relationships, query logic, and data-directory operations in Go.

## 30. Default-mode decision

**A3. Default OctetDB has meaningful performance deficits that should be fixed
before further specialization work.**

The key deficit is one synchronized accepted command at a time with no ordinary
group-commit/batch path. PostgreSQL used normal concurrent group commit without
weaker acknowledgements. Default point reads remain a strong positive.

## 31. Oct-specialization decision

**B2. Oct specialization provides useful but workload-dependent gains.**

W5 is exceptional; durable mutation and coherent mixed lanes currently have no
honest compiler-owned improvement.

## 32. Product-thesis decision

**C3. Specialization is strong, but default mode needs work.**

The “unreasonable ceiling” exists for bounded read execution. The “boring
default” is ergonomically real but not yet competitive enough for concurrent
durable mutation on the primary storage path.

## 33. PostgreSQL-boundary decision

**D2. OctetDB's current niche is narrower than expected.**

It is compelling for embedded point access and specialized stable read sets,
not yet for the broader concurrent durable bounded OLTP niche tested here.

## 34. What PostgreSQL did better

- Ordinary group commit delivered 5–8× default throughput on W1–W4 at c8.
- Secondary indexes made no-match and low-selectivity discovery far cheaper
  than default scans.
- SQL expressed guarded transitions, sorting, count, and Take concisely.
- Mature external inspection, schema, relational, join, repair, backup, and
  operational tools remain unmatched.
- General concurrent workloads did not require a new compiled representation.

## 35. What default OctetDB did better

- In-process 10k point lookup: 864,756/s versus PostgreSQL 12,837/s.
- One directory and ordinary Go structs removed service/network/SQL setup.
- WAL bytes/logical mutation were lower in all primary mutation lanes.
- Deterministic scans and exact bounded command results were simple to express.
- Recovery of 10k snapshot/WAL state was ~23–25 ms and exact.
- Windows/NTFS controls showed default can approach PostgreSQL when sync costs
  converge, exposing a focused runtime/storage opportunity rather than a
  universal application-logic deficit.

## 36. What specialization actually bought

It removed per-record JSON validation/decode and most allocation from W5's hot
loop, fused filter/map/take into one FLOW state machine, preserved source order
and early stop, and scaled the multiplier from 63× at 1k to 92× at 100k focused
filters. It did not change durability, domain structs, service semantics, or
the default point path.

## 37. Where specialization did not help

- W1–W4: durable sync/transaction architecture, not domain arithmetic, was hot.
- W5 point lookup: default was already excellent.
- W6: no product-owned coherent publication/index update boundary.
- PostgreSQL's indexed no-match query still beat default by ~40×.
- Specialization did not reduce the minutes required to populate 100k records
  via individually durable public commands.

## 38. Benchmark limitations

- WSL2 virtual disk and loopback are not bare metal.
- PostgreSQL service summed RSS double-counts shared pages; it is an upper
  bound. Client RSS is exact and separately reported.
- PostgreSQL and TigerBeetle recovery were not restart-timed in M4.
- No 1M OctetDB default population; it exceeds the released default bound.
- No common current batch-64/512 API and no replication comparison.
- W5 specialization is valid for a read-only dataset. Mutable compiled-index
  coherence remains unproven.
- No end-to-end HTTP transport was timed; the shared operation interface stops
  at the service/store boundary.
- W3 Create is exercised in durable setup; measured steady state emphasizes
  transitions and discovery.
- Windows sync bimodality prevents using those numbers as primary; all runs are
  retained.
- W7 is structural analysis, not a forced pseudo-join benchmark.

## 39. Marketing claims that evidence WOULD support

- “OctetDB v0.2.0 provides very fast embedded point lookup for bounded Go
  state; in this 10k-record WSL test it measured about 865k lookups/s.”
- “For a read-only bounded 100k-record filter, Oct-compiled safe Go measured
  about 92× the default JSON scan in this environment.”
- “OctetDB provides durable atomic Go mutations and exact bounded retries
  without running a separate database service.”
- “The performance ceiling is workload-dependent: measured query
  specialization can remove JSON decode/allocation while leaving the ordinary
  application and durable Dataset in place.”

## 40. Marketing claims that evidence would NOT support

- “OctetDB is faster than PostgreSQL.”
- “Default OctetDB is broadly competitive for concurrent durable OLTP.”
- “Oct makes every OctetDB workload faster.”
- “OctetDB is faster than TigerBeetle.”
- “Compiled W5 is a transactionally maintained secondary index.”
- “OctetDB replaces SQL for joins, ad-hoc analytics, or rich relational work.”
- “The 220× primary W5 multiplier applies to arbitrary applications.”
- “Embedded and client/server latencies are topology-equivalent.”

## 41. Required next-work decision

**E1. Improve default OctetDB runtime architecture.**

Evidence priority is the released default durability path, specifically a
boring, semantics-preserving group-commit/durable batching design that retains
per-command acknowledgement and exact results. It should be measured before
adding more specialization surface.

## 42. Exactly one next recommendation

Implement and benchmark **public-default group commit for concurrent
`Database.Mutate` calls**, preserving the current acknowledgement, ordering,
idempotency, and recovery contracts. The acceptance test is W1–W4 at c1/c8/c32
on ext4 plus Windows, with no application API rewrite and no relaxed durability.

## Appendix A — ordinary Go versus Go + SQL machinery

This answers the added research question directly.

| Machinery | Ordinary-Go OctetDB boundary | Go + SQL/PostgreSQL boundary |
| --- | --- | --- |
| schema expression | Go structs + explicit TypeIdentity/bounds | DDL, types, constraints, migrations |
| transaction expression | Go callback and `Tx` methods | SQL transaction + statements/row locks |
| query expression | Go scan/predicate/control | SQL planner, predicates, indexes, joins |
| serialization boundary | JSON inside embedded engine | driver parameters/rows over protocol |
| connection/process | disappears | pool, credentials, port, service health/startup |
| WAL/sync/recovery | remains, absorbed by library/data directory | remains, owned by service/operator |
| idempotency | explicit retained command API | explicit unique schema + SQL/application logic |
| index/planner | absent by default; application/Oct materialization harder | mature built-in machinery |
| inspection/repair | Go tools must be built | `psql`, catalogs, EXPLAIN, ecosystem |
| joins/ad-hoc work | becomes application scans/relationships | ordinary relational strength |
| deployment | one process, one owned directory | app + database process and lifecycle |

What disappears is real: SQL, network round trips, service/pool/deployment
machinery, and schema migration files for these bounded cases. What remains is
also real: storage, sync, recovery, conflicts, idempotency, schema identity, and
capacity decisions. What becomes harder is the part PostgreSQL has spent
decades productizing—indexes, relational composition, ad-hoc access,
observability, backup/repair, and concurrent query publication.

## Appendix B — evidence map

- `docs/product/evidence/PERF_M4/README.md`: reproduce the suites.
- `raw/wsl/`: primary 207 repetitions plus recovery/resource probes.
- `raw/windows/`: portability and temporal controls.
- `raw/tiger-wsl/`: current W1 specialist runs.
- `summaries/summary-wsl.json`: medians and full min/max spreads.
- `profiles/`: CPU, heap, and hardware counters.
- `specialized/query.oct`: handwritten specialization.
- `harness/specialized/generated.go`: unedited generated Go.
- `PRELIMINARY_NOTES.md`: time-stamped lab notebook written before the WSL
  primary result completed; useful for seeing how the evidence changed the
  provisional conclusion.
