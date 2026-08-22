# TIGER-COMPARE-M0 — TigerBeetle vs OctetDB architecture and memory pressure

## 1. Verdict

**Success**

Real TigerBeetle 0.17.9, OctetDB M2 Oct, the direct-Go OctetDB control, and native PostgreSQL 18.6 were run on the same WSL2 Linux host and ext4 virtual disk. The experiment retained the existing M2 engine, measured a common durable transfer, swept batches and contention, collected Go GC/allocation profiles and Linux hardware counters, forced recovery, checked storage, and completed a five-minute run. Every retained workload conserved value and suppressed the retried identifier.

The headline is deliberately conditional: at offered batch 64, direct Go slightly exceeded TigerBeetle and Oct was within 10% of it; at batch 512, TigerBeetle was 3.17× direct Go and 3.63× Oct. TigerBeetle was much less efficient at steady-state batch 1. GC pauses were always sub-millisecond, GOGC sensitivity was small, and GC capacity share was small in durable runs. At 100k accounts, however, the Go agent/mailbox representation occupied gigabytes and a large-population Oct collection consumed 27.6% of measured process CPU. That combination supports a narrowed, not universal, conclusion.

## 2. Research hypothesis

> **For a narrow durable transaction processor, manual memory management is not necessarily the primary performance determinant when a GC-managed implementation minimizes unnecessary dynamic work through semantic narrowing, batching, bounded execution, storage architecture, compact layout, and explicit ownership.**

The hypothesis is falsified for this workload if, after controlling semantics and batching as far as these systems permit, GC/allocation remains the dominant measured gap and the safe-Go control cannot approach TigerBeetle's performance class.

## 3. Systems under test

| System | Exact build | Runtime configuration |
|---|---|---|
| TigerBeetle | [0.17.9](https://github.com/tigerbeetle/tigerbeetle/releases/tag/0.17.9), commit `cc1c06a924e49b11089c521b2209d34c92caaf18`; official `x86_64-linux` ReleaseSafe binary; Zig 0.14.1; Go client v0.17.9 | one production-mode replica, Direct I/O enabled, `--memory=1GiB`, experimental `--limit-storage=10GiB`; no `--development` |
| OctetDB Oct | repository `3bfa3a7f3a71ec51a9cd3432f18c7b10c6e29f29`; Go 1.27.0-X:nodwarf5 | frozen M2 path; `GOMAXPROCS=16`; segmented semantic-delta WAL; group maximum equals offered burst |
| OctetDB direct Go | same repository, compiler, storage, commit, conflict, dedupe, and recovery code | `OpenGoM1Baseline`; only behavioral Step/control is handwritten Go |
| PostgreSQL | 18.6, GCC 16.2.1, data checksums enabled | native WSL process; `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, 1 GiB shared buffers |

Host: AMD Ryzen 7 7700X (8 cores/16 threads), 15 GiB WSL memory, Linux 6.6.87.2 WSL2, Arch Linux, native WSL ext4 (`/dev/sdf`), backed by a 2 TB SHPP41-2000GM host device. WSL exposes a virtual disk, so this is a same-host systems comparison, not a bare-metal NVMe characterization. The CPU governor was not exposed. TigerBeetle warned that its 2,932 MiB locked allocation exceeded `RLIMIT_MEMLOCK`; swap was configured but unused in the captured environment. See `experiments/TigerCompareM0/environment`.

## 4. Guarantee matrix

| Guarantee/capability | TigerBeetle lane | OctetDB lanes | PostgreSQL lane |
|---|---|---|---|
| durable acknowledgement | prepare durably stored by replication quorum before commit acknowledgement; quorum was one replica here | WAL group appended and `Sync` completed before apply/ack | transaction commit with synchronous commit and fsync |
| atomic transfer | yes, one immutable debit/credit transfer | yes, one two-account authoritative effect | yes, one SQL transaction |
| idempotency | cluster-wide immutable u128 transfer ID | exact string command ID within persisted 100k dedupe horizon | unique command row retained without bounded horizon in this run |
| crash recovery | VSR data-file restart; forced-kill tested | checksummed snapshot plus WAL tail; historical `Step` never rerun | PostgreSQL WAL recovery, not separately timed |
| corruption detection | checksums/hash chains; peer repair requires replication | WAL/snapshot checksums and fail-closed semantic compatibility | page checksums enabled plus WAL checks |
| replication | supported and normally central to durability; disabled by the one-replica comparison | absent | absent in this configuration |
| multi-process access | client protocol | no; in-process engine | yes |
| general querying | fixed account/transfer lookup and filters | canonical publication/account access only | SQL |
| workflow state | pending/posted/void transfer modes | compiled Oct FLOW state and semantic deltas | schema/application logic |
| schema flexibility | fixed ledger schema | fixed experiment schema, generated behavior | general relational schema |

The primary comparison is therefore **closest practical, not guarantee-identical**. It intentionally removes TigerBeetle replication so its local durable contract is closer to OctetDB, but this also removes TigerBeetle's normal high-availability and repair guarantee. Performance is not credited for guarantees absent from a lane.

## 5. TigerBeetle architecture reconstruction

This reconstruction uses the tagged [architecture document](https://github.com/tigerbeetle/tigerbeetle/blob/0.17.9/docs/ARCHITECTURE.md), [performance documentation](https://github.com/tigerbeetle/tigerbeetle/blob/0.17.9/docs/concepts/performance.md), [safety documentation](https://github.com/tigerbeetle/tigerbeetle/blob/0.17.9/docs/concepts/safety.md), and tagged source—not reputation.

1. At startup, each component computes an explicit worst-case object count from configuration and allocates it. The main event loop creates no new server objects; this is bounded runtime allocation, not `.bss`-only static storage and not an arena that may unpredictably exhaust.
2. The server still performs startup allocation. This run logged 2,932 MiB allocated despite the 1 GiB cache-budget flag; “no malloc/free in steady state” does not mean “small memory.”
3. Linux I/O is built around `io_uring`; production mode requires Direct I/O. Storage is one data file with a journal, hash-chained 512 KiB grid blocks, superblocks, and a specialized LSM forest.
4. The client-visible API sends homogeneous batches (documented maximum 8,190 transfers; setup used 8,000 to stay below framing limits). Internally, prepares, journal writes, LSM work, and checkpoints are batched at additional levels.
5. `Account` and `Transfer` are fixed 128-byte records. Transfers are immutable. Accounts hold cumulative pending/posted debit and credit fields, ledger/code, flags, user data, and timestamps.
6. The [safety contract](https://github.com/tigerbeetle/tigerbeetle/blob/0.17.9/docs/concepts/safety.md#replication) requires the operation in a quorum of replica WALs before acknowledgement. With one replica, quorum and local durable copy are both one.
7. Replication normally converts durability into availability and corruption repair. Adding replicas does not add state-machine throughput; the primary applies events in order.
8. The state machine is single-threaded by design, while networking, replication, prefetch, storage, and compaction are asynchronously pipelined.
9. Central layout choices are fixed records, cache-line-aware alignment, contiguous homogeneous batches, specialized fixed-value LSM trees, prefetching, radix sorting, and bounded working sets.
10. Per-transfer work includes identifier, account, ledger/code, flag, timestamp, pending-transfer, overflow, linked-event, and balance-constraint validation. The common lane enabled `debits_must_not_exceed_credits` for user accounts.
11. Comparable modes are durable single-phase unlinked `create_transfers`, existing accounts, one ledger/code, deterministic identifiers, and account lookups. There is no honest TigerBeetle memory-only acknowledgement mode, so none was fabricated.

## 6. OctetDB architecture freeze

The benchmark preserves M2 exactly: compiled Oct behavior; Go-owned accounts, ledger, registry, mailboxes, conflict ownership, ordered commit authority, semantic-delta WAL, bounded exact dedupe, checksummed snapshots, and snapshot-plus-tail recovery. It uses ordinary safe Go, the Go heap and GC, buffered filesystem I/O plus `Sync`, and no unsafe, cgo, arenas, custom allocator, mmap engine, or Direct I/O. The only addition is the external benchmark command; it invokes the existing `Submit` path and does not add an engine batch API.

## 7. Common semantic subset

### Account mapping

| OctetDB | TigerBeetle | Classification |
|---|---|---|
| uint64 account ID | u128 cluster-wide account ID | irrelevant at tested cardinality |
| signed Go `int` balance, status, version | u128 cumulative debits/credits plus immutable ledger/code/flags | TigerBeetle performs broader ledger bookkeeping; favorable to OctetDB performance |
| initial balance on create | balances must start at zero | setup funded each TigerBeetle user from an unconstrained funding account; setup excluded |
| frozen status exists | common lane used balance-limit flag only | Oct-only feature inactive; irrelevant |

### Transfer mapping

| Field | Mapping/difference | Classification |
|---|---|---|
| source/destination | Oct `Account`/`Other` ↔ TigerBeetle debit/credit account | equivalent for the common lane |
| amount | positive integer 1 | equivalent |
| identifier | exact string command ID ↔ exact u128 transfer ID | TigerBeetle ID persists with immutable history; Oct exact dedupe is bounded; favorable to OctetDB storage/performance |
| durability | local synced semantic WAL ↔ one-replica durable VSR prepare | closest practical, not replicated parity |
| failure | Oct accepted/reason/effect ↔ TigerBeetle per-transfer status | all measured workload operations accepted; rejection APIs differ but do not block the accepted lane |
| semantics | Oct also executes FLOW/decision/checkpoint logic; TigerBeetle stores a richer immutable transfer record and ledger checks | mixed; no claim of perfect semantic equality |

## 8. Harness/topology

TigerBeetle used the v0.17.9 Go client over loopback TCP to a separate server. Each offered batch was one real homogeneous `CreateTransfers` request. Oct and direct Go were in-process calls; an offered batch was a concurrent burst of individual `Submit` calls, with internal group commit capped at that burst. PostgreSQL used pgx over loopback; every logical transfer remained a separate SQL transaction even when the harness offered a burst.

Primary population was 1,000 accounts; amount was 1; initial user balance was 10^12. Every fresh run executed 1,000 excluded transfer warm-ups. Important configurations used three repetitions, fixed deterministic seeds, and rotated order: Oct→Go→Tiger, Go→Tiger→Oct, Tiger→Oct→Go. Tables report medians; min/max are retained in `summary.json`. The harness uses `GOMAXPROCS=16`; actual process CPU and operations per utilized core are reported. TigerBeetle's server state machine is single-threaded. PostgreSQL worker CPU was not aggregated by the single-PID sampler, so PostgreSQL per-core results are only an upper bound on efficiency.

## 9. Correctness

Every retained grouped result reports:

- total user-account value conserved;
- no rejected workload transfer;
- exact duplicate retry suppressed;
- deterministic final digest for that system/run;
- no partial transfer surfaced.

The forced-kill TigerBeetle restart accepted traffic and passed the same conservation/duplicate check. OctetDB's existing crash/corruption matrices and full repository tests remain authoritative for injected internal windows.

## 10. Memory-only diagnostics

TigerBeetle has no honest equivalent, so this comparison is Oct versus direct Go only.

| Lane, batch 128 | ops/s | reciprocal service time | p99 | alloc B/op | allocs/op | GC CPU / capacity | GC CPU / process CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| Oct FLOW | 328,790 | 3,041 ns | 1,631 µs | 5,415 | 26.1 | 4.88% | 18.7% |
| direct Go | 524,663 | 1,906 ns | 903 µs | 2,207 | 20.1 | 2.25% | 9.4% |

The p99 is batch-completion latency assigned to each logical command, not single-call service time. Direct Go is 59.6% faster in throughput; Oct adds about 1,135 ns in reciprocal throughput terms. This isolates a real Oct semantic/checkpoint representation cost, not a TigerBeetle/manual-memory result.

## 11. Durable single-operation results

Steady-state offered batch 1, one logical transfer per acknowledgement:

| Lane | ops/s median (min–max) | p50 | p95 | p99 | durable boundary |
|---|---:|---:|---:|---:|---|
| TigerBeetle | 221 (221–546) | 4.36 ms | 5.67 ms | 7.06 ms | one VSR prepare/request; quorum=1 |
| Oct | 1,008 (597–1,017) | 1.01 ms | 1.24 ms | 1.49 ms | one WAL `Sync` |
| direct Go | 925 (248–1,063) | 1.01 ms | 1.40 ms | 2.21 ms | one WAL `Sync` |

This lane strongly supports H2: TigerBeetle's architecture is designed to amortize work over batches; absence of GC does not rescue its low-batch fixed costs. Spread is large and reported rather than hidden.

## 12. Batch sweep

Medians; latency is logical-command p99 inherited from its offered batch completion.

| Batch | Tiger ops/s | Tiger p99 µs | Oct ops/s | Oct p99 µs | Go ops/s | Go p99 µs |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 221 | 7,059 | 1,008 | 1,488 | 925 | 2,209 |
| 4 | 903 | 7,357 | 3,730 | 1,387 | 3,648 | 1,477 |
| 8 | 6,707 | 2,201 | 6,586 | 1,781 | 6,721 | 1,591 |
| 16 | 13,373 | 2,047 | 13,105 | 2,177 | 13,446 | 2,054 |
| 32 | 20,432 | 2,743 | 23,113 | 2,868 | 24,253 | 2,212 |
| 64 | 39,729 | 3,048 | 35,899 | 4,121 | 41,105 | 2,510 |
| 128 | 75,284 | 4,845 | 48,480 | 5,405 | 64,715 | 4,514 |
| 256 | 139,784 | 7,134 | 47,906 | 7,504 | 78,459 | 5,854 |
| 512 | 236,325 | 12,294 | 65,024 | 12,007 | 74,636 | 10,760 |

At batch 64, direct Go was 3.5% faster than TigerBeetle and Oct was 9.6% slower; all are the same throughput class. At 512, TigerBeetle's true batch API and internal architecture continue scaling while Go plateaus. This is not evidence that allocation strategy alone caused the 3.17×/3.63× gap.

Offered and internal batching remain distinct. At offered 64, Oct averaged 61.2 and Go 62.7 commands per observed `Sync`. TigerBeetle's request contained 64 transfers; lower-level journal/LSM/checkpoint batching is not equated with Oct group commit.

## 13. Contention sweep

Batch 64, 5,000 measured transfers after warm-up:

| Shape | Tiger ops/s | Oct ops/s | Go ops/s | Tiger/Oct |
|---|---:|---:|---:|---:|
| independent | 38,260 | 37,823 | 39,564 | 1.01× |
| hot source | 40,125 | 466 | 473 | 86× |
| hot destination | 10,947 | 465 | 287 | 24× |
| 16-account hot set | 40,384 | 2,198 | 2,204 | 18× |

The Oct/Go collapse is common mechanism, not Oct semantic tax: per-command source-agent/mailbox/conflict ownership prevents filling groups under overlapping keys. TigerBeetle's sequential state machine and homogeneous batch keep the hot path batched. This is the strongest measured architectural gap and is unrelated to GC pauses.

## 14. Population scaling

Batch 64, 5,000 measured transfers:

| Lane | accounts | ops/s | p99 | total RSS | live heap |
|---|---:|---:|---:|---:|---:|
| TigerBeetle | 1k | 39,988 | 2.85 ms | 2.97 GB | client heap <1 MB |
| TigerBeetle | 100k | 29,520 | 30.3 ms | 3.00 GB | client heap <1 MB |
| Oct | 1k | 38,507 | 3.69 ms | 61 MB | 31 MB |
| Oct | 100k | 23,289 | 34.3 ms | 2.18 GB | 2.87 GB virtual/live accounting |
| direct Go | 1k | 40,868 | 3.77 ms | 50 MB | 29 MB |
| direct Go | 100k | 38,913 | 2.91 ms | 1.53 GB | 2.72 GB virtual/live accounting |

Oct's 100k population cost is the clearest FLOW/agent representation pressure. One million accounts was not forced: the current one-mailbox/agent architecture would exceed the 15 GiB WSL limit before yielding a meaningful database comparison. TigerBeetle's fixed startup allocation makes its RSS high at 1k but almost flat at 100k.

## 15. Storage footprint

Oct M2 rerun at 100k reproduced 486.65 B/op for semantic-delta WAL versus 1,461.30 B/op for the full-checkpoint control, and an 11.23 MB compact snapshot. At 1M, the current snapshot was 94.82 MB. Transfer-heavy primary records were 535 B/op for Oct and 497 B/op for direct Go because command/envelope content differs from the deposit storage ablation.

TigerBeetle intentionally preallocated a 1,141,374,976-byte data file. After 100k transfers, logical size was unchanged, so counting the whole file as 11 KB/transfer would be false. After 1M transfers, logical size was 2,031,620,096 bytes and allocated blocks grew by about 826 MB above the initial allocation, roughly 826 B/workload transfer, including 1k funding transfers and 1k warm-ups. Changed-sector write amplification was not observable.

The PostgreSQL 100k probe grew the database from 7.99 MB to 51.67 MB and advanced WAL by 113.26 MB; that WAL delta includes 1,000 account setup operations, so it is context, not an exact 1,132.6 B/transfer parity claim.

## 16. Recovery

| System/frontier | time to ready | bytes scanned/read | note |
|---|---:|---:|---|
| Oct 100k snapshot | 88.6 ms | 12.60 MB snapshot; zero WAL | semantic snapshot |
| Oct 100k + 10k tail | 322.7 ms | 12.60 MB + 4.87 MB WAL | 10k deltas replayed, no historical Step |
| Oct 1M snapshot | 501.5 ms | 94.82 MB snapshot | 100k dedupe horizon |
| Oct 1M + 10k tail | 751.3 ms | 94.82 MB + 4.93 MB WAL | 10k deltas replayed |
| TigerBeetle 100k forced kill | 634.6 ms | not exposed | process start through first accepted transfer and full verification |

These are architecturally different recovery mechanisms. TigerBeetle checks/reconstructs its VSR/LSM data file; Oct installs an application snapshot and bounded tail. The times do not rank durability quality.

## 17. CPU profiles

Oct memory-mode CPU was distributed across `Engine.process`, generated decision/Step work, checkpoint/delta work, JSON encoding, commit application, copying, and GC scanning. Direct Go removed generated Step/checkpoint work but retained the host/storage path. Durable profiles shifted cumulative work toward framing, JSON marshal, `bytes.Clone`, WAL append/group commit, and kernel sync wait.

The official TigerBeetle release is stripped; its release profile is retained but not falsely symbolized. A separate official debug build was used only to classify symbols, not for throughput. Its warmed 1M-transfer sample attributed major CPU to LSM compaction/merge, checksumming, radix sorting, value-block construction, and copy paths—an architecture, not “malloc-free magic.”

## 18. GC evidence

`GC CPU / capacity` is Go `/cpu/classes/gc/total` divided by total GOMAXPROCS capacity. `GC CPU / process` divides the same GC CPU by sampled process CPU and is useful on non-saturated runs, but it is not a direct throughput counterfactual.

| Mode | Alloc B/op | allocs/op | GC CPU / capacity | GC CPU / process | cycles | max pause | live heap | RSS | ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Oct memory | 5,415 | 26.1 | 4.88% | 18.7% | 14 | 453 µs | 54 MB | 118 MB | 328,790 |
| Go memory | 2,207 | 20.1 | 2.25% | 9.4% | 6 | 363 µs | 49 MB | 102 MB | 524,663 |
| Oct durable, b64 | 8,470 | 32.5 | 1.68% | 24.6% | 4 | 188 µs | 33 MB | 71 MB | 35,899 |
| Go durable, b64 | 5,086 | 26.5 | 0.39% | 9.1% | 3 | 126 µs | 32 MB | 56 MB | 41,105 |
| Oct 100k accounts, 500k ops | 8,529 | 32.3 | 1.23% | 27.6% | 1 | 113 µs | 2.96 GB | 5.25 GB | 15,868 |
| Go 100k accounts, 500k ops | 5,169 | 26.3 | 0.37% | 6.1% | 1 | 88 µs | 2.85 GB | 4.02 GB | 42,486 |

No pause approached the millisecond-scale durable p99, much less the 34–301 ms contention tails. Large-population scanning is nevertheless material CPU work for Oct and narrows H1.

### Counterfactual GC bound

In memory mode, deleting all Oct GC capacity consumption for free provides only a roughly 5% capacity bound; even the more generous process-CPU accounting cannot erase the measured 59.6% direct-Go throughput advantage by itself. In durable b64, GC used 1.68% of available Go capacity while TigerBeetle/Oct differed by 10%. At b512 TigerBeetle was 3.63× Oct, far beyond the measured GC share. At 100k accounts, however, Oct GC consumed 27.6% of process CPU during the one observed collection, so GC scanning can be an important contributor to population-scale CPU even though pauses remain tiny.

### GOGC sensitivity

| Lane | GOGC 50 | GOGC 100 | GOGC 200 | throughput range |
|---|---:|---:|---:|---:|
| Oct b64 | 35,613 | 38,044 | 38,019 ops/s | 6.8% |
| Go b64 | 40,861 | 41,557 | 41,288 ops/s | 1.7% |

Oct's lower GOGC costs throughput, but default and 200 are effectively tied. This is evidence against GC frequency being the primary durable gap; it does not say heap representation is free.

## 19. Allocation hotspots

Oct memory allocated 562.8 MB over 100k operations in the profile. Leading classified sites were:

| Category | Evidence |
|---|---|
| FLOW semantic representation | checkpoint byte copy 15.0%; utility selection 11.1%; checkpoint construction 6.2%; delta export/bytes also visible |
| host envelope/dedupe/store | `Engine.process`, `rememberResult`, `Store.apply`, registry entry and `Submit` allocations |
| WAL encoding | durable profile: `bytes.Clone` 21.7%, frame 11.0%, reflect/JSON record marshal, commit group |
| mailbox/conflict | goroutine/harness task, tokens and lock acquisition; smaller individually |
| Go runtime | allocator and GC scanning visible, but no single runtime allocator site dominates |

Direct Go memory allocated 256.2 MB/100k and removed the checkpoint/utility/delta categories. No baseline hotspot was optimized.

## 20. Layout/locality evidence

Linux user-mode counters over 100k operations (setup/warm-up are a small included fraction):

| Lane/mode | IPC | cache misses / 1k instructions | branch misses / 1k instructions |
|---|---:|---:|---:|
| TigerBeetle release | 2.03 | 5.28 | 0.16 |
| Oct memory | 1.53 | 11.13 | 1.73 |
| Go memory | 1.42 | 13.74 | 2.19 |
| Oct durable | 1.57 | 8.56 | 1.64 |
| Go durable | 1.65 | 8.81 | 1.66 |

TigerBeetle's lower miss/branch rates and higher IPC are consistent with fixed homogeneous batches and cache-conscious layout. These counters cannot identify manual allocation as the cause; layout, code shape, and semantic work move together.

## 21. Specialized Safe-Go ablation

Not implemented in M0. The rule was to establish baseline evidence first. Profiles identify a bounded future candidate—fixed-layout binary WAL batches plus safe buffer reuse—but adding it now would mix baseline comparison with optimization. No unsafe, arena, custom allocator, or manual memory lane was introduced.

## 22. Oct semantic abstraction tax

- Memory: Oct 328,790 vs Go 524,663 ops/s, a 37.3% throughput shortfall; reciprocal throughput adds approximately 1,135 ns/op.
- Durable batch 1: Oct and Go are within run spread; storage latency hides the semantic cost.
- Durable batch 64: Oct 35,899 vs Go 41,105 ops/s, a 12.7% shortfall; p99 was 4.12 vs 2.51 ms.
- Durable batch 512: Oct 65,024 vs Go 74,636 ops/s, a 12.9% shortfall.

This is separable from TigerBeetle. Generated behavior/checkpoint representation is expensive in CPU-only and large-population modes, but much of it disappears behind ordinary durability latency.

## 23. TigerBeetle gap decomposition

| Factor | Evidence-based role |
|---|---|
| semantic narrowing | both systems use a narrow transfer; TigerBeetle still performs richer fixed ledger validation/history |
| batching | dominant: Tiger 221 ops/s at b1, 236k at b512; Oct/Go plateau much earlier |
| I/O architecture | material but not isolated: Direct I/O/io_uring/VSR journal versus buffered Go writes and `Sync` |
| replication/protocol | loopback protocol costs TigerBeetle; replication disabled; cannot explain its high-batch lead |
| representation/layout | strong evidence: IPC/miss/branch counters and flat server RSS across 1k→100k |
| allocation/GC | secondary in ordinary durable runs; material Oct CPU at 100k accounts; pauses not a tail driver |
| scheduler/runtime | hot-key Oct/Go collapse is common mailbox/conflict/ack mechanism; Oct adds a smaller semantic tax |
| storage format | Tiger immutable transfers/indexes and preallocation versus Oct semantic WAL/snapshot; changed-write amplification unresolved |
| unknown | exact attribution among layout, VSR batching, LSM work, and protocol requires ablations; percentages would be invented |

## 24. Long-duration behavior

The 300.0-second Oct durable hot-set run completed 363,392 transfers at 1,211 ops/s. Aggregate p99 was 81.9 ms; interval throughput ranged 847–2,261 ops/s and maximum interval p99 was 94.5 ms. The lane ran 53 collections; max pause was 260 µs. GC intervals averaged 54.1 ms p99 versus 70.7 ms in non-GC intervals, so no GC-correlated tail spikes appeared.

Live heap ranged 29.9–83.7 MB and RSS 48.9–182.9 MB before stabilizing around the bounded dedupe/registry working set. WAL grew 190.2 MB. The major latency variance tracks hot-key serialization/storage boundaries, not stop-the-world pause time. WSL thermal/power telemetry was unavailable.

## 25. PostgreSQL context

At offered burst 64, PostgreSQL delivered 6,890 ops/s, p50 9.19 ms and p99 10.94 ms. The client consumed about 3.35 cores; worker-process CPU was not included, so `≤2,000 ops/s/captured-core` overstates total-system efficiency. This is the expected context result: a general SQL engine, multiple statements/locks/index updates per command, and one transaction commit per logical transfer are not a control for manual memory.

## 26. Manual-memory thesis verdict

**Partially supported / narrowed**

Support:

- Safe direct Go matched/exceeded TigerBeetle around batch 64, and Oct was within 10%, so a tracing-GC implementation can enter the same durable throughput class.
- TigerBeetle's batch-1 result was worse, while its batch-512 result was dramatically better. Batching and fixed costs explain far more movement than allocator choice.
- Durable GC capacity share was 0.39–1.68%; GOGC moved Go 1.7% and Oct 6.8%; pauses were sub-millisecond and not correlated with long-run p99 spikes.
- TigerBeetle hardware counters and debug symbols point to layout, branch shape, checksumming, sorting, compaction, and storage work—not a single allocation effect.

Narrowing evidence:

- At batch 512 TigerBeetle retained a 3.17× direct-Go and 3.63× Oct advantage.
- At 100k accounts, Go's per-agent/mailbox/map representation required 1.5–2.2 GB RSS in short runs and 4.0–5.25 GB in the 500k-operation GC probe; TigerBeetle stayed near its fixed 3 GB allocation.
- The large-population Oct collection consumed 27.6% of process CPU, and the experiment did not perform a safe-Go fixed-layout ablation capable of separating layout control from tracing/ownership overhead.

Therefore “GC is the main reason TigerBeetle is fast” is not supported, but “manual/static memory and the layout discipline it enables never matter” is also not supported.

## 27. What this does NOT prove

This experiment does not generalize to HPC, kernels, hard real-time, embedded systems, GPUs, general databases, replicated production clusters, distributed databases, multi-ledger applications, long histories beyond the measured scale, or bare-metal NVMe. It does not show that one-replica TigerBeetle has the safety of its recommended replicated topology. It does not establish causal percentages for manual memory versus layout.

## 28. Architecture lessons

Transferable to safe Go: homogeneous fixed-layout command batches, presized bounded slices, explicit batch APIs, denser account/agent indexing, safe buffer reuse, binary WAL framing, single-owner hot paths, and fewer pointer-indirect objects. Likely tied more closely to full memory/layout control: guaranteed no steady-state allocation, exact cache-line/padding contracts, bounded resumable I/O contexts, and whole-server worst-case memory proofs. Direct I/O/io_uring and deterministic LSM scheduling are storage/execution choices, not allocator choices.

## 29. Oct pressure findings

The Oct layer's main pressure is repeated full-checkpoint materialization/copy around a compact semantic delta, plus utility/reflect/JSON work. The larger mechanism pressure is one mailbox/channel/goroutine-shaped registry entry per account and per-command acknowledgement fan-out under contention. The 100k population and hot-key curves are more consequential than GC pause time.

## 30. Remaining limitations

- WSL2 virtualizes storage and did not expose governor/thermal telemetry; TigerBeetle could not lock all pages.
- TigerBeetle was deliberately one replica. A production replicated comparison remains a different experiment.
- Offered batching APIs differ: true TigerBeetle batch versus concurrent Oct/Go burst; PostgreSQL had no multi-transfer transaction batch.
- No Oct loopback service control was added, so in-process topology favors Oct/Go.
- PostgreSQL worker CPU was not aggregated.
- TigerBeetle changed-sector write amplification and internal effective group sizes were not observable.
- One million accounts was unsafe under the 15 GiB limit; 100k plus a longer allocation/GC probe isolated the limitation instead.
- The debug TigerBeetle profile classifies symbols but is not a performance result.
- No architecture-transfer ablation was run, so the final causal split between layout and memory-management discipline remains bounded rather than isolated.

## 31. Exactly one next recommendation

**Run one bounded safe-Go layout-specialization experiment:** add a separate C2 lane with a fixed-layout binary command/WAL batch, presized dense account index, and ordinary safe-Go buffer reuse, while retaining GC and all M2 semantics. Measure only batch 64/512, hot source, and 100k population against this frozen baseline. This directly tests whether the measured TigerBeetle gap transfers through architecture without manual memory management.

## Required primary table

Closest practical durable configuration: offered batch 64, 1k accounts, independent transfers, one replica/local durability.

| Metric | TigerBeetle | OctetDB Oct | OctetDB Go | PostgreSQL |
|---|---:|---:|---:|---:|
| operations/s | 39,729 | 35,899 | 41,105 | 6,890 |
| ops/s/utilized core | 182,857 | 41,290 | 60,952 | ≤2,000 (worker CPU omitted) |
| p50 | 1.54 ms | 1.67 ms | 1.47 ms | 9.19 ms |
| p95 | 1.78 ms | 2.31 ms | 2.07 ms | 10.94 ms |
| p99 | 3.05 ms | 4.12 ms | 2.51 ms | 10.94 ms |
| CPU | 0.22 cores client+server | 0.87 cores | 0.67 cores | ≥3.35 client cores |
| RSS | 2.98 GB client+server | 70.7 MB | 55.8 MB | 78.8 MB captured processes |
| Go GC CPU | n/a server; client 0% | 1.68% capacity | 0.39% capacity | 0.55% client capacity |
| bytes/logical op | unavailable under preallocation | 535 WAL B/op | 497 WAL B/op | unavailable in primary; storage probe separate |
| durability | one-replica VSR quorum, Direct I/O | local grouped WAL Sync, 61.2 cmd/sync | same, 62.7 cmd/sync | one synchronous SQL commit/op |

## Required architecture table

| Technique | TigerBeetle | OctetDB | Likely impact |
|---|---|---|---|
| narrow command schema | fixed account/transfer API | fixed commands + compiled behavior | high |
| batching | true client batch and pervasive internal batching | harness burst + bounded group commit | dominant in curve |
| bounded capacity | whole-server/component bounds | mailbox, dedupe, group bounds; registry population not compact | high for stability |
| static/precomputed topology | compile-time/state-machine specialization | generated FLOW + typed conflict tokens | medium/high |
| explicit conflict ownership | sequential state machine | lock tokens + source-agent owner | high; adverse under hot keys |
| contiguous/fixed layout | 128-byte records, homogeneous arrays, fixed-value LSMs | maps, channels, pointers, JSON envelopes | high; counters support |
| manual/static memory | bounded startup allocation, no steady-state object creation | ordinary Go heap | small ordinary durable GC share; material large-pop scan |
| GC | none server-side | enabled | not a pause driver; some CPU/population cost |
| direct/storage I/O ownership | io_uring + Direct I/O + one data file | buffered file writes + Sync | material, not isolated |
| general query engine absent | yes | yes | removes general-purpose overhead |
| replication | VSR available; one replica measured | none | production guarantee difference, not measured throughput benefit |

Authoritative normalized values, min/max spreads, raw file names, long-run samples, profiles, and counters are in `experiments/TigerCompareM0/summary.json` and its sibling evidence directories.
