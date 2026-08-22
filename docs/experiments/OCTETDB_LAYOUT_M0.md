# OCTETDB-LAYOUT-M0 — Bounded Safe-Go Layout Specialization

## 1. Verdict

**Success**

A real `C2 — Specialized Safe-Go` lane now exists beside, rather than inside, the frozen OctetDB M2 lanes. It uses ordinary safe Go, keeps the Go GC enabled, retains arbitrary external account IDs, exact bounded idempotency, per-agent behavioral state, accepted-effect ledger history, version/status/balance behavior, durable acknowledgement, checksummed recovery, and deterministic final state. It introduces no `unsafe`, cgo, arena, allocator, mmap ownership, Direct I/O, `io_uring`, new Oct feature, or production-format migration.

On the final rotated three-run Linux matrix, C2 was 1.51× direct Go at batch 64, 5.04× at batch 512, 131× on hot source, and 1.53× at 100k accounts. C2 was also 1.59–1.61× the one-replica TigerBeetle control at batch 64/512 and hot source, and 1.93× at 100k accounts. This does **not** make C2 a generally superior database: C2 is in-process, single-owner, benchmark-bounded, and has a narrower guarantee surface than TigerBeetle. It does answer the experiment: safe layout and batch specialization recover all of the previously measured narrow-workload throughput gap without disabling GC.

## 2. Research question

> **How much of TigerBeetle's measured advantage can be recovered through layout, batch representation, and safe buffer reuse while retaining ordinary safe Go and the Go GC?**

The tested hypothesis was: if the batch-512, hot-source, and population gaps are primarily consequences of representation and lost batch identity rather than a necessary consequence of tracing GC, then a bounded safe-Go lane with dense state, contiguous commands, fixed-layout WAL batches, and explicit reusable buffers will materially outscale the direct-Go baseline while preserving the same accepted transfer, idempotency, durability, recovery, ledger, and final-state semantics.

## 3. Frozen controls

| Item | Frozen identity/configuration |
|---|---|
| Experiment base | OctetDB repository `7e7db384850ef48d818b1406005340f8452a2dcd` plus this uncommitted C2 lane |
| TIGER-COMPARE-M0 evidence revision | OctetDB `3bfa3a7f3a71ec51a9cd3432f18c7b10c6e29f29` |
| A — TigerBeetle | 0.17.9, commit `cc1c06a924e49b11089c521b2209d34c92caaf18`, official `x86_64-linux` ReleaseSafe, one production-mode replica, Direct I/O, 1 GiB cache budget, 10 GiB storage limit |
| B — OctetDB Oct | frozen M2 `Open`, compiled Oct behavior, segmented semantic-delta WAL, exact 100k-or-larger configured dedupe horizon |
| C — OctetDB direct Go | frozen `OpenGoM1Baseline`; same M2 scheduler, storage, conflict, dedupe, snapshot, and recovery mechanism as B |
| C2 — Specialized Safe-Go | separate `OpenSpecialized`; experimental binary C2 WAL/snapshot; GC on |
| Host | same AMD Ryzen 7 7700X, 16 logical CPUs, 15 GiB WSL2 Linux host, Linux 6.6.87.2, native ext4 `/dev/sdf`, Go `go1.27.0-X:nodwarf5` |

The final primary subset reran A/B/C/C2 in rotated order over three repetitions. The workload generator, initial balance, seeds per repetition, correctness check, warm-up, one-replica TigerBeetle settings, and local durable acknowledgement rules were retained. PostgreSQL was not rerun. The earlier b64/b512 controls were reproducible within ordinary host spread: final medians were Tiger 38.6k/228.2k, Oct 33.7k/66.0k, and Go 40.7k/72.3k ops/s.

Copeland source was inspected at `2b404befdd0aa29bbcacd3dda693f2d6fb2970a6`; Oct source was inspected at `1ee679c6d6967e9c56f98334e4e81e0420722b58`.

## 4. Baseline pressure

TIGER-COMPARE-M0 showed that direct Go already matched TigerBeetle near batch 64 but plateaued near 75k ops/s while TigerBeetle reached about 236k at batch 512. It also showed:

- map-backed accounts plus a map of pointer-valued agent entries;
- one allocated channel/mailbox and synchronization object graph per resident agent;
- per-command envelopes, response channels, goroutines, token slices, token sorting, and pointer-valued commit requests;
- offered bursts decomposed into independent `Submit` operations before the commit authority tried to regroup them;
- JSON/reflection record framing, `bytes.Clone`, and repeated frame allocation in the WAL path;
- about 1.5 GiB direct-Go RSS and 2.2 GiB Oct RSS at 100k accounts in the short population run;
- a common Oct/direct-Go collapse to roughly 470 ops/s for hot-source traffic;
- durable direct-Go allocation around 5 KiB and 26 allocations per operation;
- higher TigerBeetle IPC and lower cache/branch misses per instruction.

GC pauses were sub-millisecond and ordinary durable GC capacity was small. The pressure was therefore the shape and amount of managed state, not merely the existence of a tracing collector.

## 5. Candidate selection

The existing profiles and source mechanisms were inspected before implementation.

| Area inspected | Measured/mechanistic pressure | C2 hypothesis |
|---|---|---|
| account/state representation | `map[AccountID]Account`; 100k residency and lookup pressure | arbitrary IDs can resolve once to stable dense slots holding contiguous `[]Account` records |
| agent registry | `map[AccountID]*agentEntry`; pointer, mailbox, mutex, checkpoint per agent | direct-Go behavioral state can be a compact slice parallel to account slots |
| mailbox representation | channel and goroutine per started agent | one C2 batch owner can preserve serial conflict semantics without a persistent mailbox graph |
| command envelopes | command, envelope, queue item, response channel per operation | keep the offered `[]Command` contiguous and prepare `[]c2Record` in order |
| conflict tokens | allocated/sorted token slices and map-backed locks | a single ordered batch owner serializes overlapping keys without weakening the result order |
| commit groups | pointer slice of individually prepared commits | one contiguous record slice can be encoded and synced as one unit |
| WAL/framing | JSON/reflection, cloning, per-record frames; dominant durable allocation | a versioned safe binary batch can remove reflection, temporary objects, and repeated copies |
| dedupe | exact string map and order slice are semantically required | retain the exact map/horizon, but presize its known bound and avoid extra per-command wrappers |
| result/ack | per-command channels and `outcome` objects | return one owned `[]Result` for the offered batch |

No structure was changed merely because TigerBeetle uses a different one.

## 6. C2 architecture

C2 is contained in `internal/m7write/c2.go` and selected explicitly by `-lane c2`.

| Concern | Direct-Go baseline | C2 |
|---|---|---|
| external account key | direct map to value | arbitrary `AccountID` → stable `uint32` slot map |
| authoritative accounts | `map[AccountID]Account` | contiguous `[]Account` plus presence bits |
| behavioral state | pointer-valued agent entry with mailbox/mutex/checkpoint JSON | parallel `[]c2AgentState` containing pending transfer and turn count |
| ledger | growing `[]LedgerEntry` | same accepted-effect ledger semantics, presized from the declared record bound |
| submission | concurrent per-command `Submit` calls | one `SubmitBatch([]Command) []Result` call |
| overlap | token locks and mailbox ownership | deterministic input-order serialization beneath one engine mutex |
| in-batch state | authoritative map reads after independent ownership | generation-marked reusable account scratch; no pre-durability authoritative mutation |
| preparation | heap graph of requests/acks | reusable contiguous `[]c2Record` and batch-ID map |
| WAL | one JSON frame per record | one C2 version-1 binary frame per offered batch, one CRC, one `Sync` |
| WAL buffer | allocated frames and clones | engine-owned reusable `[]byte` |
| dedupe | exact bounded map/order | same exact bounded semantics, presized to the configured horizon |
| snapshot | frozen M2 JSON snapshot | separate deterministic C2 binary snapshot; no production migration |

The mapping layer means C2 does not assume contiguous production IDs. Tests deliberately use IDs `9001` and `42`. Account and agent slots share stable row identity but authoritative application records remain separate from behavioral pending/turn state.

The batch API is a narrowly scoped alternate mechanism lane. Overlapping commands are not declared independent: each sees prior accepted commands in the same input batch, then the whole prepared group is durably framed and synced before authoritative apply and acknowledgement. A write failure poisons the engine.

## 7. Correctness

Every retained primary result reported value conservation, zero workload rejection, and exact duplicate suppression. Oct, direct Go, and C2 produced the same final balance digest in every repetition of all four workloads.

Automated tests additionally verify:

- arbitrary-ID dense mapping;
- two same-source transfers in one batch observe serial versions and balances;
- duplicate identity inside a batch is applied once and returns the original sequence;
- accepted creates and transfers retain the same ledger projection count;
- snapshot plus C2 tail restores balances, versions, ledger, behavioral turn state, and dedupe;
- a corrupted frame fails closed;
- an incomplete final frame is truncated and never applied;
- durable history is applied from effects and never reruns historical Oct `Step`.

The full repository test suite passes.

## 8. Batch 64

Independent transfers, 1,000 accounts, 12,800 measured operations, 1,000 excluded warm-ups; medians of three final rotated repetitions:

| Lane | ops/s | range | p99 |
|---|---:|---:|---:|
| TigerBeetle | 38,609 | 35,022–38,794 | 3.33 ms |
| Oct | 33,728 | 33,470–37,398 | 4.27 ms |
| direct Go | 40,658 | 38,780–41,229 | 2.91 ms |
| C2 | 61,245 | 58,120–65,252 | 1.36 ms |

C2 was 50.6% faster than direct Go and 58.6% faster than TigerBeetle. This region was already competitive in M0; the gain mostly removes per-command mechanism work rather than amortizing more storage latency.

## 9. Batch 512

Independent transfers, 1,000 accounts, 102,400 measured operations:

| Lane | ops/s | range | p99 |
|---|---:|---:|---:|
| TigerBeetle | 228,196 | 196,489–230,464 | 12.56 ms |
| Oct | 66,013 | 63,424–66,362 | 11.67 ms |
| direct Go | 72,273 | 70,864–76,464 | 9.38 ms |
| C2 | 364,012 | 352,136–366,251 | 1.78 ms |

C2 was 5.04× direct Go and 1.60× TigerBeetle. C2 continued scaling rather than plateauing: its medians at batch 64/128/256/512 were 61.2k, 117.8k, 218.1k, and 364.0k ops/s in the three-run C2 sweep.

This result is not an allocator-only effect. It combines a true batch API, fewer dynamic objects, single ownership, dense state, and one fixed-layout durable frame. It demonstrates that the earlier plateau was not inherent to safe Go or the Go GC.

## 10. Hot source

Batch 64, 1,000 accounts, 5,000 measured operations:

| Lane | ops/s | p99 |
|---|---:|---:|
| TigerBeetle | 38,085 | 2.79 ms |
| Oct | 463 | 168.72 ms |
| direct Go | 468 | 157.25 ms |
| C2 | 61,364 | 1.54 ms |

C2 was 131× direct Go. It did not weaken conflicts: all commands sharing the source were executed serially in input order and each observed the previous version. The improvement occurs because serial ownership is held for the contiguous batch and followed by one durable group, instead of making every command traverse a mailbox/token/ack path that prevents group formation. This is the limited behavioral consequence that naturally falls out of preserving the batch; it is not a general batched-agent-turn design for the Oct lane.

## 11. 100k population

Batch 64, 100,000 accounts, 5,000 measured operations:

| Lane | ops/s | p99 | total RSS | live heap |
|---|---:|---:|---:|---:|
| TigerBeetle | 29,002 | 33.17 ms | 3.14 GB | Go client 0.44 MB |
| Oct | 24,517 | 18.23 ms | 2.35 GB | 3.01 GB |
| direct Go | 36,639 | 3.80 ms | 1.55 GB | 2.85 GB |
| C2 | 55,929 | 2.48 ms | 55.3 MB | 33.9 MB |

C2 improved throughput 52.6% over direct Go while reducing RSS 96.4% and reported live heap 98.8%. Approximate observed values were 15,474 RSS bytes/account and 28,495 live-heap bytes/account for direct Go versus 553 and 339 for C2. C2's exact slice payload includes 32 bytes/account for `Account`, about 32 bytes/account for compact behavioral state, and bounded parallel presence/scratch metadata; Go map bucket overhead and retained ledger/dedupe storage make total heap larger. Objects/account was not directly measured and is not invented.

The five-run isolated lookup probe used arbitrary non-contiguous IDs. Median map-to-value lookup was 10.35 ns; map-to-slot plus dense-slice lookup was 8.81 ns, a 14.9% reduction. The much larger population gain is therefore residency/scan/object-shape reduction, not lookup latency alone.

One million accounts was not forced. The 100k result already isolates the representation pressure safely.

## 12. Allocation/GC

Go GC remained enabled with default settings.

| Workload | Lane | alloc B/op | allocs/op | GC CPU/capacity | GC CPU/process | cycles | max pause | live heap | RSS |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| b64 | Go | 5,078 | 26.48 | 0.41% | 9.11% | 3 | 172 µs | 33.4 MB | 57.7 MB |
| b64 | C2 | 247 | 2.05 | 0% | 0% | 0 | 0 | 14.8 MB | 27.1 MB |
| b512 | Go | 4,991 | 26.14 | 0.49% | 6.73% | 5 | 193 µs | 131.5 MB | 174.9 MB |
| b512 | C2 | 256 | 2.01 | 0.11% | 3.58% | 1 | 59 µs | 22.5 MB | 51.5 MB |
| 100k | Go | 4,389 | 26.50 | 0% | 0% | 0 | 0 | 2.85 GB | 1.55 GB |
| 100k | C2 | 661 | 2.07 | 0% | 0% | 0 | 0 | 33.9 MB | 55.3 MB |

The short 100k measured phase did not trigger a collection in either lane, so it cannot estimate steady large-population GC CPU. It does measure the resident shape. C2 reduced b64 allocation bytes by 95.1% and allocation count by 92.2%; b512 reductions were 94.9% and 92.3%. GC did not disappear, but it had much less graph to trace.

## 13. Hardware counters

Linux user-mode `perf stat` counters; C2/Go cover the in-process harness and engine, while TigerBeetle counters attach to the server process only. Setup and warm-up are a small included fraction, as in M0.

| Batch | Lane | IPC | cache misses/1k instructions | branch misses/1k instructions |
|---:|---|---:|---:|---:|
| 64 | TigerBeetle | 1.82 | 9.90 | 0.40 |
| 64 | direct Go | 1.54 | 10.55 | 2.09 |
| 64 | C2 | 1.06 | 13.32 | 1.27 |
| 512 | TigerBeetle | 1.74 | 5.28 | 0.16 |
| 512 | direct Go | 1.31 | 7.65 | 1.54 |
| 512 | C2 | 1.35 | 4.31 | 0.50 |

At b512 C2 closed and slightly exceeded the cache-miss gap, improved IPC modestly over direct Go, and cut branch misses by two thirds, while retaining GC. At b64 its miss rate and IPC were worse despite higher throughput; C2 executed far less total mechanism work, so per-instruction locality alone does not explain the result. C2 did not match TigerBeetle's branch rate or b512 IPC. The evidence supports layout/batch shape as causal but does not establish that all TigerBeetle locality comes from transferrable safe-Go representation.

## 14. CPU profiles

The C2 b64/b512 CPU profiles are short because the lane completes 200k operations in well under a second, so percentages are classification evidence rather than precise accounting. At b512, sampled cumulative work was concentrated in `SubmitBatch`, string-map dedupe, `applyDurable`, WAL write/`Sync`, and runtime map operations. JSON/reflection, `bytes.Clone`, channel receive/send, token sorting, mailbox dispatch, and pointer-valued commit authority sites from the frozen direct-Go profile disappeared.

| Category | C2 classification |
|---|---|
| command preparation | contiguous command/result construction; harness `fmt.Sprintf` remains visible |
| account lookup | map-to-slot then slice access; inlined inside `SubmitBatch` |
| agent lookup | same slot indexes a compact parallel slice |
| conflict ownership | one engine mutex; no token slice or per-key lock allocation |
| behavior | `decideGo` inlined; same direct-Go decisions/pending semantics |
| WAL encoding | fixed append into reusable buffer; no reflection |
| copying | `memmove` visible in earlier profile; no `bytes.Clone` hotspot |
| checksum | CRC did not register as a sampled hotspot |
| sync/group commit | file write and `Fsync` remain material |
| GC | one b512 primary cycle; scanning appears only lightly |
| runtime scheduler | far less channel/goroutine work; futex/syscall overhead remains |

The b512 allocation profile attributes most space to startup prepayment (`OpenSpecialized`), then owned command/result slices and unavoidable exact string identities. This is the intended shift from repeated runtime work to bounded startup allocation.

## 15. WAL/layout results

| Metric | direct-Go M2 | C2 |
|---|---:|---:|
| b64 WAL bytes/op | 498.34 | 141.19 |
| b512 WAL bytes/op | 502.02 | 141.02 |
| b64 temporary allocation/op | 5,078 B | 247 B total lane allocation |
| b512 temporary allocation/op | 4,991 B | 256 B total lane allocation |

C2 reduced durable bytes/op by about 71.7%. It did not omit the command ID, result, expected versions, resulting balances/status, agent pending state/turn count, or accepted-effect ledger semantics.

The isolated component probe measured median JSON `logRecord` encoding at 1,754 ns, 2,114 B, and 6 allocations versus safe C2 record append into an owned reusable slice at 14.2 ns, 0 B, and 0 allocations. This probe intentionally measures record encoding only; it excludes CRC, filesystem writes, and sync, so its 123× ratio is not an end-to-end throughput claim.

The C2 snapshot after 1,000 creates and 100,000 transfers was 9.63 MB and took 46.6 ms to encode, SHA-256 frame, write, sync, atomically install, and directory-sync. Frozen M2 context for 100k commits was 11.23–12.60 MB and about 102–108 ms snapshot pause, but its Oct FLOW checkpoint/publication payload differs, so this is context rather than a direct-Go-only format ranking.

## 16. Recovery

The measured C2 recovery probe installed a 9,633,656-byte snapshot at sequence 101,000, appended a 10,000-record/1,330,264-byte tail, closed, and reopened:

| Metric | C2 result |
|---|---:|
| snapshot decode | 23.7 ms |
| WAL scan | 1.52 ms |
| records replayed | 10,000 |
| total ready (engine metric) | 32.20 ms |
| independently measured open wall time | 32.20 ms |

The recovered engine conserved value and suppressed an identifier from the tail. Tests separately prove corruption rejection and incomplete-tail handling. C2 recovery applies encoded decisions/effects and never invokes historical Oct `Step`. Frozen M2 Oct context for a 100k snapshot plus 10k tail was 322.7 ms, including FLOW-delta reconstruction; the semantics and formats differ enough that this is not a direct causal speedup claim.

## 17. Per-change attribution

The milestone is one architecture-transfer pass, so end-to-end changes are coupled. The table distinguishes isolated component evidence from joint lane effects instead of inventing additive percentages.

| Change | Baseline problem | Throughput effect | Allocation effect | Locality effect | Keep? |
|---|---|---:|---:|---:|---|
| arbitrary ID → dense account slot | map value access and scattered authoritative records | isolated lookup 10.35→8.81 ns (-14.9%); 100k endpoint +52.6% jointly | contributes to 98.8% lower 100k live heap jointly | contiguous account payload; b512 cache misses 7.65→4.31/1k jointly | yes |
| dense behavioral agent slots; no persistent mailbox graph | pointer-valued registry, channel/mutex/goroutine residency | 100k endpoint +52.6% jointly | 100k RSS 1.55 GB→55.3 MB jointly | removes agent pointer chase and GC scan graph | yes |
| contiguous batch + single ordered owner | burst loses identity; hot key prevents commit groups | hot source 468→61,364 ops/s (131×) | hot allocations 30.17→2.07/op jointly | removes token sort, per-command channel and scheduler path | yes, C2 only |
| fixed-layout binary WAL + reusable encode buffer | JSON/reflection, clones, per-record frames | b512 endpoint 72.3k→364.0k jointly; encoder micro 1,754→14.2 ns | encoder micro 2,114 B/6 alloc→0/0; WAL -71.7% bytes/op | one contiguous frame; no reflection graph | yes, experimental format only |
| declared capacity presizing + reusable record/scratch slices | ledger/dedupe/scratch growth and repeated copying | included in final endpoint; not independently timed | b512 total 4,991→256 B/op jointly; startup allocation becomes profile leader | bounded contiguous working sets | yes |
| batch result slice instead of per-command ack channels | one channel/outcome per call | contributes to b64/hot gains; not independently timed | removes per-command channel/outcome graph | less scheduler/futex traffic | yes |

The initial implementation briefly omitted the resident ledger and was rejected before reporting. All final measurements include ledger history. No rejected shortcut evidence appears in the result tables.

## 18. TigerBeetle gap after C2

There is no remaining measured throughput gap in TigerBeetle's favor for this bounded single-replica workload: C2 was about 1.6× TigerBeetle at b64, b512, and hot source, and 1.93× at 100k. What remains is a substantial architecture and guarantee gap:

- TigerBeetle is a networked database with VSR, replication support, repair, Direct I/O, `io_uring`, a specialized LSM, immutable transfer indexing, and bounded whole-server memory control.
- C2 is an in-process experimental lane with a single owner, local file `Sync`, no replication, no repair, and benchmark-supplied bounds.
- TigerBeetle still has better b512 IPC and branch misses/instruction.
- C2's arbitrary-ID and exact-string dedupe maps remain dynamic Go maps.
- C2 has not been subjected to long-duration compaction, replication, multi-client, or general workload tests.

The recovered throughput is therefore evidence about the old OctetDB gap, not a claim of product equivalence.

## 19. Manual-memory thesis update

**weakened**

The thesis that TigerBeetle's measured advantage on this workload materially requires manual whole-program memory ownership is weakened. C2 retains Go's tracing GC and ordinary safe slices/maps yet exceeds the measured TigerBeetle throughput after changing representation, preserving batches, and prepaying known capacities. At b512 it also closes the cache-miss gap while GC remains active.

The broader value of manual/static control is not falsified. TigerBeetle still provides bounded allocation proofs, exact alignment/control, asynchronous storage contexts, and stronger operational guarantees that C2 does not attempt.

## 20. Layout ownership study

| Model | Strengths | Risks | Evidence-based assessment |
|---|---|---|---|
| Go-owned layout | simplest runtime implementation; backend can freely select slices/maps/AoS/SoA | Oct compiler cannot preserve or exploit bounds, key identity, batch shape, or access families; semantic/physical drift is invisible | too little compiler visibility for facts that directly enabled C2 |
| Oct-owned physical layout | exact programmer control over order, packing, alignment, AoS/SoA, capacity, indexes | turns Oct into a hardware/manual-layout language; harms portability; exposes choices C2 did not need | unjustified; C2 succeeded without byte offsets, padding, alignment, or allocator syntax |
| Oct semantic intent + Go realization | compiler retains meaning and derivable constraints; Go chooses safe physical structures; portable across backends | requires a well-defined layout contract/IR and diagnostics when hints cannot be honored | best boundary: C2's useful inputs were semantic bounds, keys, identity, batch shape, and access family, while the successful realization was backend-specific safe Go |

## 21. Copeland `layout table` findings

The local Copeland source was inspected, not inferred from the phrase.

Two related facilities exist:

1. The exact `layout table CustomerTable` profile is reserved by the parser/design but not implemented in M0; `docs/Copeland/machina-layout.md` explicitly says parsing leaves room for a future profile.
2. The implemented CSV-shaped stream layout table (`csv overlay ...`) is parsed in `Copeland.TS/Syntax/Parser.cs` and bound in `Copeland.TS/Machina/LayoutDataCompiler.cs`. It requires semantic row names plus content/width/height and either x/y or derivations; optional layer/z facts are validated. It lowers each row into the same `BoundLayoutNode`/slot universe as ordinary layout authoring. Its enclosing stream/layout has a required typed origin. Separately, compiler-projected `layout::Layouts`, `layout::Boxes`, and `layout::Derivations` preserve stable IDs, authored order, field sets, resolution status, and source provenance as read-only relations.

Answers to S6:

1. It preserves semantic slot/row identity, declaration order, content binding, layer identity, derivation relationships, origin context, and source provenance.
2. It prescribes spatial geometry (x/y, width/height), overlay container shape, optional z/layer, and derivation constraints. It does not prescribe memory packing, pointer layout, allocator, or cache lines.
3. Stable identity, origin/provenance, bounded finite rows, typed columns, relationships, and compiler-visible access constraints remain useful for Oct.
4. AoS/SoA, indexes' concrete data structures, byte layout, padding, alignment, buffers, and allocation should be compiler/backend decisions.
5. Yes. Copeland demonstrates that specialized layout data can lower into the existing record/tree/table concepts. An eventual Oct declaration should reuse Oct records, record tables, Concepts, and Read/Write separation with layout-contract metadata, not create a separate type universe.

Copeland is spatial-layout precedent, not direct proof that its syntax should be copied for memory layout.

## 22. Layout ownership recommendation

**Declare semantic layout intent in Oct and lower/materialize it in Go.**

The declaration should carry stable semantic facts and constraints into compiler IR; the Go backend should select and construct the safe physical representation. C2 supports this because its largest gains depended on facts knowable above raw Go—bounded cardinality, stable keyed row identity, fixed command shape, homogeneous batch element type, exact horizon, and hot access family—while none depended on exposing offsets, packing, alignment, allocator selection, or pointer arithmetic.

This is a design recommendation only. No syntax or Oct feature is implemented here.

## 23. Semantic/backend boundary

| Layout fact | Boundary | Reason |
|---|---|---|
| row identity | semantic IR | observable identity and stable references |
| origin/provenance | semantic IR | required for derivation, diagnostics, publication, and recovery compatibility |
| bounded capacity | semantic IR when it changes admission/resource contract; otherwise derived startup hint | a hard bound is observable, while spare capacity is not |
| key uniqueness | semantic IR | correctness constraint |
| lookup key | semantic IR | relation/access meaning and index eligibility |
| iteration order | semantic IR only when observable; otherwise optimizer freedom | must not accidentally change semantics |
| column hotness/access family | optimization metadata in IR | useful across backends but not ordinarily a correctness promise |
| AoS vs SoA | backend-owned | machine/workload choice unless an external ABI explicitly makes it semantic |
| field byte order | backend/storage-format contract | persistent compatibility matters, but source programs should not choose it by default |
| padding | backend-owned | machine detail |
| alignment | backend-owned | machine/ABI detail |
| cache-line placement | backend-owned | hardware-specific and unstable |
| buffer ownership | backend/runtime-owned, checked against semantic lifetime | implementation mechanism |
| allocator | backend/runtime-owned | C2 required no allocator choice |

Semantic facts should survive as compiler IR when changing them can alter identity, admissible values, observable ordering, provenance, or recovery compatibility. Machine choices should remain backend-owned when multiple realizations preserve those facts.

## 24. Compiler-prepayment opportunities

> Anything knowable before execution should justify why it remains runtime work.

| Time | Derivable facts/work |
|---|---|
| compile/publication | fixed record schema; command variants and fixed fields; batch element type; key type and uniqueness; stable row identity; known index topology; hot access family; immutable/published relations; generated binary encoders/decoders; compatibility IDs |
| startup | configured dedupe horizon; actual bounded account/record capacity; external arbitrary-ID → dense-slot map construction; slice capacities; reusable buffer sizes; validation that published constraints fit runtime configuration |
| runtime only | arriving command ID and dedupe membership; actual arbitrary external keys not published earlier; current balances/status/versions; accepted/rejected decision; conflict/order among arrivals; filesystem write/sync result; crash-tail boundary |

The C2 capacity hints came from benchmark configuration. An eventual Oct compiler/publication path should carry the semantic bounds; Go startup should validate and materialize them rather than baking benchmark constants into generated code.

## 25. What NOT to implement yet

- no Oct layout syntax or new Concept/type universe;
- no production M2 WAL or snapshot migration;
- no unsafe casting, arena, allocator, mmap ownership, Direct I/O, cgo, or `io_uring`;
- no AoS/SoA, padding, alignment, or cache-line directives in Oct;
- no Oct+C2 lane until the semantic contract cleanly crosses the existing generated behavior boundary;
- no general batched-agent-turn redesign based only on this one workload;
- no one-million-account run merely to produce a larger number;
- no attempt to clone TigerBeetle's LSM, replication, or storage engine.

## 26. Remaining limitations

- C2 is a separate experimental engine lane, not production M2.
- Its true batch API differs from the baseline's concurrent per-command burst; that difference is intentional and is a central tested variable.
- Per-change end-to-end effects are coupled; only lookup and record-encoding components are isolated microbenchmarks.
- The final primary subset has three repetitions, not five.
- WSL2 virtualizes storage and exposes no useful thermal/governor telemetry.
- TigerBeetle counters cover the server while Go counters include the in-process harness.
- TigerBeetle ran one replica; C2 lacks replication, repair, networking, compaction, and general indexing.
- Accepted transfers dominate the benchmark; rejection equivalence is covered by shared decision code/tests rather than a measured rejection sweep.
- The short 100k measured phase triggered no GC, so large-population steady scan CPU remains unmeasured for C2.
- C2 snapshots retain the full accepted-effect ledger; long-history compaction and snapshot policy were not studied.
- No 1M population or long-duration C2 run was required.

## 27. Exactly one next recommendation

**Run one design-only cross-workload validation of an Oct semantic layout-contract IR—using existing compiled-data, record-table, Concept, and Read/Write examples—before proposing syntax or changing any runtime format.**

The validation should ask whether the facts identified here (identity, provenance, bounds, keys, batch element shape, and access family) remain meaningful across at least one non-ledger workload. It should not optimize further merely to chase TigerBeetle.

## Required evidence table

Primary allocation/GC values are b64 medians. Hardware values are the representative b512 counter run. RSS is total client+server for TigerBeetle and process RSS for Go lanes.

| Metric | TigerBeetle | Go baseline | C2 Safe-Go |
|---|---:|---:|---:|
| b64 ops/s | 38,609 | 40,658 | 61,245 |
| b64 p99 | 3.33 ms | 2.91 ms | 1.36 ms |
| b512 ops/s | 228,196 | 72,273 | 364,012 |
| b512 p99 | 12.56 ms | 9.38 ms | 1.78 ms |
| hot-source ops/s | 38,085 | 468 | 61,364 |
| 100k ops/s | 29,002 | 36,639 | 55,929 |
| 100k RSS | 3.14 GB | 1.55 GB | 55.3 MB |
| alloc B/op | n/a | 5,078 | 247 |
| allocs/op | n/a | 26.48 | 2.05 |
| GC CPU | n/a | 0.41% capacity / 9.11% process | 0% in b64 run |
| IPC | 1.74 | 1.31 | 1.35 |
| cache misses/1k insn | 5.28 | 7.65 | 4.31 |
| branch misses/1k insn | 0.16 | 1.54 | 0.50 |

## Evidence and reproduction

- Machine-readable summary: `experiments/LayoutM0/summary.json`
- Primary and diagnostic JSON/CSV: `experiments/LayoutM0/evidence/`
- C2 CPU/heap profiles and text tops: `experiments/LayoutM0/profiles/`
- Linux runner: `experiments/LayoutM0/run_linux.sh`
- Summary generator: `experiments/LayoutM0/summarize.py`
- C2 recovery probe: `cmd/layoutprobe`
