# OctetDB Write M1 — durability, recovery, and storage specialization

## 1. Verdict

**Success**

M1 replaces full-history restart with a deterministic semantic snapshot plus bounded WAL tail, adds sequence-identified checksummed segments and safe retirement, and makes batch acknowledgement genuinely group-durable. Crash and corruption matrices pass under the race detector. At 1M commits, M0 replay scanned 1.457 GB and took 21.75 s; M1 restored the identical canonical hash from a 110.0 MB snapshot in 0.883 s, or in 1.132 s with a 10k-record tail.

The resulting storage architecture remains small: an ordered commit authority, append-only segments, periodic stop-the-world logical snapshots, and bounded dedupe. No page manager, MVCC, B-tree, LSM, mmap, unsafe code, cgo, custom allocator, or Oct storage implementation was introduced.

## 2. M0 lineage

The resumed M7 result remains intact. Oct owns typed admission, scalar utility arbitration, transition meaning, private workflow state, and yielded effects. Go owns keyed queues, conflict identity, authoritative mutable state, WAL/fsync, recovery, snapshots, and Octagon publication. M1 does not restart that architecture experiment or reinterpret the M7 PostgreSQL figures as universal equivalent-guarantee benchmarks.

The utility lesson is preserved unchanged: feature predicates feed scalar deterministic arbitration, followed by one typed switch and one rich decision. M1 adds no Oct compiler work and does not use record-valued `when utility` candidates as an inference system.

## 3. Durability invariants

1. A command acknowledged by `SyncEach` or M1 group commit has crossed the applicable WAL `Sync` and cannot disappear under the implemented process-crash model.
2. One committed record applies all authoritative account fields and ledger effects under one Store lock; no partial multi-account state is published.
3. authoritative state, dedupe result, and the agent's FLOW-TURN checkpoint advance under the same ordered commit authority and sequence.
4. A command ID within the configured dedupe horizon returns the persisted original result after restart.
5. An incomplete frame is authoritative only if it was already covered by an installed snapshot; otherwise an incomplete active tail is truncated at the last valid frame.
6. Bad checksums, identities, headers, footers, sequence order, snapshot hashes, or checkpoints fail closed.
7. Snapshot plus subsequent WAL replay produces the same canonical Octagon bytes and SHA-256 as uninterrupted execution.
8. Snapshot/publication bytes are produced from one committed Store frontier while the commit authority is stopped at a sequence boundary.

Guarantees by mode:

| Mode | Acknowledgement contract | Crash consequence |
|---|---|---|
| memory | after authoritative in-process apply; no WAL | acknowledged work after the latest explicit snapshot may be lost |
| M1 group | after every record in the returned group is appended and one shared `Sync` succeeds | same durable acknowledgement guarantee as fsync-each, with a shared boundary |
| fsync-each | after that record's append and `Sync` | acknowledged record replays even if memory apply/ack did not occur |
| M0 `BatchSync` control | usually before sync; sync only at batch threshold/close | retained solely as the historical recovery control; not called group-durable |

Filesystem and device write caches can be stronger or weaker than Go's `File.Sync` contract. M1 makes no device-level power-loss claim beyond the OS-visible operations tested here.

## 4. M0 crash-window reconstruction

The M0 successful path was:

```text
mailbox dequeue
→ canonical conflict acquire
→ dedupe precheck
→ authoritative state copy
→ resident Oct Step
→ yielded decision validation
→ post-turn FLOW checkpoint export
→ global commit mutex + dedupe recheck
→ sequence assignment + WAL frame construction
→ file Write
→ Sync (fsync-each or batch threshold only)
→ authoritative Store apply
→ resident committed-checkpoint update
→ dedupe-result update
→ commit/conflict release
→ result publication
```

Before append, recovery sees no command. A partial append is truncated only at the active tail. A complete unsynced append may be absent or may replay, so a write error is uncertain until command-ID lookup after restart. After sync but before state apply, replay applies the command. After state apply but before checkpoint/dedupe/ack, replay reconstructs the entire record frontier. M0's batch mode could acknowledge before its durability boundary and therefore did not satisfy invariant 1.

## 5. Final M1 commit protocol

```text
mailbox dequeue
→ canonical conflict acquire
→ authoritative state copy
→ Oct Step (or direct-Go control decision)
→ yielded decision validation
→ logical checkpoint export
→ enqueue bounded central commit request
→ authority dedupe check
→ monotonic sequence assignment
→ deterministic WAL construction
→ segment rotation if bounded count reached
→ append up to max-group records
→ one WAL Sync
→ authoritative Store apply in sequence
→ committed agent-checkpoint install
→ bounded dedupe-result install
→ result publication
→ conflict release
→ caller acknowledgement
```

Independent decisions can overlap before the authority. Conflicting commands retain their canonical tokens while waiting, so state observations cannot become stale underneath a prepared decision. The authority never speculatively mutates authoritative state before durability.

## 6. WAL architecture

Segments live under `wal/segment-<first-sequence>.wal`. The deterministic segment identity is its first global sequence. The M1 default rotates at 4,096 records; tests use smaller bounds to force rotation.

Each 32-byte header contains magic `OCTWAL01`, format version 2, segment identity, first sequence, and CRC32. Records retain M0's bounded `uint32 length | deterministic JSON payload | CRC32` framing and 1 MiB maximum. Each closed segment has a 24-byte `OCTEND01` footer containing last sequence, record count, and CRC32. Non-final segments must be closed; the final active segment alone may have a truncated tail. Global sequence gaps detect missing middle segments.

Every record includes authoritative effects rather than replaying historical commands through current behavior, plus schema/program identities and the post-turn logical checkpoint. Oct WAL records use the exact FLOW-TURN program fingerprint; the Go control uses a distinct program identity.

## 7. Snapshot architecture

The snapshot is an outer bounded frame (`OCTSNAP1`, version, payload length, SHA-256) around deterministic typed JSON. Its semantic payload contains:

- committed sequence and state/program identities;
- accounts sorted by ID and the correctness-relevant ledger;
- the bounded ordered dedupe result horizon;
- sorted `(AgentID, FlowCheckpointBytes)` entries;
- canonical Octagon publication bytes and their logical hash.

It deliberately does not serialize goroutines, channels, locks, maps, generated Go object layouts, or other implementation graph state. JSON was selected for M1 simplicity and inspectability, not as a final compact format.

## 8. FLOW-TURN checkpoint integration

Oct agent entries persist the existing generated logical checkpoint bytes without a second behavioral representation. Recovery parses and restores those bytes through the generated FLOW-TURN API. The direct-Go control uses a small separately fingerprinted `{pending transfer, turn count}` checkpoint while sharing all other storage machinery.

At 128 active agents, the 100k snapshot contained 101,032 checkpoint bytes, about 789 bytes/agent. M7's retained population experiment remains the residency evidence: about 1.60 KB and six objects per resident checkpointed Oct identity at 100k, with restore around 5.56 µs and 1,328 B/18 allocations.

## 9. Snapshot installation / WAL retirement

The authority first closes and syncs the active segment, serializes one frontier, writes a uniquely named `.tmp` snapshot, syncs that file, and renames it to `snapshot-<sequence>.snap`. Only after successful installation are closed segments with `lastSequence <= snapshotSequence` removed. Startup removes stale `.tmp` files.

The crash matrix proves: a crash during temp write or after temp sync leaves the prior WAL authoritative; a crash after install but before cleanup selects the valid snapshot and safely ignores/then retires covered WAL. Unique snapshot names avoid replacement ambiguity.

On Windows, Go does not provide a portable reliable directory-fsync contract. M1 therefore claims process-crash behavior for synced file plus same-volume rename, as exercised by injection tests, but does not overstate power-cut atomicity of directory metadata.

## 10. Recovery

```text
discover highest named snapshot
→ validate outer frame/hash and strict schema/program identity
→ restore sorted authoritative state
→ verify stored Octagon bytes/hash against restored Store
→ restore dedupe horizon
→ restore resident agent checkpoints
→ discover and validate WAL segments
→ replay only sequences after snapshot frontier
→ validate contiguous final sequence
→ open final active segment or lazily create the next
→ ready
```

Metrics separate snapshot decode, WAL scan, records replayed, agents restored, bytes scanned, allocations, and total time to ready. A corrupt latest snapshot fails closed rather than silently falling back.

## 11. Recovery scaling

| Frontier | Control | Tail replayed | Ready time | Snapshot | WAL scanned | Recovery allocation |
|---:|---|---:|---:|---:|---:|---:|
| 10k | M0 log-only | 10,000 | 198.5 ms | — | 14.37 MB | 82.7 MB |
| 10k | M1 snapshot | 0 | 28.2 ms | 2.83 MB | 0 | 18.2 MB |
| 11k | M1 snapshot + tail | 1,000 | 48.9 ms | 2.83 MB | 1.55 MB | 27.8 MB |
| 100k | M0 log-only | 100,000 | 2,025 ms | — | 144.69 MB | 812 MB |
| 100k | M1 snapshot | 0 | 233 ms | 27.50 MB | 0 | 152 MB |
| 110k | M1 snapshot + tail | 10,000 | 445 ms | 27.50 MB | 15.55 MB | 252 MB |
| 1M | M0 log-only | 1,000,000 | 21,746 ms | — | 1.457 GB | 8.18 GB |
| 1M | M1 snapshot | 0 | 883 ms | 109.99 MB | 0 | 587 MB |
| 1.01M | M1 snapshot + tail | 10,000 | 1,132 ms | 109.99 MB | 15.65 MB | 750 MB |

The uninterrupted and snapshot paths have identical hashes at 10k, 100k, and 1M. A recovered medium tail has the expected later hash. The first 100k run also exposed and then removed an O(window) allocation on every dedupe eviction; the final evidence uses amortized head/compaction maintenance.

## 12. Group commit

M1 compares group sizes 4/16/64 and waits 0/50/200 µs/1 ms. A 16-command, 50 µs group reached about 31.3k Oct and 34.7k Go commands/s on independent keys with about 15 commands/sync. Positive waits were harmful on a hot key: there is only one runnable command behind that key's mailbox/token, and Windows timer granularity turned the wait into roughly millisecond-scale delay.

The selected default is **max 16, wait 0**. It opportunistically drained about 8 independent commands/sync and 6.58 transfer commands/sync, reaching 21.7k and 17.4k Oct commands/s respectively, while degrading cleanly to one command/sync and roughly fsync-each throughput on a hot key. It never acknowledges any member before the shared sync.

## 13. Commit authority

One ordered authority is the right M1 shape. It owns sequence, WAL order, group boundaries, state/checkpoint/dedupe frontier, snapshot barriers, and publication consistency. It is visible in memory-only Go/Oct differences, but under fsync and group durability storage remains dominant. Lock-free or multiwriter WAL work is not justified.

M1 did not add a separate speculative decision queue beyond existing concurrent agent workers. The bounded authority channel is sufficient: parallel independent decisions feed one actuator, and hot-key work is correctly serialized upstream.

## 14. Crash injection

| Failure point | Recovered observation |
|---|---|
| before append | target absent |
| during append | partial active tail truncated; target absent |
| after complete append before sync | complete unacknowledged record may replay; ID resolves outcome |
| after sync before apply | record replays and applies |
| after apply before ack | record and dedupe result survive; retry is duplicate |
| during rotation | prior closed frontier survives; target absent |
| during snapshot temp write | temp ignored/removed; WAL restores state |
| after snapshot sync before install | prior WAL restores state |
| after install before cleanup | installed snapshot restores; covered WAL is harmless |

These are deterministic injected process-stop/error seams. They do not simulate controller/device loss of a completed OS sync.

## 15. Corruption behavior

Tests fail closed with categorized diagnostics for invalid header/footer, record checksum, missing middle segment, out-of-order sequence, corrupt/truncated closed data, snapshot outer hash, snapshot publication hash, and invalid FLOW-TURN checkpoint. An incomplete final active frame alone is treated as a truncatable uncommitted tail. No automatic repair or record skipping exists.

## 16. Dedupe persistence

M0 retained an unbounded result map. M1 defaults to the most recent 100,000 committed command IDs, persists that exact order/results in snapshots, and rebuilds it from tail records. An ID older than the window is semantically a new command; callers needing a longer retry horizon must configure a larger bound. The backing order slice compacts amortized and stays within roughly twice the configured horizon instead of leaking or copying the whole window per eviction.

## 17. Agent residency

M1 retains always-resident agents because M7 measured a real 100k population and because snapshot restore is only about 5.56 µs/agent. A cache was not implemented: at 100k the known 160 MB residency is material but still straightforward, while eviction would add lifecycle and checkpoint-currentness complexity without a measured workload requiring millions of simultaneously addressable workflows.

This is a rejection for M1, not a claim that residency scales indefinitely. Restore-on-demand remains economically plausible once a larger real entity population demonstrates the need.

## 18. Authoritative-state representation

The authoritative account state remains `map[AccountID]Account` plus an append-only ledger slice. Conflict-protected reads copy at most two accounts; apply uses one Store mutex and fixed effect switches. Canonical export sorts IDs. At the 128-account durability workload, checkpoint, JSON/WAL serialization, sync, ledger/dedupe snapshot encoding, and timer behavior dominate; there is no evidence for dense-ID arrays, sharding, or a custom allocator.

## 19. Performance

Representative 16-worker results:

| Lane | Mode | Independent cmd/s (p50/p95/p99 µs) | Hot cmd/s | Transfer cmd/s |
|---|---|---|---:|---:|
| Oct | memory | 553,006 (0/0/519) | 299,581 | 647,124 |
| Go | memory | 975,087 (0/0/517) | 555,494 | 776,910 |
| Oct | fsync each | 3,366 (4,642/5,505/5,897) | 3,387 | 3,293 |
| Go | fsync each | 3,458 (4,623/5,587/6,161) | 3,423 | 3,450 |
| Oct | group 16/wait 0 | 21,748 (528/1,144/1,910) | 3,113 | 17,426 |
| Go | group 16/wait 0 | 24,372 (520/1,039/1,502) | 3,060 | 19,145 |
| PostgreSQL 17 | transaction commit | 2,529 (5,275/9,146/30,737) | 404 | 2,423 |

Windows clock quantization still censors many memory-mode percentiles to zero; throughput and upper tails remain usable. PostgreSQL has broader transactional/query/recovery machinery and its normal commit semantics; this is a real control, not byte-equivalent storage.

## 20. Contention

Independent keys populate the group authority efficiently. Random transfers acquire two canonical tokens and averaged 6.58 commands/sync with the selected group. A hot key cannot populate a durability group because its FIFO agent and token correctly expose only one prepared transition at a time; it therefore stays near 3.1–3.4k/s rather than receiving artificial batching credit. PostgreSQL hot-row contention reached about 404/s under its broader mechanism.

## 21. Storage amplification

| Item at 100k commits / 128 agents | Bytes |
|---|---:|
| logical accounts + ledger JSON | 8,900,097 |
| FLOW checkpoints | 101,032 |
| bounded dedupe results | 18,452,596 |
| canonical Octagon publication | 6,714 |
| complete framed snapshot | 27,500,080 |

Oct WAL records are about 1,562–1,576 B/op; the direct-Go control is about 558–571 B/op. The roughly 1 KB difference is predominantly the rich generated FLOW-TURN checkpoint retained per committed turn. At 1M, the dedupe horizon stays bounded but the correctness-relevant ledger makes the semantic snapshot 109.99 MB; M1 bounds history replay, not the size of genuine authoritative history.

## 22. Write amplification

Application-level, not device-physical, amplification is measured. Fsync-each performs one sync/op. Selected group commit performs one sync per 8 independent operations, 6.58 transfers, or one hot-key operation. At a 100k snapshot cadence, the 27.5 MB snapshot amortizes to about 275 B/commit; combined with Oct's roughly 1.57 KB WAL it is about 1.85 KB of application bytes written per logical command, excluding filesystem metadata and segment headers/footers. Snapshot installation itself adds one snapshot-file sync and segment-close sync.

## 23. Write→Read publication frontier

The durable snapshot embeds the exact canonical Octagon bytes and SHA-256 produced from its committed Store state. Recovery recomputes both from restored authoritative data and rejects mismatch. Tests prove uninterrupted and snapshot+WAL recovery bytes/hashes agree and that no half-old/half-new frontier can be emitted.

Octagon is therefore the publication format but not the primary recovery format. Making the entire durable snapshot Octagon would complicate compact opaque FLOW checkpoints and dedupe results without evidence of faster restart.

## 24. Combined publication economics

At 100k commands, durable snapshot creation paused the authority for about 114–127 ms; canonical publication is only 6.7 KB for the 128-account specimen and is included in that pause. The M6 read-plane control required about 24.6 s to derive, emit, and build a different 100k-row compiled epoch. Thus Write can checkpoint far more frequently than Read should compile. A one-minute compiled publication epoch would spend about 0.2% of wall time in this Write snapshot pause but roughly 41% in the retained M6 build path; Read compilation, not Write frontier creation, governs cadence. The datasets differ, so this is an engineering thought experiment rather than a combined benchmark.

## 25. Memory-management interpretation

M1 remains ordinary safe Go. Oct memory-only turns measured about 4.3 KB/op and 14–17 allocations; the M1 Go control measured about 1.7–1.8 KB/op and 13–16. Under fsync these rose because JSON framing and authority envelopes dominate, while throughput converged around 3.3–3.45k/s. At recovery, full-history checkpoint decode/restore allocated 8.18 GB cumulatively at 1M; snapshot recovery allocated 587 MB.

The experiment directly found that representation/lifetime policy mattered: removing the accidental dedupe-window copy eliminated a 16 GB 100k-tail allocation pathology. Manual allocation would not fix fsync, timer granularity, repeated JSON, or checkpoint amplification. Architecture and representation remain the first levers.

## 26. Architecture verdict

1. **Is append-log + snapshots enough?** Yes for this specialized single-process write plane through 1M commits.
2. **Is central commit authority right?** Yes; it makes ordering, group durability, snapshots, dedupe, and publication one frontier without becoming the measured durable bottleneck.
3. **Is group commit essential?** Yes for independent/transfer durable throughput, but not useful for a single hot key that cannot form a group.
4. **Are per-agent checkpoints worth retaining?** Yes for durable multi-turn behavior, but embedding a full checkpoint in every WAL record is the largest Oct storage amplification.
5. **Should agents remain resident?** Yes for M1; 100k is measured and simple. Revisit only with a larger real population.
6. **Should Octagon be snapshot, publication, both, or neither?** Publication only; the recovery snapshot embeds it as a verified frontier artifact alongside a simpler host snapshot representation.
7. **Where does specialization beat conventional machinery?** Closed effect shapes permit one append record, canonical tokens, direct map effects, bounded checkpoints, and snapshot+tail recovery without pages, indexes, MVCC, or redo/undo subsystems.
8. **Where is conventional machinery valuable?** PostgreSQL still provides general queries, concurrent transactions, mature crash/power-loss engineering, catalog evolution, operational tooling, and storage compaction beyond this bounded specimen.

## 27. Oct pressure findings

M1 found no new narrow compiler blocker and made no Oct changes. Existing generated checkpoint size and typed-yield allocation remain storage/runtime pressure, while the scalar utility-code policy remains effective. Strict program fingerprints avoid replaying old commands through changed Oct behavior.

## 28. Remaining limitations

- The snapshot and WAL use deterministic JSON; decode allocation and dedupe representation are visibly large.
- Every Oct WAL record carries a roughly 780-byte agent checkpoint even when only a transition counter changed.
- The ledger is retained in full semantic snapshots; logical history size is not compacted.
- Directory metadata fsync is not claimed on Windows; tests cover process failure, not raw power-cut/device-cache behavior.
- Missing middle segments are detected by sequence gaps, but deletion of the newest unmanifested segment after an abnormal external filesystem action cannot be distinguished from an earlier valid frontier.
- Snapshot creation is a bounded stop-the-authority pause, not MVCC; 100k took roughly 0.12 s and 1M pause was not separately retained.
- Automatic cadence is count-based and intentionally simple; no adaptive daemon, background service, or concurrent snapshot copier exists.
- The dedupe guarantee is explicitly bounded; IDs older than the horizon can execute again.
- PostgreSQL server-side WAL bytes, physical write amplification, and server allocations were not measured; reported PostgreSQL allocations are client/application-side only.
- There is no replication, cross-process writer coordination, migration DSL, or full M6 hot-swap integration.

## 29. Exactly one next recommendation

**Perform durable snapshot format refinement focused on bounded dedupe encoding and FLOW-checkpoint/storage amplification.**

The evidence selects this over partitioning or replication: at 100k, dedupe consumes 18.45 MB of a 27.50 MB snapshot, and Oct WAL is roughly 1 KB/op larger than the Go control because it embeds a full logical checkpoint. A narrow deterministic bounded-binary experiment can attack the measured recovery/storage costs while preserving the successful commit authority, semantic snapshot, strict fingerprints, and Oct/Go ownership boundary.

## Reproduction

```powershell
go test -race ./internal/m7write
go test ./...

$env:DBSCHED_POSTGRES_DSN='postgres://dbsched:dbsched@localhost:54329/dbsched?sslmode=disable'
go run ./cmd/m8bench -operations 2000 -accounts 128 -workers 16 `
  -out experiments/M8/evidence/performance.json

Remove-Item Env:DBSCHED_POSTGRES_DSN -ErrorAction SilentlyContinue
go run ./cmd/m8bench -batch-matrix -operations 1000 -accounts 128 -workers 16 `
  -out experiments/M8/evidence/batching-matrix.json

go run ./cmd/m8recovery -sizes 10000,100000 `
  -out experiments/M8/evidence/recovery.json
go run ./cmd/m8recovery -sizes 1000000 `
  -out experiments/M8/evidence/recovery-1m.json
```

Normalized evidence is in [`experiments/M8`](../../experiments/M8), while the original M7 report and specimens remain unchanged.
