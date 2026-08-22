# OctetDB Write M2 — semantic FLOW deltas and compact dedupe persistence

## 1. Verdict

**Success**

M2 keeps full FLOW-TURN checkpoints as authoritative snapshot state, replaces normal per-turn checkpoint WAL repetition with exact semantic deltas, and replaces the persisted Go-map-shaped dedupe JSON with a deterministic compact ordered encoding. Recovery applies authoritative effects and FLOW deltas without calling historical behavior. The canonical state hash is identical across the full-checkpoint/delta and JSON/compact ablations.

At 100k, representative FLOW state fell from 780–784 checkpoint bytes to 54–58 delta bytes. With the same compact-result WAL envelope, full-checkpoint WAL measured 1,461.30 B/op and semantic-delta WAL 486.65 B/op, a 66.7% reduction. Compact dedupe fell from 17.28 MB to 2.08 MB raw, and the complete snapshot fell from 25.73 MB to 11.23 MB. Race, crash, corruption, determinism, exact-checkpoint-equivalence, dedupe-horizon, and full repository tests pass.

## 2. M1 lineage

M1's ordered central commit authority, canonical conflict ownership, segmented checksummed WAL, deterministic global sequence, bounded group sync, authoritative effects, semantic snapshots, snapshot-plus-tail recovery, process-crash retirement protocol, strict program identity, bounded 100,000-command dedupe horizon, and canonical Octagon publication frontier remain unchanged.

The generated FLOW/checkpoint representation remains the retained M7 artifact produced with exact Oct commit `309da01b60ec0f7917d4fd5efd1707bd71d2d40f`; M2 does not regenerate it. This pin records experiment provenance and does not promise compatibility with checkpoint bytes from other Oct compiler revisions.

Oct still owns command admission, nominal semantics, private workflow state, transition control, and yielded decisions. Go still owns mailboxes, registry, conflict scheduling, authoritative accounts/ledger, commit order, WAL, snapshots, dedupe runtime structures, recovery, and publication. No page manager, MVCC, B-tree, LSM, mmap, unsafe, cgo, allocator, replication, or Oct storage syntax was added.

## 3. Measured M1 amplification

The retained M1 evidence measured Oct WAL at roughly 1,562–1,576 B/op versus 558–571 B/op for the direct-Go control. A representative generated checkpoint was about 780 bytes. At 100k, logical accounts plus ledger JSON was 8.90 MB, FLOW checkpoints 0.10 MB, dedupe results 18.45 MB, and the framed snapshot 27.50 MB. These two measured redundancies—checkpoint repetition and map-shaped dedupe JSON—remain the sole motivation for M2.

## 4. FLOW persistent-state inventory

| Logical field | Classification | Restore requirement / M2 treatment |
|---|---|---|
| checkpoint version, package, flow, program fingerprint | immutable | required; validated, not repeated as dirty values |
| board/construction/utility/yield schema identities | immutable feature layout | required; tied to WAL program identity and delta layout version |
| construction `owner` | immutable after construction | required; initial delta derives it from durable AgentID and later deltas reject change |
| current state (`Active`) | rarely changing in this flow | required; encoded only when changed |
| continuation instruction | changes during a turn; yielded frontier is normally 5 | required; encoded only when changed and validated against the state |
| board `TransitionCount` | every turn | required; scalar replacement |
| board `Pending` | workflow-optional / rare | required; scalar replacement |
| board `WasPending` | command-dependent | required; scalar replacement |
| board `PendingTarget`, `PendingAmount` | workflow-optional / rare | required; whole-field replacement |
| last yielded `TransitionDecision` | every turn | required for exact restore and yield observability; compact typed replacement |
| `HasYield` | normally stable true at durable frontiers | required; encoded only when changed |
| history | feature absent (`nil`) | no bytes |
| resume slot | feature absent | no bytes |
| utility-site state | feature absent; arbitration is stateless | no bytes |
| started/completed lifecycle flags and transient input | derived from valid yielded restore / not persistent input | not separately encoded |

Arrays do not occur in this FLOW checkpoint. M2 therefore uses scalar/whole-field replacement and introduces no array-diff machinery.

## 5. Dirty tracking design

A regeneration-safe `DurableAccountAgent` facade lives in the generated package beside the generated FLOW code. It retains the last committed logical payload plus its SHA-256 checkpoint identity. `Step` mutates only the resident speculative machine. `ExportDelta` compares the generated semantic fields against the committed payload, writes only changed field values, and does not mutate the committed frontier. `AcceptCommitted` advances the facade frontier only after successful WAL sync and authoritative apply.

Database-Scheduler sees only the typed facade and opaque checkpoint/delta bytes. It does not inspect generated structs or compiler-private fields. Dirty flags exist only while constructing bytes; they are not machine state and are never persisted as behavior.

The initial parsing-based prototype materially increased the Step path to about 103 allocations/op. It was discarded. The final committed-frontier facade measures about 19–22 allocations/op in memory-mode Oct workloads, close to the retained M1 range of 14–17 and without checkpoint parsing or machine restoration on the hot path.

## 6. Semantic FLOW delta

The AccountAgent delta is a small deterministic binary payload:

```text
magic "OFD1"
delta version = 1
feature-layout version = 1
SHA-256 identity of prior full checkpoint (zero for construction frontier)
known-field bit mask
changed control values
changed board scalar values
typed full replacement of last yielded decision when changed
changed yield flag
```

Signed and unsigned integers use bounded deterministic varints; booleans accept only 0 or 1; nominal reason/effect/status tags are range checked. The field mask has no map iteration. Equivalent transitions produce identical bytes.

## 7. Full checkpoint relationship

Full checkpoints remain authoritative for snapshot installation, agent restore, debugging, and compatibility validation. Every snapshot stores one full checkpoint per durable resident agent, sorted by AgentID. WAL version 3 normally stores the semantic delta. The `FullCheckpointWAL` configuration switch retains the M1 control for the required ablation; it is not the M2 default.

At 100k and 128 agents, snapshots contain 101,032 FLOW checkpoint bytes. This remains tiny beside authoritative ledger state and dedupe, so M2 does not compact snapshot checkpoints.

## 8. FLOW delta recovery

Recovery restores each snapshot checkpoint through the existing generated restore API. For each later WAL record it validates sequence and identity, applies the semantic delta to the previous full logical checkpoint, validates the reconstructed checkpoint through generated restore, installs that machine, and applies the already-logged authoritative effect. It never calls `Step`.

The canonical proof is executable in `TestM2DeltaRecoveryMatchesExactUninterruptedCheckpoint`: snapshot at N, commit two later turns, restart, apply both deltas, then compare the final reconstructed checkpoint bytes. The result is byte-identical, not merely behaviorally similar; canonical authoritative hashes also match.

## 9. FLOW delta compatibility

Compatibility is layered and fail-closed:

- WAL version 3, schema ID, and exact program ID are checked before replay.
- delta magic, delta version, and feature-layout version are checked;
- the prior checkpoint SHA-256 must match the delta's `from` identity;
- unknown field bits, state IDs, nominal tags, invalid booleans, trailing/truncated bytes, and impossible continuation positions are rejected;
- reconstructed full checkpoints re-run existing flow, board, construction, utility, and yield schema/fingerprint validation.

Failures surface as machine-readable `RecoveryCorrupt` for outer WAL/framing/identity failures or `RecoveryIncompatible` for FLOW semantic incompatibility.

## 10. FLOW storage ablation

Per-turn semantic state:

| Scenario | Full checkpoint | Delta | Delta/full |
|---|---:|---:|---:|
| accepted create | 782 B | 58 B | 7.42% |
| one-shot deposit / turn counter | 780 B | 54 B | 6.92% |
| rejected withdrawal | 784 B | 55 B | 7.02% |
| BeginTransfer | 781 B | 57 B | 7.30% |
| Confirm / board change | 781 B | 56 B | 7.17% |

The current flow has no utility-state or resume-slot feature, so inventing specimens for them would not characterize this program. Their absence is encoded in the feature layout.

At 100k with all other settings equal:

| WAL form | Total WAL | Bytes/op | FLOW bytes/op |
|---|---:|---:|---:|
| full checkpoint control | 146.13 MB | 1,461.30 | ~780 checkpoint |
| semantic delta | 48.67 MB | 486.65 | 57.27 delta |

The delta removes 974.64 B/op, or 66.7% of the complete record. Against M1's original 1,562–1,576 B/op, roughly 1.08 KB/op disappeared; the remaining record is now slightly smaller than M1's 558–571 B/op Go control because M2 also removes duplicated caller-result envelope fields.

## 11. Dedupe semantic audit

The runtime map stored `CommandID -> Result` plus an ordered ID slice/head. A `Result` contains sequence, command ID, accepted outcome, reason tag, effect tag, transition count, and a transient duplicate marker. It does not return account IDs, balances, versions, timestamps, or authoritative effect details.

Sequence, accepted/reason/effect, transition count, and exact command ID are required to return the original caller-visible result. The transient `Duplicate` bit is derived for the retry call. Account effects and command/effect identity already exist in each WAL record and authoritative state, but cannot replace the immutable caller result in a snapshot because retired WAL is not available and current state may have changed.

## 12. Minimum exact dedupe payload

The compact immutable entry is exactly:

```text
exact CommandID bytes
commit sequence
accepted bit
reason tag
effect tag
transition count
```

No result is recomputed from current accounts. IDs remain exact strings; no short hash, Bloom filter, or probabilistic rule exists. A retry reconstructs the persisted result and sets only `Duplicate=true` for the current response.

## 13. Compact dedupe format

`compact-v1` is oldest-to-newest deterministic binary:

```text
magic "OCD1" | version
uint32 horizon | uint32 count
repeated:
    uvarint CommandID length | exact bytes
    uvarint sequence
    bool accepted
    varint reason | varint effect | varint transition count
```

The decoder rejects zero/oversized IDs, duplicates, non-increasing sequences, invalid result tags/counts, trailing bytes, and count/horizon/configuration mismatch. The compact bytes are currently a base64 JSON substructure inside the checked snapshot frame; this preserves the readable deterministic semantic sections without building a general serializer.

## 14. Runtime dedupe reconstruction

Recovery sequentially decodes the ordered immutable entries, rebuilds the Go hash map, and rebuilds chronological eviction order. The 100k recovery run spent 23.04 ms decoding/rebuilding dedupe; the 1M snapshot still has only the bounded 100k horizon and spent 35.16 ms. The focused compact run measured 25.65 ms. This is cheap relative to 116 ms total at 100k and 831 ms at 1M.

Runtime lookup remains O(1)-ish map access and eviction remains amortized ordered-slice/head maintenance. The compact ring is not forced onto the hot lookup path.

## 15. Snapshot format

Snapshot version 2 retains M1's `OCTSNAP1` bounded header, SHA-256 payload check, and deterministic typed JSON for accounts, ledger, sorted agents, and Octagon publication. Only dedupe becomes the compact binary substructure. The snapshot records its dedupe format and configured horizon explicitly.

In the same 100k ablation, JSON dedupe produced a 25,733,844-byte snapshot; compact dedupe produced 11,226,037 bytes, a 56.4% complete-snapshot reduction. The focused raw dedupe section fell 88.0%. At 1M the snapshot is 94,822,333 bytes versus M1's 109.99 MB because the genuine 90.94 MB ledger is now dominant.

## 16. WAL format

WAL version 3 preserves segment headers/footers, CRC framing, sequence, command identity, schema/program identity, complete authoritative effect, and deterministic JSON envelope. Normal Oct records contain `flow_delta` and no checkpoint. Go-control records retain their small Go checkpoint. The M1 ablation contains a full FLOW checkpoint and no delta.

The runtime `Result` remains ergonomic, but custom deterministic WAL marshaling persists only accepted/reason/transition count because sequence, command ID, and effect tag already exist in the enclosing record. Unmarshal reconstructs the exact caller result. Effect logging remains independent and historical behavior is never re-executed.

## 17. Commit lifecycle

```text
committed facade frontier
→ speculative Step/yield
→ full checkpoint + semantic delta export
→ enqueue exact record
→ append + Sync
→ authoritative effect apply
→ install full committed checkpoint
→ AcceptCommitted (advance/clear dirty frontier)
→ dedupe install + acknowledgement
```

Before sync, the resident machine may be speculative but the committed checkpoint/facade frontier is unchanged. A duplicate discovered by the authority or any pre-durable failure restores the committed checkpoint. After a sync failure the authority is poisoned because the filesystem outcome can be uncertain. A failure after sync is resolved by replay on restart. Snapshots run as authority barriers and copy only installed committed checkpoints.

## 18. Group commit interaction

Every group member carries its own exact delta and sequence. One shared Sync covers the ordered records. Only after it succeeds does the authority apply each effect, install each checkpoint, advance each facade frontier, install dedupe, and acknowledge. A sync failure acknowledges none.

M2 also fixed an M1 edge case exposed by the new horizon test: an authority batch containing only already-known duplicates after snapshot recovery had zero new records and no active WAL file, yet attempted a Sync. The authority now returns immediately after serving those duplicate results.

## 19. Crash matrix

The retained M1 windows remain green. New injected windows cover after Step/before export, after export/before append, after append/before Sync, after Sync/before dirty clear, after state apply/before dirty clear, snapshot FLOW checkpoint collection, and compact dedupe encoding. Before append the target is absent after restart. A complete appended or synced record may/does replay according to the M1 uncertainty contract. After sync, replay restores effect, full FLOW frontier, and dedupe even when in-memory dirty clear did not occur.

Snapshot failures before installation leave the closed WAL authoritative; after install/before cleanup the valid installed snapshot wins. No speculative post-Step state is present in the snapshot.

## 20. Corruption matrix

Tests fail closed for bad delta version, unknown field mask, unknown state ID, impossible continuation, invalid board boolean/type encoding, wrong program fingerprint, truncated delta, and wrong prior-checkpoint identity. Compact dedupe tests cover corrupt data, count/horizon mismatch, duplicate command IDs, and out-of-order sequences. Retained M1 tests continue to cover checksums, segment order/gaps, snapshot hashes/publication, truncated closed data, and incompatible checkpoints.

## 21. Recovery scaling

| Frontier | Ready | Snapshot | WAL scanned | Delta records | Dedupe decode | Agent restore | Delta apply | Recovery allocation |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100k snapshot | 116.5 ms | 12.60 MB | 0 | 0 | 23.0 ms | 2.5 ms | 0 | 109.3 MB |
| 100k + 10k tail | 509.8 ms | 12.60 MB | 4.87 MB | 10,000 | 23.2 ms | 2.0 ms | 316.2 ms | 253.8 MB |
| 1M snapshot | 830.9 ms | 94.82 MB | 0 | 0 | 35.2 ms | 1.0 ms | 0 | 544.6 MB |
| 1M + 10k tail | 1,104.0 ms | 94.82 MB | 4.93 MB | 10,000 | 23.8 ms | 2.0 ms | 285.4 ms | 752.7 MB |

M1 measured 883 ms for 1M snapshot recovery and 1,132 ms for a 10k tail. M2 stays bounded and modestly improves both. Tail WAL scan falls from M1's 15.65 MB to 4.93 MB. Snapshot-only 100k improves from M1's 233 ms to 116 ms; allocations fall from about 152 MB to 109 MB. The exact canonical hashes match uninterrupted frontiers.

## 22. Storage amplification

At 100k, M2's representative delta WAL is 486.65 B/op and the compact snapshot is 11.23 MB. FLOW checkpoints remain 101,032 bytes for 128 agents. At 1M, the snapshot is 94.82 MB: 90.94 MB logical accounts/ledger, 2.80 MB compact dedupe for that harness's longer IDs, 101,416 bytes FLOW checkpoints, and 6,842 publication bytes.

The 100,000-entry dedupe horizon remains fixed at 1M. Storage growth after that point is genuine ledger history, not dedupe or FLOW repetition.

## 23. Write amplification

At a 100k snapshot cadence, 11.23 MB amortizes to about 112 B/commit. Added to 486.65 B/op WAL, application-level write amplification is about 599 B/command, down from M1's approximate 1.85 KB/command. Fsync-each remains one sync/op. Group-16/wait-0 measured 8 commands/sync for independent keys, 6.56 for transfers, and one for a hot key. Snapshot installation retains its file/segment sync boundaries.

## 24. Performance

Representative current-run results:

| Lane/mode | Independent | Hot key | Transfer |
|---|---:|---:|---:|
| Oct fsync each | 2,307/s | 2,246/s | 2,373/s |
| Go fsync each | 2,256/s | 1,923/s | 2,450/s |
| Oct group 16/0 | 16,720/s | 2,325/s | 14,480/s |
| Go group 16/0 | 18,507/s | 2,550/s | 14,591/s |

Absolute fsync throughput was lower than the retained M1 run for both Oct and the unchanged Go control, indicating run/environment variability rather than a FLOW-delta-specific durable regression. Within the same run, Oct remains close to Go under durability and preserves the exact batching economics. Memory-mode Oct reached 427k/s independent and 536k/s transfer; it remains slower than Go because generated yield/checkpoint and delta construction are visible when storage is absent.

## 25. Memory/allocation

Compact snapshot recovery at 100k allocated 105.8 MB in the focused ablation versus 151.4 MB for JSON dedupe. The normalized 1M recovery allocated 544.6 MB versus M1's 587 MB. The 10k delta tail allocated 752.7 MB total, approximately M1's 750 MB, because applying each delta deliberately reconstructs and validates an exact full checkpoint.

Steady memory-mode Oct measured 19–22 allocations and about 4.66–4.77 KB/op; Go measured 13–16 allocations and about 1.78–1.89 KB/op. The final facade avoids the rejected parsing prototype's 103 allocations/op. No pooling, arena, unsafe code, or manual memory machinery was introduced.

## 26. Architecture verdict

1. **Is dirty semantic FLOW state the right WAL representation?** Yes. It removes two-thirds of complete WAL bytes while retaining exact behavioral state.
2. **Should full FLOW checkpoints remain snapshot-only?** Yes, except for the explicit historical ablation/compatibility control.
3. **How much WAL amplification disappeared?** 974.64 B/op in the controlled M2 ablation; about 1.08 KB/op versus M1's original format.
4. **Is compact dedupe persistence sufficient?** Yes. It removes 88% of raw dedupe bytes with exact IDs/results and strict validation.
5. **Is runtime map reconstruction cheap enough?** Yes: roughly 23–35 ms for the bounded 100k horizon.
6. **Does JSON remain acceptable anywhere?** Yes for deterministic inspectable authoritative/agent/publication sections and the small WAL envelope. It was unacceptable for the measured dedupe section and repeated full checkpoint.
7. **Did representation refinement remove pressure without storage-engine complexity?** Yes. The result is still ordered WAL, snapshots, exact bounded dedupe, and safe Go—not a new storage engine.

## 27. Oct pressure findings

No Oct repository change was required. The current compiled facade does not natively emit/apply logical deltas or retain a committed dirty frontier, so M2 implements a narrow regeneration-safe durable facade beside the generated package. It is generic in shape—stable-yield logical delta export/apply/accept—but the concrete codec is intentionally generated-flow-specific.

A future Oct compiler pass could emit this facade from FLOW MIR for arbitrary feature layouts. M2 does not justify that broader change: the local seam is exact, does not expose internals to the database, and proves the economics first.

## 28. Remaining limitations

- Delta tail replay reconstructs and validates a full JSON checkpoint per record; 10k apply costs about 285–316 ms and remains allocation-heavy.
- The compact dedupe blob is base64 inside snapshot JSON, adding 4/3 expansion. A fully binary outer snapshot could save more but is not justified while ledger history dominates.
- The 1M snapshot is now overwhelmingly the uncompressed correctness-relevant ledger; M2 does not compact or retire it.
- The durable facade is a retained generated-package sidecar, not compiler-emitted for every FLOW feature combination.
- Utility-site, resume-slot, history, and array delta paths are compatibility design points, not exercised features of AccountAgent.
- Snapshot creation remains a stop-the-authority barrier; 1M pause varied from roughly 0.35–0.45 s in final runs.
- M1's Windows directory-fsync and process-crash-only claims remain unchanged.
- The dedupe guarantee is intentionally bounded; IDs older than the configured horizon execute as new.

## 29. Exactly one next recommendation

**Measure and design bounded ledger/history compaction.**

At 1M, logical state/ledger is 90.94 MB of the 94.82 MB snapshot, while FLOW checkpoints are 0.10 MB and compact dedupe is bounded near 2.8 MB. Representation refinement has removed the targeted redundancy; the next measured storage pressure is now genuine retained history, not agent state, commit authority, or dedupe.

## Reproduction

```powershell
go test -race ./internal/m7write
go test ./...

go run ./cmd/m9storage -operations 100000 `
  -out experiments/M9/evidence/storage-100k.json
go run ./cmd/m8recovery -sizes 100000 `
  -out experiments/M9/evidence/recovery-100k.json
go run ./cmd/m8recovery -sizes 1000000 `
  -out experiments/M9/evidence/recovery-1m.json
go run ./cmd/m8bench -operations 2000 -accounts 128 -workers 16 `
  -out experiments/M9/evidence/performance.json
```

Normalized evidence is retained under [`experiments/M9`](../../experiments/M9). Giant raw WAL and snapshot files are intentionally discarded.
