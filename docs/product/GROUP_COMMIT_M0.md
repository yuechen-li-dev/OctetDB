# OCTETDB-GROUP-COMMIT-M0

## 1. Verdict

**Success.** Ordinary concurrent `Database.Mutate` calls now share bounded durable flushes without an API or WAL-format change. Focused correctness tests, the complete race suite, controlled v0.2.0 comparisons on WSL/ext4 and Windows/NTFS, and the fast harness all pass.

## 2. PERF-M4 motivation

PERF-M4 found default concurrent durable W1-W4 throughput at 0.12-0.18x PostgreSQL and a W1 profile dominated by filesystem Sync. PostgreSQL used ordinary synchronous group commit. M0 therefore changes flush cadence only, not transaction execution or durability promises.

## 3. Existing mutation/durability path

Before M0, `Database.Mutate` wrapped the callback in `KeyedDB.SubmitKeyed`. A capacity-one `admission` token serialized every mutation, read, scan, catalog operation, snapshot, and close. The callback built a transaction-local write map; finalize sorted keys and checked bounds; result/rejection and writes formed one `keyedWALRecord`; `appendKeyed` JSON-encoded one length/payload/CRC frame, wrote it, called `file.Sync`, then `applyKeyed` published records, sequence, dedupe result, and query keys. The caller returned only after all of that.

## 4. Existing acknowledgement semantics

A successful accepted or rejected decision meant its complete ordinary frame had returned successfully from Sync. Operational callback errors wrote no frame and consumed no ID. A retained duplicate replayed the exact result without a write. A callback panic propagated on the caller goroutine. M0 preserves each behavior.

## 5. Group-commit design

Each open database owns one coordinator goroutine, one mutex-protected caller-owned pending queue, and one buffered wake signal. The coordinator takes at most 64 requests, holds the existing database authority for the whole group, executes callbacks serially, appends individual frames, calls Sync once, and completes individual buffered responses. There is no timer, periodic durability, parallel callback execution, goroutine per member, or public knob. The internal test harness can force group size one.

## 6. Linearization/order model

The coordinator queue order is the database mutation order. For admitted A/B/C: callback order, staged visibility order, sequence numbers, WAL-frame order, commit visibility, and response eligibility are A/B/C. Scheduling across independent callers is not promised across runs.

## 7. Staging/visibility model

M0 uses the existing maps as an authority-private staged view while holding `admission`. B therefore sees A and C sees A+B, matching serialized execution. External reads cannot enter. If append or Sync fails, the handle is poisoned for every later operation, including reads, until close/reopen; close skips snapshot. Thus staged undurable state cannot escape or become a base for later work. This is narrower than a general overlay and requires no clone or rollback engine.

## 8. WAL/Sync model

Every decision retains the v0.2 length + JSON payload + CRC frame. Frames are contiguous and Sync cadence is the only format-level change. No member whose result depends on a new group frame is completed before the shared Sync succeeds. Instrumentation counts groups, frames, Sync calls, maximum size, and a 1-64 group-size histogram internally.

## 9. Durable rejection behavior

`Reject` and `RejectWithResult` create ordinary mutation-free frames and share Sync with accepted decisions. Their code/result replays exactly after commit and restart.

## 10. Operational error behavior

A non-rejection callback error creates no frame, consumes no command ID, and does not discard durable neighbors. Its response is delivered when the current group finishes; retry may execute normally.

## 11. Cancellation semantics

Cancellation is checked immediately before callback execution. A canceled queued request executes no callback, writes no frame, and consumes no ID. Once callback execution starts, cancellation cannot revoke the command: the caller receives its durable result after Sync (or the storage failure), including cancellation during frame append or Sync. No helper goroutine is leaked.

## 12. Duplicate/in-flight idempotency

Committed duplicates replay as before. A duplicate already drained behind a newly staged original attaches to the original decision, marks `Duplicate`, executes no callback, and cannot complete until the original frame's shared Sync. A duplicate queued while that Sync runs is handled in the next group against the now-durable dedupe entry. The bounded `DedupeHorizon` remains the only retention promise. A dedicated append-failure test proves an attached duplicate is not falsely acknowledged if a later frame fails.

## 13. Failure injection

Two narrow package-internal seams inject errors before frame append and before Sync. There is no storage abstraction or public fault API. Tests cover injected Sync failure and an append failure after an original and duplicate have staged.

## 14. Recovery behavior

Recovery is unchanged: complete CRC-valid frames replay as a contiguous sequence; a short final header/body/checksum is truncated; corruption of a complete frame fails closed. If a physical prefix A/B survives a failed/unacknowledged group, reopening may replay it. This is the existing “commit may have happened although the response was lost” window, made safe by command-ID retry. One Sync is not treated as atomic multi-frame write.

## 15. Close behavior

`Close` atomically stops admission, wakes the coordinator, drains all requests admitted before close, waits for the coordinator, then snapshots/closes under the existing authority. New callers fail closed and no admitted caller hangs. A poisoned handle closes the WAL without snapshotting staged memory.

## 16. Allocation/runtime structure

The runtime adds one goroutine per database and one bounded response object/channel per concurrent caller. Groups use fixed result/durability arrays; the queue is sliced without a per-group copy. Formal malloc counts increased 6-22% depending workload/concurrency (roughly 5-8 extra allocations per operation); this is measurable but modest relative to the 3.2-3.7x c8 throughput gain. WAL bytes per durable decision are unchanged.

## 17. Fast dev harness design

`TestGroupCommitDevHarness` is opt-in and runs D1 transfer, D2 inventory reserve, D3 job transition, and D4 webhook/idempotent decision at c1/c8/c32. It compares package-internal legacy single-Sync mode with default group commit and emits JSON containing workload, concurrency, operations, throughput, p50/p95/p99, allocations/bytes, WAL bytes, Sync calls, commands/Sync, group-size distribution, and correctness digest. A PowerShell comparator reports throughput, p99, allocation, Sync, and commands/Sync deltas with non-binding -10%/+20% warnings.

## 18. Dev harness timing

Normal Windows/NTFS: 3.874 seconds. Normal WSL/ext4: 12.069 seconds. Windows smoke: 1.077 seconds. All are far below 60 seconds; build/test setup is included in the `go test` wall clock but JSON `elapsed_ms` measures the harness body.

## 19. W1 transfer results

Formal WSL/ext4, 1k records, 384 measured operations:

| c | v0.2.0 ops/s | group ops/s | gain | v0.2 p99 | group p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,022 | 1,003 | 0.98x | 1.45 ms | 1.23 ms |
| 8 | 968 | 3,527 | 3.64x | 9.95 ms | 2.65 ms |
| 32 | 994 | 11,763 | 11.83x | 34.61 ms | 3.06 ms |

## 20. W2 inventory results

| c | v0.2.0 ops/s | group ops/s | gain | v0.2 p99 | group p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 982 | 1,010 | 1.03x | 1.64 ms | 1.55 ms |
| 8 | 1,012 | 3,674 | 3.63x | 9.57 ms | 2.57 ms |
| 32 | 1,014 | 12,081 | 11.92x | 32.47 ms | 2.77 ms |

## 21. W3 jobs results

| c | v0.2.0 ops/s | group ops/s | gain | v0.2 p99 | group p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,270 | 1,196 | 0.94x | 1.22 ms | 1.53 ms |
| 8 | 1,210 | 4,081 | 3.37x | 14.90 ms | 2.45 ms |
| 32 | 1,278 | 12,486 | 9.77x | 27.46 ms | 3.02 ms |

## 22. W4 webhook results

| c | v0.2.0 ops/s | group ops/s | gain | v0.2 p99 | group p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,123 | 1,124 | 1.00x | 1.35 ms | 1.94 ms |
| 8 | 1,150 | 4,066 | 3.53x | 8.12 ms | 2.60 ms |
| 32 | 1,125 | 13,559 | 12.06x | 33.69 ms | 3.13 ms |

## 23. c1/c8/c32 scaling

c1 remains deliberately ungrouped and ranges from -6% to +3% on ext4 (-2% to +15% on NTFS), without a p99 pathology. Every W1-W4 lane exceeds 3x at c8. c32 reaches 9.8-12.1x on ext4 because the bounded coordinator continues closing groups instead of allowing a continuously filling queue to starve earlier requests.

## 24. commands-per-sync/group-size evidence

WSL dev harness: D1-D3 commands/Sync are 1.0/4.0/16.0 at c1/c8/c32; D4 is 1.0/2.0/8.0 because half its calls are exact duplicates and append no frame. At c8, group p95 is 5-7 for D1-D3 and 3 for D4; maxima are 7-8 and 4. At c32, p95 is 31 and maxima 31-32 for D1-D3; D4 p95/max is 15. Minimum remains one, proving there is no artificial wait.

## 25. WSL/ext4 results

The primary environment is WSL2 Linux 6.6.87.2 on `/dev/sdf` ext4, Go 1.27. The formal tables above compare a freshly built pinned v0.2.0 module with the corrected checkout using the same harness and 12 matched W1-W4/c1/c8/c32 lanes. Every correctness flag passed. Raw JSON is under `evidence/GROUP_COMMIT_M0/raw/wsl-v0.2.0` and `wsl-current`.

## 26. Windows/NTFS controls

Windows 10.0.26200, NTFS, Go 1.26.2 showed c8 gains of W1 3.46x, W2 3.44x, W3 3.17x, and W4 3.72x. c32 gains were 7.77-10.62x. c1 was 0.98-1.15x. All correctness flags passed; raw JSON is under `windows-v0.2.0` and `windows-current`.

## 27. PostgreSQL gap comparison

No `PERF_M4_POSTGRES_URL` was configured, so M0 did not fabricate a new PostgreSQL rerun. Against PERF-M4's historical c8 reference, current formal WSL throughput is about 0.64x PostgreSQL on W1, 0.54x on W2, 1.56x on W3, and 1.80x on W4. The comparison is directional because Go/toolchain and run length differ; the matched v0.2 control, not PostgreSQL, is the causal group-commit evidence.

## 28. Latency tradeoff

There is no coalescing timer. c1 p99 stays within 0.35 ms of v0.2. At c8, p99 falls 65-81% on ext4; at c32 it falls 66-90%. Throughput is therefore not purchased with an artificial latency window.

## 29. Correctness/race verification

`go test ./...` passes. `go test -race ./...` passes, including heavy distinct mutation and same-ID concurrency. Focused tests cover ordered cross-command visibility, durable rejection, operational error retry, panic propagation/coordinator survival, duplicate callback-at-most-once, pre/post-execution cancellation, Sync failure visibility barrier, staged-duplicate append failure, recovery after shared Sync while responses are blocked, and concurrent close/drain. Existing tests cover cross-dataset atomicity, snapshot/restart dedupe, incomplete tail truncation, and complete-frame corruption.

## 30. Compatibility/format result

No magic, envelope version, record schema, frame encoding, checksum, sequence rule, snapshot schema, or format marker changed. v0.2-created databases open and mutate through the same recovery path. A v0.2 reader sees only ordinary frames written with a different Sync cadence, so no format bump is warranted.

## 31. Architecture decision

**A. Opportunistic durability grouping fits the current single-authority architecture cleanly.** The necessary addition is a failure poison barrier over all operations, not a broad mutation overlay or engine rewrite.

## 32. Performance decision

**P2. Group commit provides a large improvement but another bottleneck remains.** It clears the Strong interpretation band on all four W1-W4 c8 lanes, yet W1/W2 remain directionally below the historical PostgreSQL reference and allocations rose modestly.

## 33. Dev-harness decision

**H1. <=60-second harness is sufficient for normal runtime iteration.** It completes in about 4 seconds on Windows and 12 seconds on WSL/ext4.

## 34. Remaining bottleneck

After Sync amortization, request/channel allocation plus JSON result/frame encoding and transaction bookkeeping are the next visible default-path costs. W1/W2's remaining PostgreSQL gap should not be attributed to Sync count: c8 commands/Sync is already 4.0 and scaling is now substantial.

## 35. Release recommendation

Recommend **v0.3.0**, not an immediate tag. The public API and on-disk format are compatible, but callback execution moves to a product coordinator and poison behavior intentionally becomes stricter for reads after durability failure. That observable error-path clarification deserves a minor pre-1.0 release and release-note review.

## 36. Exactly one next recommendation

Run a repeated, matched WSL/ext4 c8 profile of current W1/W2 versus a freshly configured PostgreSQL control, then use that single profile to choose the next default-path optimization (allocation/JSON encoding versus remaining storage work).
