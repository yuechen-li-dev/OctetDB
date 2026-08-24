# BSO-SIM-M2 — Bounded Concurrent Scheduler Workers

## 1. Verdict

**Success.** The M1 partitions now execute concurrently with one long-lived goroutine per live scheduler worker. The distributed-random normal matrix reached 2.08x speedup at four workers and 2.72x at eight while correctness, migration, protocol, coordinator, and locality invariants remained green.

## 2. M1 bottleneck

M1's `simulation.runWorkers` was the precise serialization point. One caller loop visited worker 0's queue budget, then worker 1, and so on. `step` synchronously waited for each participant `Database.Mutate`; transport delivery also synchronously performed participant mutations. Worker load was partitioned, but only one queue and durable call progressed at a time. This explains M1's approximately 292–297 transfers/s flat 1/2/4/8 curve.

## 3. M2 concurrency model

M2 creates exactly one long-lived goroutine for every configured `SchedulerWorker`. A logical-round barrier dispatches one bounded queue snapshot to every live worker concurrently and waits for those workers before advancing deterministic transport time. Workers block on their round channel when idle and stop through an explicit stop/done handshake. There is no goroutine per agent or message, no async abstraction, and no real-time protocol timer.

## 4. Worker ownership

Each `TransactionAgent` exists in exactly one worker's agent map. The coordinator's placement map records that worker; `TransferID` and `PlacementGeneration` survive checkpoint migration. Transport events carry worker and generation out of band. A worker rejects a stale-generation delivery rather than stepping an agent under the wrong placement. `TestSemanticDigestNormalizedAcrossWorkerCounts` and the migration tests cover this seam.

## 5. Queue design

Every worker owns one explicit queue containing agent-step or delivered-envelope work items. Coordinator placement adds initial work; deterministic transport delivery adds inbound work. There is no central ready queue and no work stealing. The observed random workload remained almost perfectly balanced: at eight workers, step counts were 504/505/505/504/506/503/504/504.

## 6. Transport synchronization

One mutex protects the bounded transport event queue, logical clock, and per-message attempt counters. Fault choices are deterministic hashes of seed, stable message ID, attempt, and decision kind, so goroutine arrival order does not assign randomness. Events sort by logical delivery time and stable order key. Delivery enqueues work on the recorded owning worker; financial mutation happens later on that worker.

## 7. Coordinator behavior

The coordinator still performs deterministic least-loaded-live-worker placement with worker-ID tie-break, observes failure, checkpoints the failed worker's agents, and reassigns only those agents. Ordinary steps and message routing do not consult it. Measured hot-path coordinator messages remained 0; the random normal run used 1.00 coordinator operation per success.

## 8. BSO concurrent admission

Workers naturally call `Database.Mutate` concurrently against independent and shared BSO databases. The simulator adds no BSO serialization. `TestConcurrentSameBSOAdmissionIsSafe` exercises eight workers against hot-merchant and insufficient-funds hot-payer workloads under drop/duplicate/delay/reorder faults. Conservation, non-negative value, exact idempotency, and zero double debit/credit remain green.

## 9. Group-commit interaction

Concurrent admissions can naturally overlap at a hot BSO. Commands/sync and group-size histograms are **N/A**: the available group-commit counters are internal product instrumentation, and M2 deliberately did not widen the public OctetDB API or add a simulator-only synthetic batching path.

| Workload | Workers | Hot BSO? | Commands/sync | Median group size | Max |
| --- | ---: | --- | ---: | ---: | ---: |
| hot merchant | 1/2/4/8 | yes | N/A | N/A | N/A |
| hot payer | 1/2/4/8 | yes | N/A | N/A | N/A |

## 10. Worker-loss behavior

With four workers and 1,000 transfers, killing worker 1 at logical round 3 migrated exactly its 250 agents. The other 750 agents were not migrated, `UnrelatedAgentsPaused` remained 0, and all 1,000 transfers converged with conservation and no duplicate financial application. The bounded run completed at 130 transfers/s; instantaneous disruption is not separately instrumented, so no unsupported pause-duration claim is made.

## 11. BSO-restart locality

The 10,000-configured-BSO control restarted BSO 37 for the transfer involving BSO 829. It opened two databases, examined one reconciliation entry, touched one affected agent, touched exactly two recovery BSOs, and touched zero unrelated BSOs. Concurrency did not broaden the affected set.

## 12. Goroutine-count evidence

Worker lifecycle instrumentation recorded exact starts and stops. For the 100-BSO normal controls, one worker changed the observed goroutine count from 101 idle to 102 peak and eight workers changed it from 101 to 109; shutdown returned to 101. The 101 baseline includes the existing OctetDB commit coordinator for each of 100 open BSO databases. The relevant simulator delta is exactly the configured worker count and is independent of 1,000 active transfers. Tests also assert exact worker start/stop counts and cancellation without leakage.

## 13. Random workload scaling

The primary normal workload used 100 BSOs, 1,000 distributed-random transfers, deterministic seed 20260823, and the `fun` transport profile. Setup and all four runs completed in 6.68 seconds. This is the workload with the best independence and it materially scaled.

## 14. Hot merchant

With one shared destination authority and no transport faults, throughput rose only from 185/s to 249/s at eight workers. This honest 1.34x ceiling is consistent with real serialization at the merchant BSO; M2 did not disguise it with scheduler work stealing or synthetic batches.

| Workers | Throughput | Speedup | Correct |
| ---: | ---: | ---: | --- |
| 1 | 185/s | 1.00x | yes |
| 2 | 202/s | 1.09x | yes |
| 4 | 230/s | 1.24x | yes |
| 8 | 249/s | 1.34x | yes |

## 15. Hot payer

The insufficient-funds source hotspot remained almost flat, as expected for one serialized financial authority: 127/s at one worker and 139/s at eight. Correctness, rather than scheduler scaling, is the result of this workload.

| Workers | Throughput | Speedup | Correct |
| ---: | ---: | ---: | --- |
| 1 | 127/s | 1.00x | yes |
| 2 | 129/s | 1.02x | yes |
| 4 | 138/s | 1.09x | yes |
| 8 | 139/s | 1.09x | yes |

## 16. 1/2/4/8 worker table

Independent random workload, 100 BSOs, 1,000 transfers, `fun` faults. Per-transfer latency percentiles are N/A because completion timestamps were not added solely for this experiment.

| Workers | Transfers | Throughput | p95/p99 | Avg steps/worker | Max steps/worker | Correct |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1,000 | 421/s | N/A | 4,035.0 | 4,035 | yes |
| 2 | 1,000 | 613/s | N/A | 2,017.5 | 2,019 | yes |
| 4 | 1,000 | 876/s | N/A | 1,008.8 | 1,010 | yes |
| 8 | 1,000 | 1,145/s | N/A | 504.4 | 506 | yes |

## 17. Speedup/efficiency

| Workers | Speedup | Efficiency |
| ---: | ---: | ---: |
| 1 | 1.00x | 100% |
| 2 | 1.46x | 73% |
| 4 | 2.08x | 52% |
| 8 | 2.72x | 34% |

The causal classification is **Meaningful** (at least 1.5x at four workers), not Strong (less than 2.5x at four).

## 18. Locality metrics

For the random normal control: 4.087 messages/success, 2.0 participants/success, 0 reconciliation entries/success, and 0 unrelated BSOs touched by recovery. The targeted restart control measured 4.0 messages/success, 2.0 participants/success, 1.0 reconciliation entry/success, and zero unrelated BSOs.

## 19. Coordinator metrics

Random normal: 1.00 coordinator op/success and 0 hot-path coordinator messages. Worker loss: 1.501 coordinator ops/success due to 250 checkpoint reassignments and still 0 hot-path coordinator messages.

## 20. Race/correctness verification

`go test -race ./experiments/BSOSim/M2 -count=1` passes. The suite verifies Octagon round-trip/version rejection, fault convergence, same-BSO races, deterministic normalized terminal digest across one and eight workers, exact worker ownership generation, worker loss after outbound activity, targeted restart locality, normal shutdown, and cancellation. All recorded runs have zero lost value, zero double debit, zero double credit, no negative balance/reservation, no unresolved transfer, and stable command idempotency.

## 21. M1 comparison

M1's same conceptual curve partitioned work evenly but executed the queues serially and stayed approximately 292–297 transfers/s across 1/2/4/8 workers. M2 makes that execution variable real. Its measured random `fun` curve was 421/613/876/1,145 transfers/s. These absolute values come from the current M2 100-BSO/1,000-transfer run; the causal comparison is labeled conceptual because the recorded M1 report used its own bounded configuration.

## 22. Remaining bottleneck

Random scaling is sublinear, but it is not flat, so M2 did not perform the conditional profile or optimize another subsystem. The hot-merchant and hot-payer controls clearly show intended participant-authority serialization. No measured worker imbalance dominates: eight-worker random steps differ by only three steps.

## 23. Architecture decision

**A. One-goroutine-per-worker concurrency validates the scheduler-fabric architecture.**

## 24. Scaling decision

**S2. Aggregate throughput increases sublinearly but clearly.**

## 25. Concurrency decision

**C1. Plain goroutines + explicit worker queues are sufficient for this architecture stage.**

## 26. Next-optimization decision

**N1. Stop scheduler optimization; return to BSO protocol/application experiments.**

## 27. Exactly one next recommendation

Return to the next bounded BSO protocol/application experiment; do not add work stealing, batching, or an async scheduler layer without new evidence.

## Reproduction

```powershell
go run ./cmd/bso-sim-m2 --mode normal
go run ./cmd/bso-sim-m2 --mode normal --workload hot-merchant --fault-profile none
go run ./cmd/bso-sim-m2 --mode normal --workload hot-payer --fault-profile none
go test -race ./experiments/BSOSim/M2 -count=1
```
