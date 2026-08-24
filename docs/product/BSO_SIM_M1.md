# BSO-SIM-M1 — Agentic Transaction Scheduler Fabric

## 1. Verdict

**Meaningful progression.** M1 removes broad reconciliation, makes each transfer an explicit migratable record, preserves BSO-only financial authority, survives worker loss, uses typed/versioned Octagon protocol data, and keeps ordinary coordination proportional to the affected transfer set. It does not yet demonstrate increased wall-clock capacity from additional workers: the deterministic harness partitions agent steps but executes worker queues and durable calls serially.

## 2. M0 findings

M0 proved bilateral durability, replay safety, crash recovery, and conservation. Its faulted 1,000-BSO lane reached 6.21 messages/success and performed broad sweeps over every BSO to discover unresolved work. M0 also opened every configured BSO eagerly.

## 3. M1 architecture

| Property | M0 | M1 |
| --- | --- | --- |
| transfer protocol owner | BSO/orchestrator | `TransactionAgent` |
| reconciliation | broad sweeps | targeted agent recovery |
| financial authority | BSO | BSO |
| placement authority | implicit | `SchedulerCoordinator` |
| migration | none | explicit checkpoint record |
| protocol DTO | JSON/Go representation | typed/versioned Octagon |
| hot-path coordinator | orchestrator | worker/agent only after placement |
| scaling mechanism | more BSOs | more BSOs plus partitioned workers |

The implementation is in `experiments/BSOSim/M1`; `cmd/bso-sim-m1` runs bounded experiments. The companion Oct package is `oct/Experiments/BSOSim/M1`.

## 4. Authority separation

`durableBSO` alone changes balance, reservation, incoming/outgoing state, and audit. `TransactionAgent` chooses protocol actions but cannot access an OctetDB transaction. `SchedulerCoordinator` owns placement and migration but has no balance-mutating method. Tests additionally require zero outcome-changing duplicate applications after retries and migration.

## 5. TransactionAgent model

The checkpoint contains protocol version, transfer/sender/receiver IDs, amount, phase, retry/deadline counters, last observed participant versions, placement generation, and last message kind. It contains no BSO snapshot, goroutine, channel, closure, execution stack, or worker-local graph.

## 6. Agent state machine

The implemented phases are `Created → OfferReceiver → AwaitAccept → CommitSender → CommitReceiver → AwaitAcknowledge → Done`, with `Reconcile`, `Rejected`, and `Expired` branches. Waiting phases have logical deadlines. Duplicate or stale replies either leave the explicit phase unchanged or cause a two-participant durable-fact reconciliation. Oct tests caught and fixed a transition that initially conflated a duplicate external fact with an internal scheduled step.

## 7. SchedulerCoordinator

The coordinator registers workers, deterministically places new agents, records placement, kills a simulated worker, decodes checkpoints, and assigns replacements. It does not step agents or route protocol messages. The ordinary run measured 300 placement operations for 300 successes and zero hot-path coordinator messages.

## 8. SchedulerWorker

Each worker owns a map of explicit records and a ready queue. The simulation uses no goroutine per transaction. A bounded loop pops runnable IDs, performs one legal state-machine step, and requeues only runnable work. Waiting agents reside as data until a reply or deadline wakes them.

## 9. Placement policy

Placement is least-loaded live worker with worker ID as deterministic tie-break. This exposes load partitioning directly. Migration increments `PlacementGeneration`; it does not change `TransferID` or participant idempotency identity.

## 10. Octagon protocol DTOs

The machine codec emits one data-only Octagon record with `ProtocolEnvelopeV1` identity, protocol version, and a narrow payload schema identity: `TransferOfferV1`, `TransferAcceptV1`, `TransferCommitV1`, `TransferAcknowledgeV1`, or `TransferReconcileV1`. Checkpoints use `TransactionAgentCheckpointV1`; the Oct contract also declares `AgentPlacementV1`. Unknown versions, wrong top-level identity, missing/extra fields, duplicate fields, and kind/schema mismatch fail clearly.

## 11. Authentication binding

The deterministic authentication stub hashes canonical Octagon envelope bytes with the `Auth` field empty, then binds the sender identity secret. Validation re-encodes the typed payload; it never re-stringifies JSON.

## 12. Transport/fault model

Logical-time transport carries encoded Octagon bytes and deterministically injects drop, duplication, delay, and reorder. Worker loss is an independent scheduler event. No sockets, TLS, key custody, consensus, or service discovery were added.

## 13. Targeted reconciliation

There is no loop over all BSOs during progress or recovery. A timed-out agent loads only its named sender and receiver records and examines one unresolved entry. Ordinary 10/100/1,000/10,000-identity fault lanes examined zero reconciliation entries because stable replies converged before escalation.

## 14. Agent migration/reconstruction

Worker loss encodes `TransactionAgentCheckpointV1`, decodes a fresh record, and installs that record on a live worker. The simulation observation index is switched to the restored record immediately; a regression test guards against stale dead-worker pointers. If a checkpoint were lost, minimum reconstruction authority is the `TransferID` plus either participant's pending record: durable transfer records retain counterparty, amount, and local phase. Full checkpoint-loss reconstruction is documented but not implemented in M1.

## 15. Worker failure

In a 1,000-identity, 300-transfer, four-worker lane, killing worker 1 affected and migrated exactly 75 agents. The other 225 were not paused. Exact checkpoints required no BSO-wide rediscovery, all transfers converged, and audit counts found no double debit/credit.

## 16. BSO restart

Restart reopens one durable catalog, enumerates only that BSO's pending transfer IDs, and wakes corresponding current agents. The 10,000-identity affected-set lane restarted BSO 37 during its transfer with BSO 829: two databases were open, two BSOs were touched by recovery, one agent/reconciliation entry was examined, and unrelated BSOs remained untouched.

## 17. Scaling invariant

Ordinary bilateral work measured two semantic participants, about 4.11 messages, about 4.04 worker steps, and one coordinator placement per success independent of configured network size. Adding workers divided agent steps almost exactly. It did not increase wall throughput because the current deterministic driver serializes worker execution and synchronous durable calls.

## 18. Worker scaling

Measured on this Windows/NTFS host with 100 BSOs, 300 transfers, and the `fun` profile:

| Workers | Transfers | Throughput (wall transfers/s) | Avg agent steps/worker | Max | Coordinator ops/success | Correct |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 300 | 296 | 1,214.0 | 1,214 | 1.00 | yes |
| 2 | 300 | 292 | 607.0 | 609 | 1.00 | yes |
| 4 | 300 | 295 | 303.5 | 307 | 1.00 | yes |
| 8 | 300 | 297 | 151.8 | 155 | 1.00 | yes |

Load partitions; capacity does not yet rise. The isolated bottleneck is the single-threaded `runWorkers` driver around synchronous OctetDB mutations, not coordinator amplification.

## 19. BSO count scaling

| BSOs | Successes | Messages/success | Participants/success | Reconcile entries/success | Open databases | Wall ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 100 | 4.120 | 2.000 | 0.000 | 10 | 261 |
| 100 | 300 | 4.123 | 2.000 | 0.000 | 100 | 1,061 |
| 1,000 | 500 | 4.108 | 2.000 | 0.000 | 634 | 4,031 |
| 10,000 | 500 | 4.108 | 2.000 | 0.000 | 944 | 5,539 |

Work is bounded, while wall time grows with the number of distinct active durable participants and filesystem catalogs. Lazy activation prevents 10,000 configured identities from becoming 10,000 open databases.

## 20. Recovery locality

| BSOs | Messages/success | Participants/success | Reconcile entries/success | Unrelated BSOs touched |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 4.120 | 2.000 | 0.000 | 0 |
| 100 | 4.123 | 2.000 | 0.000 | 0 |
| 1,000 | 4.108 | 2.000 | 0.000 | 0 |
| 10,000 affected-set restart | 4.000 | 2.000 | 1.000 | 0 |

## 21. Hot merchant/payer

The 100-BSO hot-merchant lane completed 300/300 at 234 wall transfers/s. With a deliberately small 1,000-unit payer balance, the hot-payer lane completed 13 and durably rejected 287 without negative balance or conservation loss. These are workload hotspots at a chosen BSO; the scheduler does not pretend to remove them.

## 22. Group-commit interaction

Every participant mutation uses the real current `Database.Mutate` path. The deterministic worker driver currently admits those mutations serially, so this milestone does not claim commands/sync grouping evidence. That same serialization is the identified worker-scaling bottleneck and the next experiment should replace it with a bounded concurrent admission lane, not synthetic batches.

## 23. Octagon size/cost evidence

| DTO | JSON bytes baseline | Octagon bytes | Encode/decode result | Typed/versioned |
| --- | ---: | ---: | --- | --- |
| `TransferOfferV1` envelope | 259 | 293 | deterministic round trip | yes |
| `TransferAcceptV1` envelope | 262 | 296 | deterministic round trip | yes |
| `TransferCommitV1` envelope | 263 | 297 | deterministic round trip | yes |
| `TransferAcknowledgeV1` envelope | 278 | 312 | deterministic round trip | yes |
| `TransferReconcileV1` envelope | 270 | 304 | deterministic round trip | yes |
| `TransactionAgentCheckpointV1` | 275 | 313 | deterministic round trip | yes |

The text Octagon form is modestly larger here; its value is nominal schema, deterministic bytes, strict replay/authentication input, and cross-checking by Oct. In a 300-transfer fault lane, 1,250 decodes totaled about 5.03 ms (~4.0 μs/decode). Individual encodes were below the host clock's measurable interval, so no stronger encode-speed claim is made.

## 24. M0 comparison

| Measure | M0 | M1 |
| --- | --- | --- |
| messages/success at 1,000 identities, faulted | 6.21 | 4.108 |
| reconciliation | two broad sweeps in cited lane | zero ordinary entries; one entry in isolated restart |
| normal runtime | 27.321 s for M0's broader normal suite | 4.696 s for M1's four worker-count rows; not workload-equivalent |
| unresolved | 0 | 0 |
| money conservation | exact | exact |

Runtime is context only because suite composition and transfer counts differ. The comparable architectural improvement is elimination of population-wide discovery and lower fault-message amplification.

## 25. Global-control context

M0's neutral `GlobalLedgerControl` remains unchanged. M1 does not copy it or make the scheduler a ledger. The relevant comparison is M0 broad discovery versus M1 targeted agent recovery.

## 26. Correctness invariants

Tests verify exact conservation, no negative balance/reservation, zero double debit/credit, stable message and command identity, deterministic semantic digest, OctetDB close/reopen recovery, deterministic Octagon bytes, incompatible-version rejection, migration outcome preservation, zero hot-path coordinator messages, and affected-set locality. Scheduler placement never supplies a financial decision.

## 27. Runtime/resource observations

Smoke (four worker-count rows) remains under one second; normal remains under five seconds. A 10,000-identity/500-transfer row finished in 5.539 seconds with 944 active databases rather than 10,000. Active agent state is one compact record per transfer; ready queues contain IDs; execution stacks are bounded by the driver rather than agent count. No deep heap profile was run.

## 28. Architecture decision

**C. Explicit transaction agents improve recovery locality but do not improve execution scaling enough to justify the architecture yet.** The authority split is clean; the capacity claim remains unproved until workers execute durable admissions concurrently.

## 29. Scaling decision

**S3. Worker count does not materially increase capacity.** Work per worker falls almost exactly, but wall throughput is flat.

## 30. Locality decision

**L1. Recovery/reconciliation work is proportional to affected unresolved transfers rather than total network population.**

## 31. Octagon decision

**O1. Octagon is a clean typed protocol DTO format and should remain the BSO transport/checkpoint representation.** Its text representation is not smaller than JSON in this tiny sample.

## 32. Experiment decision

**E2. Architecture is promising, but optimize one identified scheduler/BSO bottleneck first.**

## 33. What this still does not model

No real network, crypto custody, scheduler metadata consensus/HA, multi-process execution, global financial authority, production timeout policy, checkpoint-loss reconstruction, concurrent worker admission, or hardware-normalized capacity is claimed. Placement persistence is bounded in-memory simulation state. The narrow Go Octagon decoder intentionally supports only the declared M1 DTO scalar/enum record surface.

## 34. Exactly one next recommendation

Add one deterministic bounded concurrent worker-admission lane—one goroutine per scheduler worker, never per agent—with race-safe outbound queues and metrics, then rerun the 1/2/4/8 curve while recording OctetDB commands/sync at hot BSOs if public instrumentation permits.
