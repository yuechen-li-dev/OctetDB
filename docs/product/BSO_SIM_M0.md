# BSO-SIM-M0 — Bank-Shaped Object Distributed OLTP Simulation

## 1. Verdict

**Success.** The real OctetDB public mutation path runs deterministic 10/100/1,000-BSO simulations, survives loss/duplication/reordering and close/reopen crash points, reconciles partial transfers, conserves every integer unit, applies no debit or credit twice, exercises hot receiver/source workloads, and finishes the normal suite in 27.321 seconds on this host.

## 2. Purpose

This proof of concept models “being your own bank” as a distributed OLTP problem: independently durable participants coordinate value movement through a bilateral protocol. It compares that shape with a neutral `GlobalLedgerControl` in which every transfer enters one serialization domain. The result is educational architecture evidence, not a claim about any real financial system.

## 3. What a BSO is

A Bank-Shaped Object is the common participant abstraction for a person, merchant, company, institution, bank, or clearing participant. M0 deliberately gives all of them the same protocol implementation. Stable semantic IDs use `bso:000000`; filesystem routing is separate.

Each BSO owns its spendable balance, reservation total, incoming and outgoing transfer maps, seen-message IDs, protocol version, and local audit entries. Values are signed 64-bit integer smallest units (`Money int64`); floating point is never used for value state.

## 4. Non-goals

M0 is not a real bank, cryptocurrency, payment network, Visa/Stripe competitor, custody product, compliance system, production protocol, blockchain, consensus implementation, wallet, or key-management system. It implements no proof-of-work, proof-of-stake, mining, global chain ordering, token economics, sockets, or world transaction.

## 5. Local durable state

Every BSO owns a separate OctetDB directory and opens the ordinary `OpenCatalog → Bucket → Dataset → Database.Mutate` path. One readable `BSOState` record is atomically replaced per local transition. Stable command IDs are database-wide. Audit entries identify the transfer, terminal local state, and signed delta.

The simulation is based on OctetDB revision `512d4ddc7ab025f73bb5a58412f0e06ef88972f7`, which contains the successful GROUP-COMMIT-M0 coordinator. This is current main state for the experiment, not a v0.2.0 historical comparison. Every mutation therefore uses the current coordinator path, although deterministic single-event dispatch does not manufacture concurrent admissions solely to inflate grouping.

## 6. Transfer protocol

The sender durably reserves funds and sends `Offer`. The receiver durably records `Accepted` and returns `Accept`. The sender finalizes its debit, removes the reservation, records `Committed`, and sends `Commit`. The receiver credits exactly once, records `Committed`, and sends `Acknowledge`. The sender records `Acknowledged`.

There is no callback spanning BSO databases. The orchestrator only routes messages and checks invariants; reconciliation participants mutate only their own local truth. Rejection before sender commit releases the reservation. A bounded unresponsive reservation expires, releases locally, and sends an expiry reconciliation fact.

## 7. Authentication model

M0 uses a deterministic SHA-256 authenticated-envelope stub derived from the typed envelope and sender identity. It represents identity, message authenticity, integrity, and session trust only; it is not custody or consensus cryptography. Tests cover wrong sender, wrong receiver, tampered amount/authentication, replay, and an authentic unknown transfer. These cases change no value state.

## 8. Transport/fault model

The in-process transport owns an explicit logical-time event queue. A fixed-seed PRNG controls drop, duplicate, bounded delay, and a reorder window. The normal `fun` profile uses 1% drop, 2% duplicate, a 5% bounded-delay decision with maximum 3, and a 5% reorder decision with window 5. The `mean` lane uses 5% drop, 10% duplicate, maximum delay 5, and reorder window 9. There are no sleeps or real sockets.

## 9. Idempotency/replay model

One stable `TransferID` survives all retries and restarts. Each protocol kind/sender pair also has a stable `MessageID`. The receiver invokes `Database.Mutate` with that ID; exact replay returns OctetDB's durable original outcome without rerunning the transition, after which the simulator may resend the same response. Duplicate Offer, Accept, Commit, Acknowledge, and Reconcile tests are all green. Audit cardinality proves one debit and one credit application.

## 10. Crash/restart model

The crash schedule supports after reserve, after receiver accept, after sender commit, after receiver commit, and before acknowledgement. A crash closes the BSO database and immediately reopens its catalog/dataset from disk before delivery continues. The object retains only identity, path, and metrics across restart; balance and protocol truth are reloaded from OctetDB. The test lane triggers all five points and converges.

## 11. Reconciliation

At a logical round boundary, a sender resends Offer while reserved, Commit while committed, or its Expired fact after reservation timeout. A receiver resends Accept while accepted and Acknowledge after credit commit. A lost acknowledgement therefore causes a duplicate Commit/Acknowledge exchange, not another credit. Reconciliation never edits both sides and creates no central value authority.

## 12. Conservation invariant

M0 checks this equation after reservation and every delivery round:

```text
sum(spendable balances)
+ sum(sender reservations)
+ sum(sender-committed value not yet credited by a receiver)
= initial total
```

Local balances and reservations must also remain non-negative. At quiescence all reservations and committed-in-flight ownership are zero, so final spendable balances must equal the initial total. The digest covers sorted balances, sender/receiver terminal-state pairs, and unresolved IDs; it excludes time and performance.

## 13. Workloads

`random` chooses deterministic source/destination/amount triples. `hot-merchant` sends many payments to one destination. `hot-payer` makes one BSO reserve many outgoing transfers against a small balance, intentionally producing insufficient-funds rejections. `institution` generates a tiny institution-like settlement mix and is available as a bounded override. The normal suite runs random scaling plus the two hot lanes.

## 14. Global-ledger control

`GlobalLedgerControl` owns one OctetDB record containing all balances and stable transfer decisions. Each attempted transfer performs one ordered `Database.Mutate`, checks the sender, and atomically updates the two balances within that one database. It is a fair simple serialization control, not a blockchain or a stand-in for a named cryptocurrency.

## 15. Smoke benchmark

Command: `go run ./cmd/bso-sim --mode smoke`. Windows/NTFS runtime was 424 ms, including three scenarios.

| Scenario | BSOs | Transfers | BSO ops/s | Global ops/s | Messages/transfer | Unresolved | Conservation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| scale-10 | 10 | 50 | 519 | 1,919 | 5.22 | 0 | true |
| hot-merchant | 10 | 30 | 542 | 2,906 | 4.00 | 0 | true |
| hot-payer | 10 | 30 | 1,377 | 2,765 | 4.00 | 0 | true |

## 16. Normal benchmark

Command: `go run ./cmd/bso-sim --mode normal`. Windows/NTFS runtime was 27.321 seconds. Seed was `20260823`.

| BSOs | Transfers | BSO ops/s | Global ops/s | Messages/transfer | Unresolved | Conservation |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10 | 300 | 414 | 1,571 | 5.25 | 0 | true |
| 100 | 750 | 522 | 846 | 5.21 | 0 | true |
| 1,000 | 1,500 | 511 | 390 | 6.21 | 0 | true |

## 17. Fault-injection results

| Fault profile | Success | Retries | Duplicates suppressed | Reconciliations | Lost value | Double apply |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fun / 10 BSOs | 300 | 311 | 393 | 311 | 0 | 0 |
| fun / 100 BSOs | 750 | 768 | 943 | 768 | 0 | 0 |
| fun / 1,000 BSOs | 1,500 | 3,046 | 3,419 | 3,046 | 0 | 0 |
| mean / 100 BSOs | 400 | 846 | 1,323 | 846 | 0 | 0 |

Response loss is exercised probabilistically here and directly in tests. The all-crash-points test uses the mean profile, observes retries and suppressed duplicates, and produces the same logical digest on two independent runs.

## 18. Hot merchant/source results

The normal hot merchant lane completed 600/600 payments at 224 attempted transfers/s, four messages per success, zero unresolved, and conserved value. Its lower rate reflects repeatedly rewriting one receiver's growing local transfer history.

The hot payer lane attempted 1,200 transfers from a BSO holding 1,000 units. Eight succeeded and 1,192 were durably rejected without making the payer negative. It measured 680 attempted transfers/s, zero unresolved, and exact conservation. The deliberately small balance makes reservation contention visible rather than pretending every request can succeed.

## 19. Scaling shape

The BSO lane stayed roughly flat at 414/522/511 attempted transfers/s across 10/100/1,000 participants on this single-threaded deterministic dispatcher. The global control fell from 1,571 to 390 in the same three rows because its one growing JSON authority is rewritten and synchronized for every transfer. This is toy architecture shape only; dataset size, filesystem cache, JSON rewrite cost, and setup choices all affect it.

## 20. Coordination cost

The no-fault successful path uses four messages and five local durable mutations: reserve, receive Offer, receive Accept/finalize debit, receive Commit/apply credit, and receive Acknowledge. Fault lanes rose to 5.21–6.99 messages per success due to replay/reconciliation. Initial BSO records add one durable mutation per participant to reported totals. Exact replays are suppressed by OctetDB and do not append another decision. The control records exactly one global serialization operation per attempted transfer. The 1,000-BSO fun lane needed two broad reconciliation sweeps, illustrating that an untargeted scan/resend policy can amplify coordination work even while correctness remains intact.

Group-commit statistics are package-internal and therefore unavailable through the public experiment path. The current coordinator is used, but this deterministic event dispatcher ordinarily admits one message at a time; this benchmark does not claim group-size evidence.

## 21. What “being your own bank” actually requires

The simulation immediately exposes identity, replay, duplicate messages, fund reservation, crash recovery, timeouts, reconciliation, authentication/key failures, counterparty failure, audit, backups, and local risk/policy. Holding an integer balance is the easy record; maintaining explainable value through partial distributed failure is the system.

## 22. What the simulation does not model

M0 does not model fraud, AML/KYC, regulatory compliance, consumer protection, disputes or chargebacks, liquidity management, credit risk, FX, tax, settlement law, operational security, real key custody, real networking, real custody, loans, interest, merchant acquiring, card rails, or bank APIs. Successful M0 is only the beginning of “being your own bank.”

## 23. Oct/OctetDB architecture observations

**Architecture decision: B. BSO protocol works but reconciliation/state complexity is already substantial.** Stable bilateral state remains coherent, but even this tiny lane needs reservations, two durable views, replayed responses, expiry, and reconciliation.

**OctetDB decision: O1. Default OctetDB is a natural local BSO state engine.** The ordinary catalog API, atomic local callback, restart recovery, and exact keyed-command decisions directly express local authority and replay safety. No storage specialization was needed.

**Simulation decision: S1. The tiny harness is sufficient for continued protocol experimentation.** Logical time, explicit envelopes, deterministic faults, stable digests, and bounded restarts caught protocol windows without sockets or wall-clock sleeps.

The companion Oct contract at revision `ecdc49f959d60aa57bd8638e64887d67151a5e9c` expresses pure sender/receiver state transition rules. All three facts pass interpreted and compiled. Ordinary functions were clearer than FLOW for this six-state transition surface; durable transport orchestration remains in Go rather than embedding Oct source in the host.

## 24. Benchmark limitations

This is one Windows/NTFS run of an in-process event simulator using whole-record JSON state. Wall numbers include protocol mutations but exclude participant creation, and they are not hardware-normalized. Work is dispatched deterministically rather than as a goroutine per message. The BSO and control representations intentionally differ because they represent different ownership domains. Latency is logical rounds, not network time. No claims are made about consensus protocols, blockchains, card networks, payment processors, real banks, or world scale.

Raw human-readable evidence is under `experiments/BSOSim/M0/evidence`. Correctness comes from tests and deterministic state, not benchmark throughput.

## 25. Exactly one next recommendation

Add one bounded deterministic per-BSO admission-batch lane that preserves stable event order while allowing concurrent `Database.Mutate` calls at a hot participant, then record public-safe commands-per-sync evidence if OctetDB exposes it.
