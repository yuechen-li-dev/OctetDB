# DBSCHED-M2 — Conflict-aware deterministic request coordination

## 1. Verdict

**Success**

Typed M1 conflict information now changes dispatch through runtime conflict
tokens. The centralized E lane traded queueing for rejection: versus D it
completed 4.7% more requests and cut service p99 by 62.9%, while admitted
request p99 rose from 35.3 ms to 92.2 ms. F's utility policy and G's agentic
coordination were genuine, isolated experiments; neither improved runtime over
E. G did improve causal trace structure. PostgreSQL state was identical across
all seven correctness lanes in every aggregated run.

## 2. Thesis tested

- **Conflict-aware scheduling:** validated as a useful overload-control tradeoff,
  not an unconditional latency win. It prevents hot work from piling into pgx
  and PostgreSQL, admits more work than D, and makes database service predictable.
- **Utility-scored policy:** tested first with ranked utility and then, after the
  stronger probe was requested, with controller-bound `when policy`, hysteresis
  8, and minimum commit 3. Ranking worked; persistent policy memory did not cross
  the current per-decision generated-flow boundary. F added large allocation and
  policy-evaluation cost without a stable behavioral win.
- **Agentic mailbox/actuator coordination:** validated as a deterministic,
  bounded architecture and trace model. Runtime was approximately E plus small
  overhead; it was not a better scheduler in this workload.

## 3. Static vs runtime knowledge

M1 remains authoritative for the four commands, five statements, 4×4 batch
compatibility relation, priority, conflict class, transaction role, queue
capacity 128, maximum batch 8, and eight workers. M2 does not rediscover any of
that topology.

At runtime M2 supplies request identity, arrival/age, current state, and the key
projected from request data. Read-only transaction roles do not own a token.
`OrderWrite` projects M1's `Orders` conflict class to its `CustomerID` key;
`InventoryWrite` projects `Inventory` to `ProductID`. Dynamic ownership,
waiting, batching opportunity, completion, and failure are runtime facts.

## 4. Conflict model

The Go boundary uses the typed value `ConflictToken { Domain ConflictDomain;
Key int64 }`; it does not use strings. The closed domains are `None`, `Orders`,
and `Inventory`. A write owns at most one token. A request with an owned token is
ineligible for dispatch, while independent tokens and non-owning reads remain
eligible concurrently.

Ownership is acquired immediately before a job reaches a worker and released
only when the worker returns success or failure. Context cancellation still
feeds a completion through the worker and releases scheduler capacity and token
ownership. A second release is counted as an invariant violation. Because a
request can own only one scalar token and never waits while holding another,
multi-resource wait cycles are impossible by construction.

## 5. Workload

The deterministic PCG seed is `20260821`. All regimes retain unique order IDs,
stable timestamps, 100 customers, 20 products, and the final-state oracle.

- **Low:** the M1 mix over the full customer/product sets.
- **Mixed:** the M1 mix, with half of writes projected into four customers and
  two products.
- **Hot:** 65% writes (25% orders, 40% inventory), predominantly four customers
  and two products. This is a bounded small-tenant/limited-SKU hotspot, not a
  single-row synthetic impossibility.

The authoritative phases are 10 s low at 750/s, 10 s mixed at 1,500/s, 10 s
low before overload at 750/s, 8 s hot at 5,000/s, and 12 s low recovery at
750/s: 79,000 attempted operations per lane.

## 6. Centralized conflict scheduler

E keeps a bounded pending set and an ownership map inside one coordinator.
It sorts admitted requests by stable sequence, filters owned tokens, selects
the oldest legal request, gathers compatible reads up to the static maximum,
acquires ownership, dispatches an explicit job, and releases on worker
completion. Safety is a predicate, never a score.

The implementation records distinct blocked requests, wait/ownership duration,
hot-key depth, releases, double releases, and leaks. The three aggregated runs
had median 21,481 blocked requests, zero double releases, and zero ownership
leaks.

## 7. Utility-scored policy

F has four real candidates: `Dispatch`, `DeferForBatch`, `PromoteAgedRequest`,
and `JoinBatch`. Conflict safety and worker availability are eligibility checks
before Oct policy is invoked. Named considerations are priority, queue age, and
batch opportunity. Source-order tie breaking is deterministic.

The final Oct policy is controller-bound `when policy { hysteresis: 8;
min_commit: 3 }`. Separate persistent-flow probes prove that hysteresis retains
a current score-60 choice against a score-65 challenger, permits a score-80
challenger, and minimum commit retains the first choice against an immediate
score-100 challenger. In F itself, the Go boundary creates and completes one
flow per decision, so there is no prior choice on the next scheduler tick.
Consequently hysteresis/minimum commit are semantically inert in the live lane.
Making them live would require a persistent flow input/event boundary, not a
larger policy expression.

In the final paired runs F made a median 1,263,657 policy decisions (four
candidates each) for 79,000 attempts. F completed 255 fewer requests than E,
reduced throughput 0.3%, and raised p99 11.4%. It rose from 39.83 to 153.08
allocations/op and from 2,192 to 12,783 bytes/op. The full policy is therefore
classified as harmful at this integration boundary.

## 8. Agentic architecture

G gives each request the smallest useful explicit automaton:

```text
Created -> Admitted -> Ready
                      | conflict owned
                      v
               WaitingConflict --Wake--> Ready
                      |
Ready -> Dispatched -> Running -> Completed | Failed
```

Each agent has fixed history storage and a two-entry FIFO mailbox. The typed
messages are `Wake` and `Completion`; overflow is rejected and counted. The
actuator vocabulary is `AcquireConflict`, `Dispatch`, `ReleaseConflict`, and
`Complete`. Transition logic emits/records the intent; ordinary Go workers
perform pgx calls and return completion events to the coordinator.

The authoritative runs processed a median 102,862 messages with peak mailbox
occupancy 1 and zero overflow. Go channels transport jobs/completions, but typed
mailbox order and state transitions—not channel scheduling—define request
semantics.

## 9. Correctness

`go test ./... -count=1` covers:

- no concurrent dispatch for an identical token;
- concurrent execution for independent tokens;
- release on success, injected failure, and cancellation completion;
- no double release or ownership leak;
- bounded FIFO mailbox plus explicit overflow rejection;
- deterministic utility selection and source-stable request tie break;
- an aged low-priority request promoted over a new high-priority request;
- one-token cycle prevention;
- deterministic hot workload and unique write IDs.

Each benchmark run separately executed 500 logical operations through A, B, C,
D, E, F, and G after independent resets. All seven snapshots—including order
IDs, item counts, per-product inventory, and totals—were identical in runs 1,
2, and 4. No lane duplicated a write or changed transaction semantics.

The compiled Oct validation suite passes one positive Fact and four negative
contracts with zero interpreted fallbacks. It rejects missing runtime key
projection, mailbox capacity below two notifications, a conflicting write in a
read batch, and an illegal actuator. Exhaustive matches cover priority and
actuator enums.

## 10. Benchmark methodology

PostgreSQL 17.11 ran in the repository container; Go was 1.26.2, pgx was 5.7.6,
and the pool/static worker count was eight. Each lane reset and reseeded the
database, ran `VACUUM ANALYZE` and `CHECKPOINT`, executed 5,000 unmeasured warmup
operations, reset again, then ran the same phase sequence and seed. Tracing was
disabled in timed lanes. Ordinary Go GC remained enabled.

Runs 1, 2, and 4 rotate lane order and are aggregated by median. After the
final audit corrected F's early `JoinBatch` eligibility, E/F were rerun as
three direct pairs (`E,F`; `F,E`; `E,F`) and that paired median is authoritative
for F and the E->F ablation. Run 3 is
retained locally but excluded: after its first two lanes, both D and E collapsed
to about 970/s with roughly 30,000 rejections, and later conventional p99 rose
to 10.8 s with no measured recovery. Utility, run last, returned to 1,539/s.
That cross-lane discontinuity is an environment/order outlier, not a selective
M2 result. Generated JSON is ignored; reproduction commands and normalized
evidence are retained in Markdown.

## 11. Results

Medians of runs 1, 2, and 4:

| Lane | Completed | Rejected | Throughput/s | p50 ms | p99 ms | p99.9 ms | Queue p99 ms | Service p99 ms | Hot p99 ms | Recovery s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A conventional | 79,000 | 0 | 1,580.0 | 42.33 | 1,254.39 | 1,274.03 | 0.00 | 1,254.39 | 1,269.83 | 3 |
| B fixed batch | 73,961 | 5,039 | 1,479.3 | 2.31 | 35.85 | 40.39 | 32.78 | 6.26 | 37.46 | 2 |
| D static plan | 73,835 | 5,165 | 1,476.7 | 2.18 | 35.28 | 39.10 | 32.20 | 6.38 | 36.45 | 2 |
| E conflict | 77,275 | 1,725 | 1,545.5 | 2.14 | 92.18 | 101.94 | 91.14 | 2.37 | 95.67 | 2 |
| F policy | 77,345 | 1,655 | 1,546.8 | 2.57 | 100.93 | 110.77 | 99.84 | 2.23 | 104.85 | 2 |
| G agentic | 76,692 | 2,308 | 1,533.9 | 2.13 | 95.21 | 105.57 | 94.14 | 2.57 | 100.87 | 2 |

The conventional lane's service timer includes pgx pool acquisition and
database execution; its aggregate pool-wait duration was 25.8 million ms versus
single-digit milliseconds for bounded lanes. E moved the remaining delay out
of PostgreSQL/pgx service and into an explicit application conflict queue:
conflict-wait p99 was 97.8 ms (paired-run mean 38.8 ms) and hot-key depth
remained bounded. This is better
control and more completed work, but worse admitted-request tail than D.

Median allocation evidence:

| Lane | allocations/op | bytes/op |
|---|---:|---:|
| D | 37.35 | 2,309 |
| E | 39.86 | 2,197 |
| F | 153.08 | 12,783 |
| G | 40.51 | 2,214 |

## 12. Ablation

- **D -> E (conflict awareness):** +3,440 completed (+4.7%), 66.6% fewer
  rejections, +4.7% measured throughput, and 62.9% lower service p99. Total p99
  increased 161% because E queues hot owners rather than rejecting them.
- **E -> F (policy, paired median):** -255 completed (-0.3%), -0.3%
  throughput, +11.4% p99 (90.58 to 100.93 ms), and no service-tail benefit.
  Generated policy maps/flow instances caused 3.8× allocations/op and 5.8×
  bytes/op. Hysteresis/commit did not persist between decisions.
- **E -> G (agentic):** -583 completed (-0.75%), -0.75% throughput, +3.3% p99,
  and about +1.6% allocations/op. G adds explanation/state locality, not better
  scheduling behavior.

## 13. Authoring comparison

| Structural metric | E imperative | F policy | G agentic |
|---|---:|---:|---:|
| Request states | 0 explicit | 0 explicit | 8 |
| Coordinator transition cases | 4 | 4 | 4 plus agent transitions |
| Candidate actions | 1 oldest-legal rule | 4 | 1 oldest-legal rule |
| Scoring considerations | 0 | 3 | 0 |
| Conflict-safety branches | 3 | 3 | 3 |
| Mailbox message kinds | 0 | 0 | 2 |
| Actuator effects | implicit 4 | implicit 4 | explicit 4 |
| Global mutable scheduling groups | 3 | 3; no durable policy state | 3 |

E is **clearer** for authoritative safety and oldest-legal fairness. F is
**more complex**: the candidate list is readable, but controller memory cannot
survive the boundary and generated overhead is disproportionate. G is
**equivalent for policy, clearer for causality, and more complex overall**.
Request-local state replaces no global ownership structure because conflict
arbitration is necessarily shared.

## 14. Trace/replay evidence

Diagnostic traces are capped at 256 events and excluded from timed lanes. One
deterministic G lifecycle was:

```text
request 11 WaitingConflict: Inventory:2 owned by request 0
request 11 Wake: released_by=5
request 11 ConflictAcquired: Inventory:2
request 11 Dispatch: batch=1
request 11 ConflictReleased: Inventory:2
```

The releasing request can differ from the owner observed at first wait because
FIFO owners 0 and 5 complete before request 11 becomes next; the event chain
preserves that causality. A representative F decision was:

```text
request 6 eligible after conflict filter
candidates: Dispatch, DeferForBatch, PromoteAgedRequest, JoinBatch
considerations: priority=Interactive, age=502us, compatible_batch=8
winner: JoinBatch (deterministic source-order tie semantics)
dispatch: batch=8
```

## 15. Oct feature findings

| Capability | Finding |
|---|---|
| `when policy` | **Useful in a persistent flow probe; harmful in F.** Hysteresis/minimum commit work, but per-call Go integration discards state. |
| standalone `when utility` | **Neutral.** Natural one-shot ranking, superseded by the stronger controller probe. |
| flows / Octomata | **Useful.** Explicit policy state/history and controller-memory tests compile to ordinary Go. |
| record tables / artifact plan | **Useful.** M1 topology remains the closed legal substrate. |
| Concepts / `Require` | **Useful.** Four M2 invalid structures fail before Go generation. |
| enum / exhaustive match | **Useful.** Closed priority/action coverage and deterministic codes. |
| mailbox representation | **Neutral.** Fixed Oct-style records/arrays map cleanly to Go, but no native mailbox facility improved the design. |
| persistent external event input | **Blocked.** Generated flow parameters are fixed; F cannot feed successive scheduler observations into one live policy machine. |

The Go emitter also rejected mutable flow locals and `while` in the first
controller probes. Board fields plus a state self-transition expressed the same
bounded behavior and compiled cleanly; no compiler change was made.

## 16. Dominatus correspondence

M2 independently reproduces explicit automata, request-local state/history,
bounded FIFO mailboxes, actuator/effect separation, bounded admission,
semantic ownership, and deterministic wake causality. It does not reproduce
Dominatus's C implementation, memory/runtime model, broader multi-agent
topology, or any performance claim. There is no cgo, copied C, linkage, or
parity assertion.

## 17. Interpretation

| Idea | Classification | Reason |
|---|---|---|
| Conflict-aware application scheduling | **Useful** | More completed work and much lower service tail; explicit queue-tail tradeoff. |
| Utility scoring / full policy | **Harmful** | No stable behavior gain, non-persistent memory, large generated allocation cost. |
| Agentic request automata | **Neutral** | Better causal structure, small runtime cost, no control improvement. |
| Mailboxes | **Neutral** | Bounded and explainable, but centralized wake logic remains. |
| Actuator separation | **Useful** | Failure/cancellation release and trace boundaries are easy to test. |
| Static conflict topology | **Useful** | Closed legality and capacity remain compile-time inspectable. |
| Runtime conflict keys | **Useful** | Avoid coarse all-command serialization and expose real hot identities. |

## 18. Limitations

This is one Windows host, one PostgreSQL/container configuration, one pool
width, one schema, and three aggregated runs. Arrival timing is subject to
Windows timer scheduling. Bounded lanes reject at admission; p99 therefore
describes completed requests and must be read beside rejection/completion
counts. The `Orders:CustomerID` projection is an application declaration, not a
PostgreSQL lock prediction, and order inserts themselves create less physical
row contention than inventory updates. PostgreSQL lock tables were not sampled,
so service/pool metrics provide indirect rather than server-internal evidence.

G's mailbox is fixed and typed but backed by ordinary Go memory, and the shared
ownership map remains centralized. F's generated helper allocation dominates
its cost; another persistent integration could produce a different language
result. Trace samples are bounded diagnostics, not a full production replay
log. No unsafe memory, manual arena, GC disabling, LLM/agent runtime, MCP,
Dominatus C, or Oct compiler expansion was introduced.

## 19. Exactly one next recommendation

**Pursue priority/fairness specialization of the centralized E scheduler.**
E established that conflict-aware admission completes more work and removes
database-service amplification, but its 91 ms queue p99 is now the limiting
cost. The next experiment should keep E's imperative safety/ownership core and
test a compact age/priority/hot-key fairness rule without generated per-decision
flow allocation, using completion count, rejection rate, conflict-wait tail,
and independent-request latency as separate objectives.
