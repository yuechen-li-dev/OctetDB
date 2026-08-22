# DBSCHED-M7 — OctetDB Write M0

## 1. Verdict

Meaningful progression

M7 removed the largest initial uncertainty: current Oct can own nominal command, effect, decision, and static conflict-shape logic, and compiled flows retain deterministic per-instance control state and declared board values across suspension. Two positive compiled contracts and four negative contracts pass with zero interpreter fallback.

The next blocker is foundational and precisely isolated. A declared Oct board is flow-instance-local memory; it is neither a shared blackboard nor an addressable authoritative store. A flow accepts inputs only when constructed, `Step` accepts no message, and compiled flows expose no checkpoint/restore or supported Go embedding seam. Building scheduling, durability, and PostgreSQL controls now would put the real state and mailbox in handwritten Go while merely calling an Oct decision helper. That would not test the requested hypothesis, so M7 stops before creating that misleading architecture.

This is not a verdict that durable state-machine databases are incoherent. It is evidence that current Octomata `flow` + declared `board` do not yet supply the agent/blackboard boundary assumed by the hypothesis.

## 2. Research question

> What mutable database/runtime architecture naturally emerges from Oct flow + board + explicit transitions?

The present answer is: no defensible write-runtime architecture emerges without first adding an explicit runtime ownership and input boundary. Oct already describes behavior well, but its board is local control memory, not database state.

## 3. Existing Oct reconstruction

### Flow

A compiled flow instance is a generated pointer-backed Go struct containing:

- `started`, `completed`, `currentState`, and `instruction`;
- a typed result plus `hasResult`;
- construction parameters retained as fields;
- one anonymous typed board struct when declared;
- optional history, one resume slot, and utility-policy site state only when used.

Construction allocates a new independent instance. Multiple instances of one flow therefore work, but Oct has no keyed identity registry; a host would have to retain `AccountID -> instance`. Parameters are frozen at construction. `Step(flow)` has exactly one argument and advances until the next `suspend` or `return`, possibly crossing several `goto` transitions. One scheduler step is therefore not intrinsically one database transition.

The control structure is not a pushdown stack. `remember`/`resume` use one overwriteable slot; successful resume clears it. State history is optional observability storage, not recovery authority.

Compiled state and board accesses lower to direct struct fields and switch-based instruction dispatch. A resident scalar-policy flow measured a median 9.182 ns/step and 0 allocations, versus 1.936 ns for its competent handwritten Go control. This is about 4.74x instruction overhead but only 7.25 ns absolute. It is not evidence that state machines are generally slow.

Flow recovery exists only in the interpreter-oriented checkpoint API used by Make flows. Version 1 supports incomplete suspended flows, but restore rejects any flow parameters, non-`Int` return shapes outside its Make subset, state locals, and utility-policy state. Completed flow checkpointing is unsupported. Generated compiled flow types contain the necessary simple fields but expose no checkpoint/restore API.

Compile-time material consists of declared states, transitions, field types, and expression structure. Runtime state consists of the flags/cursor, retained parameters, board values, optional resume/history/policy memory, and result.

### Board

The authoritative reference calls a declared board fixed-shape, flow-local control memory and explicitly advises against general mutable application state. Current typechecking accepts `Bool`, `String`, `Int`, `Float`, dimensioned numeric scalars, and arrays recursively composed from those scalar types. It rejects records, enums, vectors, matrices, and other nominal runtime types.

This differs from the same reference section's `BoardSnapshot` text, which says only scalar fields are supported. The compiled M7 probe proves an `Int[]` board field and typed array snapshot work. This is a documentation gap.

Boards default-initialize and have no constructor/initializer surface. Reads and writes lower to direct fields on the owning flow instance. Array element assignment is supported. `BoardSnapshot` returns a typed read-only value to Oct; generated Go currently copies slice headers rather than cloning backing arrays.

There is no board identity, address/key type, shared board instance, ownership declaration, concurrency control, transaction view, atomic multi-board mutation, or public external mutation API. Two flow instances cannot coordinate through one declared board. Determinism holds for one sequential instance, not for concurrent shared access because shared access does not exist.

An older compatibility surface permits a nominal record parameter literally named `board` to be field-mutated inside a flow. M7 did not use it as authoritative state: it is construction-time by-value data, is not a declared board, and would obscure rather than resolve the ownership problem.

### Messages, events, effects, and errors

Octomata has no typed mailbox or cross-instance send operation. UI events are a separate UIBridge concern and do not inject flow inputs. `Step(agent, message)` is rejected for arity, and `Send` is undefined.

There is no first-class effect set. Ordinary nominal enums and records work well as bounded returned intent values; M7 uses `EffectKind` and `TransitionDecision`. Fallibility is explicit for ordinary functions, but compiled flow expressions reject some fallible-call shapes, and a flow itself does not declare a transition-level commit failure protocol.

### Static validation and generated Go

Enums and records cleanly express closed command/effect spaces. `ShapeOf(CommandKind)` cleanly expresses static command-to-resource cardinality and maximum effect count. `Concept` refinements can validate scalar inputs. `Require` is intentionally bounded compile-time evaluation and cannot prove arbitrary runtime access topology. `StaticAssert` belongs to artifact/build-time validation, not transaction admission.

`internal/build.EmitGoSource` and OctGen are compiler-internal Go packages. The CLI provides real compiled test/build paths, but generated flow constructors and methods are unexported implementation details. There is no supported `Banking.oct -> importable application Go package` contract for a host runtime.

## 4. Architecture candidates considered

| Candidate | Evidence | M0 disposition |
|---|---|---|
| Many keyed persistent flows sharing one declared board | Best match for the hypothesis, but shared/addressable boards and message injection do not exist | Blocked |
| One global coordinator flow with all accounts in array board fields | Arrays work, but inputs are frozen, nominal status is rejected, capacity is awkward, and one flow serializes all work | Harmful |
| Ephemeral flow per command returning typed effects; Go owns a map | Compiles and is easy to build | Rejected because Go, not board, would be authoritative |
| Legacy record parameter named `board` | Can mutate a copied record within one flow | Rejected as a compatibility loophole, not declared board semantics |
| Direct board mutation as commit | No external atomicity/durability boundary | Blocked |
| Effect intent returned to a Go actuator | Decision boundary is elegant | Promising, but no supported persistent-agent integration seam |

No final runtime candidate was selected. The least misleading next architecture remains keyed flows plus explicit message input and commit-returned effects, but that statement is a language-pass target, not an implemented M0 claim.

## 5. Final M0 architecture

The only implemented architecture is the bounded executable seam probe:

```text
nominal command kind
  -> static typed access/effect shape
  -> construct compiled flow with immutable inputs
  -> direct flow-local board evaluation
  -> typed TransitionDecision
```

The requested architecture breaks at the marked boundaries:

```text
commands
  -X-> persistent keyed flow inbox       (no input/send surface)
       -> flow-local declared board
  -X-> shared authoritative blackboard   (no identity/share/ownership surface)
       -> typed effect decision
  -X-> supported host commit integration (generated API is internal)
  -X-> compiled recovery                 (interpreter-only restricted checkpoint)
```

## 6. `.oct` behavioral source

The real source is [`behavioral_source.octest`](../../experiments/M7/reconnaissance/behavioral_source.octest). It owns:

- nominal `CommandKind`, `AccountStatus`, and `EffectKind`;
- a typed `CommandShape` function;
- a typed withdrawal decision flow;
- a two-step transfer workflow probe;
- board state and bounded effect-code arrays;
- positive correctness contracts.

For example:

```oct
fn ShapeOf(kind: CommandKind) -> CommandShape {
    return match kind {
        case CommandKind.Transfer => CommandShape {
            Kind: kind AccountWrites: 2 LedgerAppends: 1 MaxEffects: 4
        }
        // closed remaining cases
    }
}
```

Oct owns behavior and closed topology in this probe. The Oct compiler owns generated state structs and dispatch. No Go runtime was added because it would necessarily own the missing semantics.

## 7. Board model

What is mutable: only predeclared scalar/array fields within one flow instance. Who may access it: the owning flow writes it; Oct callers may request a typed snapshot. Addressing: field names and array indexes, not entity/resource keys. Snapshotting: `BoardSnapshot` at runtime, or restricted interpreter checkpoint export. Mutation authority and sharing are not represented.

This is useful controller memory and inadequate authoritative database state.

## 8. Agent/flow model

Identity is host-object identity only. Lifetime is as long as the generated pointer is retained. There is no mailbox. State is a state ID, instruction cursor, result flags, retained construction parameters, local board, and feature-specific memory. The stack is a single resume slot. A transition is whatever work one `Step` performs before suspension/completion. Recovery is not available for compiled instances.

Millions of instances are theoretically ordinary heap objects, but no registry, eviction, snapshot, restoration, or capacity control exists. M7's escaping `DecideWithdrawal` instance is 224 B/op and one allocation in the measured harness; this is a generated-layout result, not a population-scale measurement.

## 9. Effect/actuator model

Returning a nominal decision record is the cleanest current separation:

```text
immutable observed values + command
  -> flow decision
  -> TransitionDecision / EffectKind
  -> future actuator
```

It avoids hidden host effects. However, Oct cannot currently bind the observed values to a shared authoritative board view or bind the result to one atomic host commit. The effect representation itself is not the blocker; the ownership and integration seams are.

## 10. Conflict scheduling

`ShapeOf` proves that closed static conflict shape is natural in ordinary Oct. `Transfer` declares two account writes, one ledger append, and four maximum effects. Runtime conflict identity, canonical ordering, acquisition, and release were not implemented because no authoritative board mutation follows acquisition.

A future runtime should compile command kind to typed token constructors and supply only concrete account IDs dynamically. M7 intentionally did not add a generic string lock manager.

## 11. Durability model

No transition log, fsync mode, checkpoint, or replay implementation was added. Doing so would durably record Go-owned state changes and thereby answer a different question.

Existing Oct interpreter checkpointing is not a substitute:

- it checkpoints suspended incomplete interpreter instances;
- it rejects parameterized flows, which excludes command-bearing agents;
- it rejects state locals and utility state;
- it requires a narrow return shape;
- it has no compiled-flow equivalent.

Therefore no commit point or crash-consistent atomicity claim is made.

## 12. Correctness

Compiled, no-fallback evidence passes six contracts:

| Contract | Result |
|---|---|
| Typed transfer conflict/effect bounds | Pass |
| Accepted withdrawal emits 100 -> 60 | Pass |
| Frozen withdrawal emits rejection and 100 -> 100 | Pass |
| Board persists across a suspend | Pass |
| Nominal record/enum authoritative board fields rejected | Pass |
| Post-construction `Step(..., message)` and `Send` unavailable | Pass |

Conservation under concurrent transfer, no partial commit, freeze race, deadlock freedom, duplicates, mailbox overflow, replay, and restart are not tested because the necessary authoritative runtime boundary is absent.

## 13. Performance

No PostgreSQL/Go/OctetDB A/B/C throughput comparison is reported. There is no C implementation with comparable semantics, so producing A and B would create numbers without a motivating comparison.

The only retained micro-measurements are implementation-path diagnostics:

| Probe | Median / result | Allocations |
|---|---:|---:|
| Resident compiled Oct scalar-policy step | 9.182 ns | 0 B, 0 allocs |
| Resident handwritten policy step | 1.936 ns | 0 B, 0 allocs |
| Escaping M7 flow instantiation | 40.03 ns | 224 B, 1 alloc |
| Escaping handwritten three-field object | 16.35 ns | 48 B, 1 alloc |
| M7 instantiate + complete withdrawal step | 52.86 ns | 232 B, 2 allocs |
| Escaping board snapshot | 19.61 ns | 48 B, 1 alloc |

Five samples were used except the board snapshot (one confirmation sample). Hardware was an AMD Ryzen 7 7700X on Windows/amd64 with Go 1.26.2. These are nanosecond-scale lowering costs, not durability or concurrency measurements.

## 14. Contention

Cold, mixed, hot-key, many-to-one, and cross-key workloads were not run. Current flows have no shared resource ownership, so contention would exist only in a handwritten Go layer and would not pressure-test Oct's semantics.

## 15. Flow/board overhead

The resident policy result shows the state-machine dispatch overhead is real but small in absolute terms and allocation-free. The M7 flow object is larger because it retains result, inputs, board fields, and slice headers. Board reads/writes lower to direct fields. Snapshot extraction allocates when its typed value escapes through `any`.

One compiler pathology was found: `Append(board.EffectCodes, value)` type-checks but compiled flow lowering reports `compiled mode does not yet support builtin Append`. Fixed array literals compile. This is an avoidable generated-code/compiler gap, not a fundamental state-machine cost.

## 16. Memory behavior

The measured escaping instance uses 224 B versus 48 B for the minimal handwritten control. Persistent step execution allocates nothing. The result does not establish population-scale memory behavior; it only shows that topology specialization does not automatically erase per-instance metadata or escape-driven heap allocation.

No unsafe, cgo, arena, manual allocator, or custom memory manager was introduced.

## 17. Recovery

Snapshot load, replay throughput, time-to-ready, truncation, corruption, and hash equivalence remain unmeasured. The blocker is not log framing; it is the absence of a serializable/restorable compiled agent plus authoritative board contract. Implementing a log first would cement the wrong state owner.

## 18. Octagon publication seam

Nominal transition decisions and command shapes are Octagon-compatible data in principle, but no stable committed Write state exists to publish. Consequently M7 does not claim a canonical state hash or Write-to-Read publication seam. The existing M6 publication machinery remains usable once a real commit authority can produce canonical nominal state.

## 19. Architecture verdicts

| Component | Verdict | Reason |
|---|---|---|
| Flow as persistent agent | Needs redesign | Persistent state exists; post-construction input, identity, and compiled restore do not |
| Board as blackboard | Harmful for authoritative state | Explicitly local control memory with no sharing/ownership |
| Typed command/effect values | Useful | Nominal enums/records compile cleanly |
| Mailboxes | Needs redesign | No surface exists |
| Effects returned from transition | Useful | Clean decision/actuation split, subject to integration seam |
| Central scheduler | Inconclusive | Static shape is clean; runtime authority could not be tested |
| Static conflict topology | Useful | Closed match-based description is natural |
| Transition log | Inconclusive | No valid authoritative commit to log |
| Checkpoint/replay | Needs redesign | Interpreter-only restricted flow checkpoints |
| One global flow | Harmful | Serializes independent keys and encodes nominal data as scalar arrays |

## 20. Oct pressure findings

| Symptom | Workaround used | Root cause / scope | Classification | Future abstraction candidate |
|---|---|---|---|---|
| Flow cannot receive a second command | Freeze input at construction in seam probe | General flow call model | missing agent/message abstraction | typed `Step(flow, message)` or inbox delivery contract |
| No bounded mailbox, overflow, correlation, or addressing | None; negative contracts retained | General | missing agent/message abstraction | typed bounded mailbox attached to flow identity |
| Declared board cannot be shared | None | Board is per-instance by design | missing board abstraction | explicit board instance/handle and sharing policy |
| Record and enum board fields rejected | Integer codes/parallel scalar arrays only in probe | General type restriction | missing board abstraction | nominal board fields or typed keyed collections |
| No board initialization | Copy construction parameters into fields on first step | General | missing board abstraction | typed constructor/restore initializer |
| No visible ownership/concurrency semantics | None | General | missing board abstraction | declared read/write authority and typed resource identity |
| One `Step` may cross multiple `goto`s | Treat suspend/return as observable boundary | General semantics | missing flow abstraction | explicit transition/commit yield boundary |
| Resume is one slot, not a stack | Do not model nested workflow | General | missing flow abstraction | only add a bounded stack if real workflow evidence requires it |
| Compiled flow cannot checkpoint/restore | None | Compiled/runtime parity gap | Go runtime integration gap | versioned generated state codec |
| Interpreter checkpoint rejects parameters | None | Make-oriented H1/H2 subset | Go runtime integration gap | serialize typed parameters or separate inbox from persistent state |
| Generated flow API is unexported/internal | Use CLI test path only | General integration gap | Go runtime integration gap | supported generated package/runtime ABI |
| `Append` type-checks but compiled flow rejects it | Fixed array literals | Backend coverage gap | missing static-analysis/compiler support | lower bounded array helpers in flows |
| Reference says scalar snapshot; compiler accepts arrays | Trust compiler contracts and retain probe | General documentation inconsistency | Oct bug | align reference and implementation deliberately |
| Effect arrays need codes because enum arrays cannot be board fields | Typed effect returned separately | General | missing effect abstraction | bounded nominal effect collection returned from transition |
| Atomic commit cannot be expressed | Stop before Go-owned mutation | General | missing effect abstraction | transition result plus actuator-owned atomic commit protocol |
| `Require` cannot derive dynamic conflict identity | Ordinary match describes static cardinality | Correct phase boundary | not worth solving | keep runtime identities dynamic |
| Legacy record parameter named `board` mutates in flows | Deliberately unused | Compatibility inconsistency | Oct bug | remove/clarify after migration evidence |

The suggested abstractions are evidence targets, not implemented language changes.

## 21. What should OctetDB Write actually be?

1. It may be a state-machine command processor, but current evidence does not support calling it an actor database.
2. It should not be a durable runtime over today's flow-local board.
3. A transition log remains attractive only after the commit authority is defined.
4. The strongest current description is “typed command processor with explicit future state/effect intents.”
5. Keyed agents are more coherent than one global flow, provided inputs and restoration become real.
6. Authoritative mutation should occur through committed effects, not hidden direct board writes.
7. Mailboxes are required for long-lived agents but should stay bounded and typed; they need not become a general actor graph.
8. Command/effect types, state topology, conflict shape, capacity bounds, and transition logic should be compiled.
9. Concrete keys, current values, contention, commit sequence, and external-effect results must remain dynamic.
10. SQL, generic row mutation, MVCC, actor-network topology, distributed consensus, and a general event-sourcing framework should remain outside this product hypothesis.

## 22. Relationship to OctetDB Read

The intended seam remains:

```text
committed Write authority
  -> stable nominal state
  -> canonical Octagon + logical hash
  -> existing compiled Read publication
```

M7 validates only that nominal behavioral data fits Oct's type system. It does not produce committed authoritative state and therefore does not demonstrate the seam end to end.

## 23. Memory-management interpretation

Explicit generated topology yields a zero-allocation resident step without manual memory management. Handwritten Go is about 7 ns faster for the isolated policy, while escaping compiled instances carry 176 additional bytes over the tiny handwritten object. On this evidence, representation and dispatch specialization matter more than allocator choice for resident steps; object lifetime/layout matter for large populations. Nothing here justifies unsafe code or manual allocation, and nothing here generalizes to durable throughput.

## 24. Remaining limitations

- No authoritative shared state, scheduler, atomic commit, log, recovery, mailbox, external actuator, or publication snapshot was built.
- No PostgreSQL or conventional Go end-to-end control was measured.
- No concurrency, contention, crash, truncation, corruption, duplicate, or replay workload ran.
- Flow microbenchmarks use generated internals and one existing policy specimen, not the missing application integration path.
- Generated test source is not retained because the Oct reference explicitly marks that layout non-public; its SHA-256 and structural observations are retained instead.
- The bounded compiler MCP named by the Oct skills was unavailable; all source was checked through repository revision `319b07a...` using the real `gooct-cli` compiled path.

These are consequences of stopping at the foundational boundary, not deferred claims.

## 25. Exactly one next recommendation

Perform a dedicated Oct flow/board/agent language and compiler pass before continuing OctetDB Write.

That pass should be driven by these exact blockers: typed post-construction input, bounded mailbox semantics, explicit keyed identity, shareable/authoritative typed board state or an intentionally separate state-store abstraction, a one-transition/one-effect-result boundary, and compiled checkpoint/restore plus a supported generated-Go integration contract. Only after those seams exist should M7 resume with the append log, scheduler, Go/PostgreSQL controls, contention workloads, and Write-to-Read publication.

## Reproduction

From the Oct repository:

```powershell
go run ./cmd/oct test `
  C:\Users\yuech\source\repos\Database-Scheduler\experiments\M7\reconnaissance `
  --execution compiled --json

$env:OCT_FLOW_SPECIALIZATION_BENCH = '1'
go test ./internal/build -tags=integration `
  -run '^TestPersistentPolicyFlow(Benchmark|GeneratedStructure)$' -v -count=1
```

Normalized evidence is in [`experiments/M7/summary.json`](../../experiments/M7/summary.json) and [`experiments/M7/evidence`](../../experiments/M7/evidence).

---

# DBSCHED-M7 resumed after FLOW-TURN-M1

This section preserves the initial stop above as historical evidence. It records the resumed implementation against Oct commit `309da01b60ec0f7917d4fd5efd1707bd71d2d40f`.

## 1. Final verdict

Success

Generated Oct flows now own real command admission, transition arbitration, workflow memory, and typed decisions. Go owns keyed queues, concrete conflict identity, authoritative accounts, WAL commit, checkpoint durability, recovery, and publication. The full path runs without Oct source or compiler symbols at runtime, survives restart/truncated tails, stops on corruption, and has competent Go and PostgreSQL controls.

## 2. Lineage

Initial M7 correctly stopped because Oct had no typed turn input, exported facade, or compiled restore. FLOW-TURN-M0/M1 fixed those seams generally. Resumed M7 rebuilt the specimen first, retained the corrected private-board model, and then implemented the host runtime. The Database-Scheduler base revision was `65d1b9a...`; the exact resumed Oct revision is `309da01b...`.

## 3. Final architecture

```text
typed command envelope
  -> bounded per-key FIFO mailbox
  -> canonical typed account-token ownership
  -> immutable authoritative state view
  -> resident generated AccountAgent.Step(context)
  -> typed TransitionDecision
  -> validate expected versions/effect bounds
  -> framed WAL record + post-turn logical checkpoint
  -> sync according to mode
  -> atomic in-memory effect publication
  -> retain advanced resident agent
```

On commit failure the resident flow is restored from its prior committed checkpoint. Normal successful turns do not restore, so this is genuinely a persistent-agent lane rather than ephemeral construction per command.

## 4. Ownership boundary

Oct owns nominal command/status/reason/effect types, the `CommandContext` Concept, positive-amount refinement admission, private workflow board, transition meaning, stateless utility arbitration, and yielded decision. Go owns identity, mailboxes, ordering, concrete account tokens, goroutine scheduling, accounts/ledger, validation, WAL/fsync, idempotency index, recovery, traces, and Octagon publication. No Go map is presented as an Oct board, and no authoritative account mutation occurs inside Oct.

## 5. `.oct` behavioral source

The source is [`account_agent.oct`](../../experiments/M7/runtime/oct/account_agent.oct). The key decision shape is:

```oct
fn DecideCode(input: CommandContext, pending: Bool, pendingTarget: Int, pendingAmount: Int) -> Int {
    return when utility {
        case 12 when input.Kind == CommandKind.Confirm and
            (not pending or pendingTarget != input.AccountB or pendingAmount != input.Amount) score 110
        case 13 when (input.Kind == CommandKind.Transfer or input.Kind == CommandKind.Confirm) and
            input.Amount <= 0 score 100
        case 17 when input.Kind == CommandKind.Transfer or input.Kind == CommandKind.Confirm score 10
        else 0
    }
}
```

The outcome code is mapped through one closed `switch` to one nominal `TransitionDecision`. Direct record-valued utility candidates compiled but cost about 1.05 µs and 4,240 B/turn; integer arbitration plus one construction reduced this to about 300 ns and 576 B/turn.

## 6. Generated host seam

Representative real host usage is:

```go
turn, err := entry.machine.Step(generated.Main_CommandContext{...})
decision, err := turn.Yielded()
checkpoint, err := entry.machine.Checkpoint()
restored, err := generated.RestoreAccountAgent(checkpoint)
```

The retained generated package is 80,098 bytes. Direct seam tests prove typed step/yield, deterministic checkpoint bytes, refinement rejection, restore, and identical next-turn continuation.

## 7. Agent registry

Identity is `AccountID -> agentEntry`. Creation is lazy; entries retain one generated machine, its last committed checkpoint, and a mailbox. There is no eviction and no hard registry cap in M0. At 100k active checkpointed identities the measured delta was 159,981,104 heap bytes and 600,290 objects: about 1.60 KB and six objects per identity. Lookup rose from about 8.85 ns at 1k to 15.84 ns at 100k.

## 8. Mailboxes

Each active key has one FIFO Go channel with configurable fixed capacity (64 by default). One lazy worker consumes it serially. Non-blocking enqueue returns categorized `mailbox_full`; queues never silently grow. Per-key FIFO is deterministic for accepted enqueue order. Cross-key order is intentionally scheduler-dependent.

## 9. Authoritative state

Go stores `map[AccountID]Account`, where `Account` contains ID, balance, status, and version, plus a ledger slice. Reads produce immutable copied observations. Only validated committed log records mutate the map. Canonical snapshot export sorts account IDs and emits stable nominal Octagon text.

## 10. Conflict scheduling

Static command shape remains closed in Oct; the host projects runtime identities into structured `Token{Kind: AccountToken, ID}` values. Tokens are sorted by kind and ID before acquisition. Transfer/Confirm acquire both accounts before reading authority or stepping the flow. The concurrency test blocks two independent agents at `state_view` simultaneously, while hot and many-to-one workloads intentionally serialize.

## 11. Transition/effect model

For `Transfer(1,2,25)`, Go reads versions 1/1 and balances 100/10, then Oct yields accepted effect tag 3 with balances 75/35 and expected versions 1/1. Go validates identity, versions, nonnegative balances, and referenced accounts, embeds the post-yield checkpoint in WAL sequence 3, syncs, applies both account versions to 2, appends one ledger entry, and retains the advanced agent. The inspectable trace is [`transfer.json`](../../experiments/M7/traces/transfer.json).

## 12. Atomic commit

The authoritative boundary is one WAL record containing one command result, the complete bounded effect, expected versions, and the post-turn agent checkpoint. In `SyncEach`, a successful return means the complete checksummed record was synced before state became visible. In-memory application is prevalidated and infallible under held conflict ownership. A crash after WAL sync but before memory publication replays the record. A sync/write error is an uncertain external durability outcome in the general filesystem model; clients resolve it by command ID after restart. M0 does not claim isolation for readers that bypass the Store API or broad ACID.

## 13. Durability log

The log uses `big-endian uint32 length | deterministic JSON payload | CRC32`. Payload version is 1 and includes sequence, agent/command identity, effect, expected/new state, result, and embedded checkpoint. Records are bounded to 1 MiB. Recovery accepts complete valid records, truncates an incomplete final frame, and stops on invalid length, JSON, version, ordering, or checksum failure. Modes are memory-only, batch sync every 64 records, and fsync per commit.

## 14. Checkpoints

Every logged turn embeds its post-yield FLOW-TURN logical checkpoint, prioritizing recovery simplicity. Representative account-agent checkpoints are 779–782 bytes. Export measured 1.03–1.24 µs, 1,249 B, two allocations; restore measured 5.55–5.57 µs, 1,328 B, 18 allocations. This is substantial log amplification and an explicit M0 tradeoff.

## 15. Recovery

M0 is log-only: scan valid frames, apply authoritative effects in sequence, validate and restore each embedded generated checkpoint, rebuild the duplicate-result index and registry, truncate an incomplete tail, then reopen for append. Tests cover clean multi-agent restart, transfer, workflow continuation, truncated tail, corrupt checksum, and incompatible checkpoint categories. Periodic snapshots and compaction are not implemented.

## 16. Correctness

Tests prove transfer conservation, both account versions advance together, frozen withdrawal has no authoritative effect, insufficient/invalid/missing operations reject, duplicate command IDs replay without a second ledger entry, durability failure publishes neither state nor flow progress, and the utility table matches the direct Go control across create/deposit/withdraw/transfer/freeze/workflow cases. FLOW-TURN private board state survives `BeginTransfer -> restart -> Confirm` and its transition count continues exactly.

## 17. PostgreSQL baseline

The real PostgreSQL 17 control uses pgx transactions, canonical `SELECT ... FOR UPDATE` row order, checked balances, account versions, ledger, pending workflow rows, and a command-result idempotency table. Its integration test passed against the repository's healthy container. At eight clients and 500 measured commands: independent deposits reached 1,843/s (p50 3.93 ms, p99 17.14 ms), random transfers 1,871/s (p50 3.84 ms, p99 17.98 ms), hot account 403/s, and many-to-one 405/s. PostgreSQL commits have its normal database durability semantics and are not byte-for-byte equivalent to the M0 WAL.

## 18. Go baseline

The Go control uses the same account representation, typed conflict tokens, WAL framing/fsync modes, idempotency policy, and workflow semantics, but implements decisions directly with ordinary typed Go. Behavioral equivalence tests pass. It is not deliberately weakened: its smaller decision path is the relevant control.

## 19. Throughput/latency

At 128 accounts, eight submitters, and seed 7737, representative commands/s were:

| Workload | Oct memory | Go memory | Oct batch-64 | Go batch-64 | Oct fsync | Go fsync |
|---|---:|---:|---:|---:|---:|---:|
| independent deposits | 383,811 | 1,297,185 | 55,370 | 76,604 | 2,989 | 3,412 |
| hot account | 237,823 | 1,290,822 | 64,975 | 89,471 | 3,240 | 3,492 |
| random transfers | 425,731 | 752,417 | 54,740 | 91,928 | 3,358 | 3,538 |
| many-to-one | 176,236 | 1,239,772 | 52,124 | 99,151 | 3,405 | 3,436 |

Fsync p50 was roughly 2.0–2.6 ms and p99 3.1–7.8 ms. Windows clock quantization censored many memory/batch p50 samples to zero; throughput and upper percentiles remain usable, but those zero medians must not be read as zero latency. Full raw evidence is [`benchmarks.json`](../../experiments/M7/evidence/resumed/benchmarks.json).

## 20. Contention

Independent work reaches Oct decisions concurrently. Hot source and hot destination workloads serialize at typed ownership as intended. In memory-only mode the persistent Oct lane fell from 383,811/s independent to 176,236/s many-to-one; fsync collapsed the difference because the single durable append boundary dominated. This experiment did not implement a separate naive-lock ablation, so it proves correct upstream scheduling and concurrency, not that scheduling outperforms every locking design.

## 21. Persistent flow value

The long-lived flow carries pending transfer target/amount and transition count privately. `BeginTransfer` commits no account effect but commits the workflow checkpoint; after process restart, `Confirm` validates the later input against that private state and yields the transfer. That is behavior a pure one-shot decision function cannot provide without a separate host-owned state machine. The cost is real: checkpoints and resident machines dominate per-agent memory.

## 22. Memory behavior

At scale, active checkpointed entries measured about 1.60 KB and six objects each; 100k used about 160 MB. Generated Step/Yield measured 576 B and one allocation because the nominal record yield is boxed in the generated last-yield slot. End-to-end Oct memory-only turns measured about 3.67 KB and 14 allocations versus roughly 0.75 KB and eight for Go. No unsafe, cgo, arena, slab, or custom allocator was introduced.

## 23. Write-to-Read publication seam

The trace workload publishes [`accounts.octagon`](../../experiments/M7/publication/accounts.octagon) with SHA-256 `f03c6591...01082d`. Tests prove uninterrupted and recovered state produce identical bytes/hash, and a subsequent committed deposit changes the logical hash. This proves the stable canonical boundary expected by compiled-data publication; it does not rerun the full M6 read benchmark.

## 24. Architecture ablations

Persistent Oct vs direct Go is measured: direct Go is about 0.74–0.76 µs per memory-only hot turn versus 6.07–6.18 µs end-to-end Oct, while fsync narrows throughput to within roughly 1–14%. The accidental restore-every-turn prototype was removed before final measurement because it was the ephemeral-flow ablation, not the target architecture. Direct record-valued utility candidates were also ablated against integer outcome scoring; the latter cut Step/Yield allocation from 4,240 B to 576 B.

## 25. Oct pressure findings

Three retained findings are in [`oct-pressure.json`](../../experiments/M7/evidence/resumed/oct-pressure.json):

- **Bug:** a flow-local record bound by `let` and yielded passed checking but generated `f.decision` without a field.
- **Compiler gap:** record-valued `when utility` candidates materialize excessive data; score scalar outcome codes and construct one winner.
- **Host ABI gap:** typed nominal record yields still pass through an internal `any` slot and allocate once.

The issues were not patched in Oct because none blocked M7 after narrow source workarounds.

## 26. OctetDB Write identity

1. “Agent/state-machine database” describes its behavior layer but overstates the storage layer.
2. The real primitive is a durable keyed command processor.
3. Authoritative state is separate from agents.
4. One flow should own one behavioral/workflow key, not necessarily one physical row and not the entire database.
5. Mailboxes are central scheduling/backpressure infrastructure, not language semantics.
6. Explicit effect/commit separation is worth the complexity because it makes rollback and recovery defensible.
7. Transition logging is right for M0, but needs snapshot/compaction work.
8. Upstream conflict scheduling is correct and exposes concurrency; superiority over ordinary locking remains unproven.
9. Persistent state is justified for real multi-turn workflows, not for every one-shot entity.
10. Compile command/effect types, admission, transition structure, utility policy, and static conflict shape.
11. Keep key identity, contention, capacity, ordering, commit, fsync, and recovery host-dynamic.
12. Never make mailboxes, shared mutable boards, locks, WAL/fsync, or database storage syntax part of Oct merely to hide the host.

## 27. Memory-management interpretation

Explicit topology and ownership produced competitive durable performance using ordinary safe Go: at fsync-each, Oct was close to Go because storage dominated. Manual memory manipulation would not address that bottleneck. In memory-only mode, however, generated typed-record boxing, checkpoint copies, envelopes, response channels, and traces/IDs create measurable allocation and GC pressure. The evidence supports optimizing representation and lifetime first; it does not support a general claim that manual allocation never matters.

## 28. Remaining limitations

M0 has no periodic state snapshot, log compaction, eviction, hard registry capacity, cross-process coordination, replication, MVCC, or reader transactions beyond canonical Store snapshots. WAL append is one serialized boundary. Batch mode may acknowledge records not yet synced. The exact syscall failure point can make a failed commit outcome uncertain until restart; command IDs are the resolution mechanism. The PostgreSQL duplicate path is tested sequentially, not under same-ID concurrent races. Persistent controller-policy utility state and the resume slot were already proven by FLOW-TURN-M1 but are not duplicated in this domain; M7 uses stateless utility scoring and private-board workflow persistence. Latency decomposition below end-to-end, Step/checkpoint microbenchmarks, and trace phases is incomplete.

## 29. Exactly one next recommendation

OctetDB Write M1 durability/storage refinement.

Add periodic canonical state snapshots, log compaction, explicit poisoned-log handling for uncertain write/sync failures, and checkpoint cadence experiments while retaining the exact Oct/Go ownership boundary demonstrated here.

## Resumed reproduction

```powershell
# Generate the independent Go facade from exact Oct source/revision.
cd experiments/M7/runtime/generator
go run . ../oct/account_agent.oct ../../../../internal/m7generated/account_agent.generated.go

# Core and optional real PostgreSQL correctness.
cd ../../../..
go test ./...
$env:DBSCHED_POSTGRES_DSN='postgres://dbsched:dbsched@localhost:54329/dbsched?sslmode=disable'
go test -v ./internal/m7write -run TestPostgreSQLBaseline -count=1

# Seeded A/B/C evidence and deterministic trace/publication.
go run ./cmd/m7bench -operations 2000 -accounts 128 -workers 8 -out experiments/M7/evidence/resumed/benchmarks.json
go run ./cmd/m7trace
```
