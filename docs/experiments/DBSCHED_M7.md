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
