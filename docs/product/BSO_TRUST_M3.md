# BSO-TRUST-M3 — Policy Correctness, DAO-Class Failures, and Programmable-State Architecture Study

## 1. Verdict

**Success.** A bounded typed policy layer composes with BSO-owned durable
financial state without a contract VM. Exact policy identity and immutable
versions survive admission, audit, retry, and migration. The commit boundary
atomically rechecks mutable authority facts and reserves capacity, so 20
concurrent ten-unit attempts against a 100-unit authorization admit exactly 10.

The deliberately wrong recurring policy is equally important: its types are
valid and its per-operation rule executes exactly as encoded, so twelve
ten-unit payments pass an intended 100-unit cap. The error affects two financial
authorities, one relationship, and thirteen transfers including one separately
admitted in-flight transfer; unrelated BSOs and balances touched remain zero.
Disabling V2 stops future admissions, V3 restores bounded semantics, and one
wrong transfer is compensated by a new refund. Twelve committed effects remain
uncompensated in the refusal/incomplete-recovery model. Types constrain
authority; they do not infer intent or compel a counterparty to return funds.

The recorded experiment completed in 270 ms. The compiled Oct lane ran five
compiled cases plus one negative type contract with zero interpreted fallback
and zero diagnostics in 713 ms.

## 2. Historical failure class

M3 addresses execution that is mechanically correct but semantically wrong.
Provider, network, storage, and scheduler failure are not required. The rule
itself grants more repeat authority than its author intended. The response is
not global rollback; it is structural scope, immutable provenance, local
disablement, new policy versions, and explicit compensation.

## 3. Threat model

The tested threats are:

- a well-typed policy omits a cumulative bound;
- a delegate reuses valid authority for a different counterparty, class,
  amount, or time;
- two decisions race against one consumable limit;
- a nested action observes state before the outer action establishes its
  invariant;
- an old decision reaches commit after relevant facts changed;
- two individually valid policies disagree;
- a new version materially widens authority;
- a worker dies after decision production;
- retry or duplicate messages attempt a second financial effect.

TrustProvider balance mutation, application-policy mutation, arbitrary
callbacks, arbitrary contract storage, contract bytecode, and global history
rewriting are outside the available APIs.

## 4. Policy taxonomy

`PolicyClass` distinguishes `Trust`, `Authorization`, `Transaction`, and
`Application`. They share identity/provenance fields but not ambient authority.

| Policy class | Decides | Does not own |
| --- | --- | --- |
| TrustPolicy | Required trust evidence and roles | Balances or authorization consumption |
| AuthorizationPolicy | Who may request which bounded action | The financial mutation API |
| TransactionPolicy | Transfer-class invariants | Application-specific mutable storage |
| ApplicationPolicy | Escrow/recurrence/refund admission requirements | Arbitrary BSO writes or callbacks |

The Go evaluator accepts only `PolicyV1 + PolicyFactsV1`; it cannot reach an
`Authority`. The `Authority` type separately owns OctetDB state and mutation
methods.

## 5. Policy identity/versioning

Every policy has `PolicyID`, positive `PolicyVersion`, and SHA-256
`PolicyDigest`. `ProposePolicy` seals and validates canonical policy bytes.
Attempting to propose different bytes under an existing `(PolicyID, Version)`
is rejected. `ActivatePolicy` changes only the future active version.

The Alice experiment retains seven versions: one correct recurring, three
buggy/repair history versions, two widening versions, and one migration policy.
Bob retains the separate refund policy.

## 6. Bounded policy vocabulary

The implemented vocabulary is closed and typed: `PolicyClass`,
`TransactionClass`, `AuthorizationScopeV1`, per-operation and cumulative
amounts, validity time, required trust roles, an escrow threshold, and
`DeliveryOrTimeoutNoDispute`. Decisions use `ReasonCode` and a bounded
`RequiredAction` list. There is no executable AST, general DSL, key/value
contract store, dynamic code loading, callback, or gas model.

The malformed escrow specimen sets a release threshold without a valid typed
release condition and fails `ValidatePolicy`. This catches structural absence;
it cannot prove that `Delivery OR (Timeout AND NoDispute)` is the business rule
participants meant.

## 7. Authority scope

`AuthorizationScopeV1` binds exactly:

```text
AuthorizationID
Subject
Delegate
Counterparty
TransactionClass
MaxAmount
MaxCumulativeAmount
ValidUntil
```

One relationship's delegated authority cannot select another counterparty or
class. The authority-amplification attempt from Bob to Carol is rejected and
produces zero mutation.

## 8. Pure decision / authorized mutation separation

`EvaluatePolicy(policy, facts) -> PolicyDecisionV1` is deterministic and has no
side effects. `Authority.Reserve` is the only admission mutation and
`Authority.Finalize` is the only debit/consumption mutation. `Authority.Credit`
is a typed idempotent bilateral protocol effect. An ApplicationPolicy value has
no method, closure, pointer, interface, or capability through which it can call
those operations.

The safe ordering is:

```text
pure validation
-> authority-owned atomic recheck and reservation
-> local debit/finalization
-> typed receiver credit
-> retry/reconcile if the external effect is interrupted
```

This does not provide global atomicity; it makes local invariants and partial
progress explicit.

## 9. Authorization scope

Service X receives authority for Alice, Bob, Subscription, at most 10 units per
operation, at most 100 cumulatively, and until logical time 1000. It receives no
authority over Alice's other counterparties, transaction classes, policy
versions, or unrelated state. Missing trust yields the typed
`MissingTrust`/`RequireTrustAttestation` decision.

## 10. Cumulative limits

The correct recurring lane admits ten payments of 10 and rejects the eleventh
with `CumulativeLimitExceeded`. `Consumed + Reserved + Requested` is checked,
so independently valid calls cannot collectively exceed the grant.

The buggy V2 encodes `MaxAmount=10` and `MaxCumulativeAmount=0`, where zero
means no cumulative bound. All twelve calls pass. `IntendedCumulativeAmount=100`
is retained only as experiment evidence; it deliberately does not override the
encoded rule.

## 11. Reentrancy analogue

Action A reserves all 10 units of a bounded escrow-release authorization.
Before A finalizes, indirect Action B evaluates against `Reserved=10`. B is
deterministically rejected with `CumulativeLimitExceeded`; A then finalizes
once. No Solidity lock or callback stack is modeled. The invariant is protected
because capacity is authority-owned state established before interaction.

Policies have no `SendMoney` capability. A follow-on action must be a new typed
request with a new transfer and decision identity.

## 12. TOCTOU

Two ten-unit decisions are produced from the same version-zero facts for a
ten-unit grant. The first reservation increments the authorization fact
version. The second reservation rejects the captured decision with
`StateConflict`. Re-evaluation would then report `CumulativeLimitExceeded`.
The earlier pure read is evidence, not a commit lock.

Rejected stale reservations are idempotent by `(TransferID, DecisionID)`, not
only `TransferID`. This is necessary so a fresh decision can retry after a stale
one; a successful transfer record still prevents duplicate financial effects.

## 13. Concurrent authority consumption

Twenty goroutines concurrently attempt 10 units each against one 100-unit
authorization. Stale decisions retry from current facts.

| Attempted | Succeeded | Rejected | Consumed | Double consumption |
| ---: | ---: | ---: | ---: | ---: |
| 20 | 10 | 10 | 100 | 0 |

Authority mutation is single-owner serialized and durably committed through
OctetDB. The exact permitted total succeeds.

## 14. Policy composition

Composition is explicit conjunction: all applicable decisions must allow.
There is no policy merge engine. A Subscription authorization allows a six-unit
request; the applicable Marketplace application policy requires escrow above
five and denies it. The composed decision is `MissingEscrow`, with zero
financial mutation.

## 15. Policy conflicts

Allow plus deny is deny. There is no silent precedence or "last rule wins."
The machine decision retains the denying typed reason and required action.
Role-specific precedence could be introduced only as another explicit bounded
contract; none is needed for M3.

## 16. PolicyDiff

`PolicyDiffV1` carries the policy ID, old/new versions, typed changes, and
`AuthorityExpanded`. Implemented changes include amount-limit increase or
decrease, cumulative-limit addition/removal/increase/decrease, counterparty
change, validity extension, and required-trust reduction.

## 17. Policy widening

The 10-to-10,000 amount change is detected as `AmountLimitIncreased` and
`AuthorityExpanded=true`. Activation without the additional local approval flag
fails and V1 remains active. Activation with the flag selects V2, after which a
100-unit future transfer that V1 could not admit succeeds under exact V2.
The deliberately buggy cumulative-limit removal is also surfaced as authority
expansion, demonstrating that explicit approval is necessary but not proof of
correct intent.

Restriction changes such as a lower maximum or adding a cumulative bound are
classified as non-expanding. This is a practical monotonic-safety subset, not a
general formal policy calculus.

## 18. Emergency disable

Disabling `(policy:buggy-recurring, V2)` makes the next reservation return
`PolicyDisabled`. Completed transfers, audits, balances, and the V2 definition
are unchanged.

The deterministic in-flight rule is: **successful reservation is admission**.
One transfer reserved under V2 before disable continues to finalization under
its captured V2 identity. Requests not yet reserved must pass the current
active/not-disabled check.

## 19. Policy rollback/version restoration

V3 restores V1's per-operation and cumulative semantics with a new
authorization period. V1, V2, and V3 remain distinct immutable records. This is
future version restoration, not mutation or rollback of V2 history.

## 20. Compensating transactions

`refund:bug:00` is a new Bob-to-Alice `Refund` transfer under Bob's separate
refund policy and authorization. It references `bug:00`, has its own transfer,
decision, policy, audit, debit, and credit. The original remains committed.

One compensation succeeds. Twelve affected committed transfers (the remaining
batch transfers plus the in-flight transfer) remain uncompensated in the
refusal/incomplete-recovery model. Compensation is not time travel and policy
correctness cannot force a recipient to cooperate or possess funds.

## 21. Deliberately buggy policy

V2 is syntactically valid, type-correct, canonical, digest-bound, explicitly
activated, and deterministically replayable. It omits the cumulative limit that
participants intended. Twelve of twelve ten-unit requests succeed, proving the
honest limit of typed policy: better structure and blast-radius control do not
solve specification.

The Oct `TypedPolicyDoesNotDivineIntent` case independently demonstrates that a
twelfth individually valid payment is allowed by the same bounded but wrong
rule.

## 22. Blast radius

| Metric | Result |
| --- | ---: |
| Affected financial authorities | 2 (Alice and Bob) |
| Affected relationships | 1 |
| Affected transfers | 13 (12 exploit batch + 1 admitted in flight) |
| Unrelated BSOs touched | 0 |
| Unrelated balances touched | 0 |

Carol and Dave are opened with durable balances before the exploit and compared
afterward; both balance and transfer maps remain unchanged. The policy has no
route to enumerate or mutate them.

## 23. Confused deputy

The valid delegated authorization is reused four ways:

| Attempt | Result |
| --- | --- |
| Wrong counterparty | `WrongCounterparty` |
| Wrong transaction class | `WrongTransactionClass` |
| Amount 11 against maximum 10 | `AmountExceeded` |
| Logical time 1001 after expiry 1000 | `ExpiredAuthorization` |

All four fail. The separate amplification attempt toward Carol also fails.

## 24. Migration/replay

`AgentCheckpointV1` carries `TransferID`, exact policy identity, full typed
decision, and placement generation; it carries no policy closure or BSO handle.
Migration increments only placement generation. Decision ID, facts digest,
request digest, policy version, and policy digest remain exact. Replaying
finalize and receiver credit causes one financial effect.

The established real scheduler path is also rerun with worker 1 killed at round
2: one worker failure migrates two agents, and conservation/correctness remain
green. M3 therefore changes neither scheduler placement authority nor M0's
checkpoint recovery mechanism.

## 25. Audit provenance

Every admitted debit retains transfer ID, policy ID/version/digest, decision ID,
typed admission, and relevant attestation IDs. The experiment retains 26 sender
audit records across the tested Alice/Bob policy paths. Application payload is
not copied into the audit DTO.

The public policy projection exposes only identity, transaction class, maximum
amount, escrow threshold, and required trust roles. It omits consumption,
balances, other counterparties, history, and unrelated private rules.

## 26. Oct/Octagon contracts

`policy_contract.octest` defines positive amount/version/time Concepts, exact
authorization scope, policy facts, typed decision/reason/diff/change enums, and
pure bounded evaluation. It proves valid admission, confused-deputy rejection,
authority-expansion detection, deny-wins composition, and the deliberately
wrong-but-well-typed rule. `policy_wrong_type.octfail` proves a string cannot be
used as `PositiveAmount`.

Go policy digests derive from fixed-field, data-only canonical Octagon bytes,
not JSON. The measured V2 representation is 449 bytes. Replaying equal policy
and fact values produces equal decision identity.

Compiler result:

```text
6 passed, 0 failed, 0 skipped
compiled cases: 5
interpreted fallback: 0
diagnostics: 0
time: 713 ms
```

## 27. Limits of typed policy

Types catch wrong shapes, invalid enums, missing required structure, and
cross-dimension misuse. Exact scopes, versioning, reason codes, and capability
separation shrink structural failure. They do not determine whether ten means
per call or per month, whether an escrow condition matches a legal agreement,
whether a trusted attestation is truthful, or whether a recipient will return
funds.

The model also gives up arbitrary on-the-fly computation, arbitrary synchronous
callbacks, generic shared storage, and permissionless contract deployment.
Applications outside the bounded vocabulary need a new reviewed typed policy
or explicit protocol, not a user-supplied program.

## 28. Comparison with arbitrary smart contracts

| Property | Bounded typed policy | Hypothetical arbitrary contract model |
| --- | --- | --- |
| Authority scope | Exact subject/delegate/counterparty/class/amount/time | Determined by every reachable API/call and contract-held asset |
| State ownership | BSO owns durable financial state | Contract/global VM owns mutable shared state |
| Execution bounds | Static closed evaluator, no recursion/callbacks | General computation needs runtime resource limits |
| Replay | Exact policy bytes + facts + version -> decision | Exact bytecode + global state + execution environment |
| Upgrade | Proposed immutable versions, diff, explicit future activation | Contract-specific proxy/governance/immutability patterns |
| Blast radius | Relationship and capability bounded | Potentially every asset/contract reachable under encoded authority |
| Recoverability | Disable future version; explicit compensation | Revert only within execution; committed errors also require later action unless history is rewritten |

No EVM, WASM runtime, blockchain, token, gas, global ledger, governance vote,
or global rollback was added.

## 29. Distributed programmable-state architecture study summary

The companion [Programmable Distributed State Study](PROGRAMMABLE_DISTRIBUTED_STATE_STUDY.md)
finds a credible authority-domain architecture: durable local authorities,
typed message protocols, process-manager transaction agents, attenuated
capabilities/attestations, reservations, sagas, and selective consensus.

It resembles established actor, capability, event-sourcing, distributed
database, escrow, saga, CRDT/local-first, federated identity, state-machine
replication, and Byzantine-consensus ideas. Its advantage is locality: unrelated
participants do no work and acquire no implicit authority. Its decisive cost is
the loss of synchronous shared-state composability and global atomic rollback.
Global scarce names/tokens and canonical shared auctions remain legitimate
consensus-domain cases.

## 30. Architecture decision

**A. Bounded typed policy composes cleanly with BSO financial authority and
materially limits DAO-class failure scope.**

## 31. Policy-safety decision

**P2. Structural protections work, but specification bugs remain a major
operational risk.** This is the observed boundary, not a failed result.

## 32. Programmability decision

**G1. Bounded typed policy covers the tested programmable-finance cases without
a contract VM.**

## 33. Recovery decision

**R2. Recovery works but one committed-error class remains operationally
severe.** Disable/versioning are coherent and compensation is explicit, but
uncooperative or insolvent recipients make some completed wrong transfers
irrecoverable by software policy alone.

## 34. Distributed-architecture study decision

**D1. A credible general architecture exists around local durable authorities,
explicit protocols, typed capabilities/attestations, and selective consensus.**

## 35. Exactly one next recommendation

Test adversarial cross-authority saga recovery when one participant crashes or
refuses compensation after local commit.
