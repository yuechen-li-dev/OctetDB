# Programmable Distributed State Study

## Question and verdict

There is a coherent general architecture for programmable cross-party state
that does not make globally replicated execution the default. The unoriginal
but useful synthesis is:

```text
durable authority domains
+ typed asynchronous protocols
+ attenuated capabilities and attestations
+ local atomicity and reservations
+ sagas and explicit compensation
+ selective replicated/consensus domains
+ content-addressed evidence where needed
```

Call it an **authority-domain architecture**, descriptively, not as a product
claim. It is a composition of established ideas. Its advantage is that
unrelated participants do no execution, storage, or agreement work for an
unrelated transaction. Its largest cost is the loss of Ethereum-style
synchronous shared-state composability and ubiquitous atomic rollback.

## What Ethereum was trying to solve

Ethereum joined several real needs behind one abstraction: shared programmable
state, cross-organization transactions, public deployment, deterministic
execution, auditable history, portable economic authority, and composition of
independently deployed components. Ethereum's documentation accurately calls
the EVM a global virtual computer whose state participants store and agree on,
and describes smart contracts as code plus state at an address. Its strongest
ergonomic property is that public contracts behave like mutually callable APIs
inside a common execution environment. Sources: [technical introduction to
Ethereum](https://ethereum.org/developers/docs/intro-to-ethereum/), [smart
contracts](https://ethereum.org/developers/docs/smart-contracts/), and [smart
contract composability](https://ethereum.org/developers/docs/smart-contracts/composability/).

Those needs do not all imply the same agreement scope. Public verifiability is
not identical to global execution. Cross-party coordination is not identical
to one world state. Portable authorization is not identical to a bearer key
with access to every contract that accepts it.

## Agreement-scope decomposition

The default question for each datum is: who can legitimately disagree about
it? That identifies the smallest plausible authority and ordering domain.

| State or decision | Local authority | Bilateral agreement | Small-group consensus | Global consensus |
| --- | --- | --- | --- | --- |
| Alice's available balance | Alice's BSO or account authority owns it | Payment protocol communicates its consequence | Only if the account itself has joint owners/replicas | No |
| Alice pays Bob | Each endpoint commits its own state | Yes; transfer identity and protocol outcome | Optional replicated authority at either endpoint | No, unless the asset definition demands one canonical ledger |
| Twenty-person auction | Bidder balances remain local | Settlement is bilateral with winners | Auction ordering/winner needs one bounded shared authority | No unless participation or the asset is intentionally global |
| Guild inventory | Player and guild domains own their respective records | Transfers cross domains | Guild membership or raid outcome may use bounded agreement | No |
| Unique worldwide name | Local claims are insufficient | Pairwise recognition can support non-canonical aliases | Communities can run separate registries | Possibly, if one canonical winner is the product |
| One canonical token supply | Not if every holder must recognize the same supply history | Channels can move claims off the shared base | Shards/committees can execute portions | Yes, for the canonical base unless trust is delegated to issuers |

Consensus is therefore for disagreement over shared authority. It is not a
generic distributed-compute primitive. State-machine replication is a sound
way to implement a fault-tolerant service by coordinating replicas, not a rule
that every independent service must join the same machine ([Schneider's state
machine tutorial](https://www.cs.cornell.edu/courses/cs614/2003sp/papers/Sch90.pdf)).
Byzantine replication is justified where members of one authority domain may
fail arbitrarily; PBFT is a scoped replicated-service technique, not evidence
that unrelated applications need one order ([Castro and Liskov,
PBFT](https://www.usenix.org/legacy/events/osdi99/full_papers/castro/castro_html/castro.html)).

## Relationship to established architectural families

| Family | What it contributes | Where it does not substitute for policy |
| --- | --- | --- |
| Actor systems | Encapsulated state, message passing, and independent progress. A BSO resembles a durable actor; a TransactionAgent resembles a process manager more than an account actor. The original actor work centers computation on actors and messages ([Hewitt, Bishop, and Steiger](https://www.eighty-twenty.org/files/Hewitt%2C%20Bishop%2C%20Steiger%20-%201973%20-%20A%20universal%20modular%20ACTOR%20formalism%20for%20artificial%20intelligence.pdf)). | Actor isolation does not define financial authority, idempotency, or protocol settlement. |
| Distributed objects/capabilities | Object ownership and unforgeable/attenuated authority fit local domains. Macaroons demonstrate contextual caveats that restrict how, where, and when delegated authority may be used ([Macaroons paper](https://theory.stanford.edu/~ataly/Papers/macaroons.pdf)). | A credential is not the resource state and cannot make a completed transfer reversible. |
| Event sourcing | Immutable events retain provenance; a correction is another event. M3's original transfer plus compensating transfer follows this historical model. | An event log does not by itself settle concurrency, ownership, or truth between domains. |
| Sagas/workflows/process managers | A long-lived action becomes explicit local transactions plus compensation. This is directly aligned with the original saga formulation ([Garcia-Molina and Salem](https://www.cs.princeton.edu/techreports/1987/070.pdf)). Migratable TransactionAgents add durable protocol position and placement independence. | Compensation may be impossible or economically incomplete; it is not rollback. |
| Escrow transactions | Reserve bounded quantities so concurrent operations cannot collectively violate an invariant. O'Neil's escrow method explicitly addresses long-lived and distributed work while isolating resources placed in limbo ([Escrow Transactional Method](https://www.ics.uci.edu/~cs223/papers/p405-o_neil.pdf)). | It applies to decomposable quantitative invariants, not every semantic conflict. |
| Distributed databases | Local ACID transactions, conditional writes, dedupe keys, and durable indexes implement each authority. Atomic commit or consensus can be selected when a workload truly requires shared commit; Gray and Lamport clarify the relationship between transaction commit and consensus ([Consensus on Transaction Commit](https://www.microsoft.com/en-us/research/publication/consensus-on-transaction-commit/)). | Cross-domain atomic commit adds coordination and failure coupling; it is not a free default. |
| CRDTs/local-first | CRDTs give deterministic convergence for data types satisfying specific algebraic conditions ([Shapiro et al.](https://dsf.berkeley.edu/cs286/papers/crdt-tr2011.pdf)); local-first work makes local ownership and offline progress a design priority ([Kleppmann et al.](https://martin.kleppmann.com/2019/10/23/local-first-at-onward.html)). | Scarce balances, exclusive winners, and revocation are generally not arbitrary mergeable state. |
| Federated identity/OAuth/OIDC | Separate resource owner, client, authorization server, and resource server roles; issue limited credentials rather than sharing owner credentials. OAuth explicitly frames third-party limited access ([RFC 6749](https://datatracker.ietf.org/doc/rfc6749/)). | Identity and access grants are evidence inputs, not financial mutation authority. |
| Payment channels/rollups/sharding | Move repeated work into narrower execution or data-availability domains while retaining a selected base for dispute/finality. Ethereum itself describes channels as minimizing on-chain interaction and now favors rollup-centric scaling ([state channels](https://ethereum.org/developers/docs/scaling/state-channels), [scaling overview](https://ethereum.org/developers/docs/scaling/)). | They retain a canonical base and its asset/security assumptions; they do not establish that all application state needs that base. |

The BSO model is best described as durable actors/authority domains plus
process-manager transaction agents, local event-sourced audit facts, scoped
escrow reservations, saga recovery, and capability-like policy. It is not a
novel replacement for any one of those bodies of work.

## Ordering and data locality

Different state needs different order:

| Order | Example | Mechanism |
| --- | --- | --- |
| Per-authority order | Debits and cumulative authorization at Alice | One BSO mutation/reservation sequence |
| Per-relationship order | Subscription installment sequence | Relationship protocol sequence or predecessor identity |
| Causal order | Refund refers to an original transfer | Explicit reference and provenance |
| Bounded total order | Auction bids before a deadline | Auction authority or replicated auction domain |
| Global total order | One canonical global name/token transition | Global consensus domain, if that product property is mandatory |
| No meaningful shared order | Independent Alice-to-Bob and Carol-to-Dave payments | None |

The scaling criterion is aggregate useful capacity: adding independent
authority domains should add capacity for partitionable workloads. Adding
replicas to one authority increases redundancy/security, not aggregate
independent work. These are separate axes.

## Atomicity, failure, and composability

Ethereum-style execution offers one transaction-level state transition against
the shared machine: nested calls can compose synchronously, and failure can
revert the transaction's changes. This is genuinely valuable for atomic asset
swaps, tightly coupled market operations, flash liquidity, and other operations
whose meaning depends on one instantaneous shared snapshot.

The authority-domain default is local atomicity plus protocol-level eventual
completion:

```text
validate -> reserve/commit local invariant -> send typed effect -> finalize/reconcile
```

That default isolates failure but exposes partial progress. Developers must
design idempotency, deadlines, reconciliation, and compensation explicitly.
The hard cost is not gas; it is protocol and recovery complexity. Some
cross-domain operations can compose sequentially or through a saga, some can
use pre-authorized reservations at every participant, and some should opt into
a bounded shared consensus/atomic-commit domain. They cannot all retain
Ethereum's synchronous "call any public state now" ergonomics.

## Verification without universal replay

Evidence should be proportional to the claim:

| Claim | Plausible evidence |
| --- | --- |
| A specific authority approved this request | Signature or authenticated channel plus exact policy/version decision |
| A credential is current and scoped | Provider attestation, expiry, revocation state, capability caveats |
| A record is the one previously referenced | Content digest or Merkle proof |
| A small group agreed on one winner | Quorum/consensus certificate from that domain |
| Computation was executed correctly without revealing inputs | Succinct proof only where its cost and trust model are justified |
| A service ran approved code | Replicated execution or trusted execution, with their distinct assumptions |

SNARK/STARK proofs, TEEs, and replicated execution are optional evidence
services, not defaults. Signatures do not prove that a policy was wise; proofs
of execution do not prove the specification matched intent.

## Multiplayer-world thought experiment

A distributed multiplayer world decomposes naturally:

- player inventory is owned by a player/account authority and may be replicated
  within that domain;
- region state is ordered by a region authority, possibly a small replicated
  group for availability or adversarial operation;
- guild state is owned by the guild's membership/treasury domain;
- an auction is a temporary bounded consensus/ordering domain whose outcome
  triggers explicit inventory and payment protocols;
- movement between regions is a handoff protocol with a transfer token or
  reservation, not a hidden callback into both databases;
- a truly unique world name or season-wide canonical winner may require a
  broader registry/consensus domain.

This generalizes beyond finance, but it does not imply that arbitrary game
logic should become financial policy or that all world state is mergeable.

## Required comparison

| Property | Ethereum-style global execution | Federated authority / agent model | Notes |
| --- | --- | --- | --- |
| State ownership | Contract/account state in one canonical machine | Explicit durable authority domain | Domain may internally replicate without globalizing ownership |
| Ordering | Canonical chain order | Per-authority, per-relationship, causal, or selected consensus order | Global order only for genuinely global state |
| Atomicity | Transaction-wide shared-state commit/revert | Local atomicity; saga/reservation across domains | Bounded domains may opt into atomic commit |
| Failure scope | Bug may affect all assets/relationships a contract can reach | Type/API/capability scope bounds reachable authorities | Shared providers and shared authorities can still create correlated risk |
| Scaling with nodes | More validators primarily add redundancy; rollups add scoped execution capacity | More independent domains add partitionable capacity | Replicas and independent domains measure different things |
| Cross-domain composition | Synchronous and discoverable public-contract calls | Typed messages, process managers, and explicit compatibility | More latency and developer-visible recovery |
| Programmability | General deployed bytecode under gas/resource rules | Bounded policy, protocols, and state machines | Loses arbitrary callbacks and runtime extension |
| Verification | Replicated execution and canonical state proofs | Signatures, attestations, digests, selective proofs/replication | Evidence selected per claim |
| Upgrade semantics | Contract-specific proxy/governance patterns; immutability varies | Explicit versions; future activation; historical binding | Widening is machine-readable and locally approved |
| Recovery | Revert within a transaction; later committed history normally remains canonical | Disable future admission and issue compensating protocols | Neither model makes an externally completed economic act disappear |

## Required application table

| Application | Local authority sufficient | Bilateral protocol | Small-group consensus | Global consensus |
| --- | --- | --- | --- | --- |
| Payment | Balances and reservations | Normal transfer path | Optional replicated bank/account domain | Only for a deliberately global canonical asset |
| Subscription | Schedule and cumulative authorization | Each payment | Usually unnecessary | No |
| Marketplace escrow | Buyer/seller balances | Funding and release effects | Marketplace/dispute authority may be a bounded group | No |
| Auction | Bidder funds | Deposit/settlement | Yes for canonical bid order and winner within the auction | Only for an intentionally worldwide auction |
| Multiplayer game inventory | Player/region inventory | Item handoff | Region/guild/auction may require bounded agreement | Only for deliberately global scarce objects or outcomes |
| Global namespace | Local aliases | Federation can exchange claims | Community registries can disagree | Yes if one worldwide canonical name is essential |

## Architecture synthesis

1. **What useful problem was Ethereum solving?** Permissionless deployment of
   programmable shared economic state with deterministic public execution,
   auditability, and unusually strong synchronous composability.
2. **Which parts genuinely require consensus?** Decisions for which multiple
   mutually distrustful participants require one canonical result: global
   scarce supply, a bounded shared auction, replicated authority state, or a
   canonical registry. The consensus scope should match the authority scope.
3. **Which parts require verifiable cross-authority protocols?** Ordinary
   payments, subscriptions, escrow funding/release, delegation, refunds,
   credentials, and most workflow handoffs.
4. **What does BSO resemble?** Durable actors/aggregate roots plus workflow
   process managers, capability-shaped authorization, escrow reservations,
   event-sourced provenance, and saga compensation.
5. **Biggest advantage?** Failure, storage, execution, and authority stay local;
   unrelated participants do no work and policy bugs cannot automatically gain
   global reach.
6. **Biggest cost?** Loss of synchronous shared-state composability and global
   transaction rollback; protocol design, latency, reconciliation, and partial
   failure become explicit.
7. **Credible beyond finance?** Yes for organizational workflows, games,
   supply-chain handoffs, federated collaboration, and other naturally
   partitioned state, with selective consensus for truly shared decisions.
8. **What should not be generalized?** Do not force all data into BSOs, treat
   every assertion as financial authority, pretend CRDTs solve scarcity, make
   compensation look like time travel, or reject consensus where one canonical
   contested decision is the actual requirement.

## Decision

**D1. A credible general architecture exists around local durable authorities,
explicit protocols, typed capabilities/attestations, and selective consensus.**
The finding is a change of default authority model, not "Ethereum but better."
