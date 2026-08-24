# BSO-TRUST-M2 — Continuity, Latent Substitutability, and Escape-Hatch Testing

## 1. Verdict

**Success.** The dominant-provider lane keeps 100% of ordinary role usage on
`general:a`, verifies relationship-local specialist escape paths off the hot
path, and improves provider-removal survival from TRUST-M1's 0/600 to 600/600.
Ordinary provider calls remain three per transaction. The initial continuity
pass costs three lightweight calls and three canonical proof records per
three-role relationship; the recurring lane amortizes that to three continuity
calls across 100 payments.

The adverse lane remains important: when `risk:b` silently changes and both
available Risk alternates are unusable, restore-day revalidation rejects the
stale proof and failover stops. Running maintenance earlier detects `risk:b`
and replaces it with `risk:c`, after which failover succeeds. A backup that is
never retested is still only historical evidence.

## 2. TRUST-M1 problem statement

TRUST-M1 demonstrated ordinary provider plurality but deliberately converged
90% of policies on provider family `:a`. The monopoly workload settled 600/600
before removal and 0/600 after removal. No provider owned financial state, yet
policy convergence made that provider operationally mandatory.

## 3. Centralization versus captivity

M2 separates `PrimaryUsageShare` from `ContinuityCoverage`. The measured
dominant lane has 100% primary usage by `general:a`, 100% continuity coverage,
and zero captive relationships. Concentration therefore does not imply
captivity when admissible alternates are explicitly and recently tested.

## 4. ContinuityPolicy

`ContinuityPolicyV1` is local policy containing Role,
MinimumViableAlternates, CheckInterval, MaxStaleness, Strict,
TransactionClass, and MaximumAmountBand. Identity uses a 300-unit lifetime,
Authorization 200, and Risk 100. Risk is checked more frequently because its
viability is more transaction-sensitive.

## 5. ContinuityState

`ContinuityStateV1` contains RelationshipID, Role, PrimaryProviderID,
deduplicated VerifiedAlternates, LastChecked, ValidUntil, Health, and a typed
Reason. Health is the closed enum `Healthy`, `Degraded`, `Unknown`, or `Failed`;
degradation is not reduced to prose or a boolean.

## 6. ContinuityProof

`ContinuityProofV1` binds the relationship, role, primary, alternate, both BSO
policy versions, provider policy/schema versions, CheckedAt, and ValidUntil.
Its deterministic ID is SHA-256-derived from canonical Octagon bytes. Repeating
the same check produces the same ID and overwrites the exact role/provider key,
so duplicates cannot inflate alternate count.

## 7. Alternate viability

The check requires provider existence, declared role capability, availability,
sender and receiver acceptance, no local revocation, accepted transaction
class, sufficient amount band, independent provider identity, and a working
typed request/result path. Registry presence alone is insufficient. Tests
reject an unsupported class, an excessive amount band, and an operational
replica presented as an independent authority.

## 8. Relationship-level compatibility

Both BSO policies must list the alternate for the exact role. A provider
accepted by only Alice is not viable for Alice-to-Bob. The no-shared-alternate
lane reports `Degraded/NoSharedAlternate` while the primary remains usable. A
separate disjoint-community lane becomes healthy after both policies accept
`identity:bridge`; the bridge receives one continuity check and zero hot-path
transactions.

## 9. Check scheduling

Maintenance runs only when a relationship's proof is absent, non-healthy, or
older than its role's CheckInterval. It uses deterministic logical time. There
is no global scheduler, coordinator, background service, goroutine per
relationship, or continuous provider probing.

## 10. Dry-run semantics

`ContinuityCheckRequestV1` carries a dedicated ContinuityCheckID, relationship,
role, provider IDs, class, amount band, and logical time. It carries no
TransferID, purchase content, balance, or relationship history. Every check
reports `FinancialMutation=false`; it creates zero reservation, transfer, or
balance mutation and one bounded continuity proof record.

## 11. Proof staleness

An expired proof produces `Degraded/ProofExpired`. Low-risk advisory payments
may continue while the primary works. If the primary fails, stale evidence is
never used directly: failover synchronously exercises the full check path
again. The stale adverse lane made four bounded revalidation calls and failed
safely because no Risk substitute remained viable.

## 12. Provider/version invalidation

Changing either BSO policy version invalidates the affected proof. Changing a
provider policy or schema version also invalidates it even when ProviderID is
stable. Both paths are covered by focused tests. Revocation produces the typed
reason `ProviderRevoked`; availability and policy compatibility remain
separate reasons.

## 13. Recurring continuity

The Patreon-like relationship makes 100 payments. Its ordinary path uses 102
provider calls: one payment-scoped Risk call per payment plus the initial
reusable Identity and Authorization calls. Continuity maintenance adds three
calls and three records, not one alternate check per payment. Provider
replacement changes future provenance only and requires no balance,
subscription, or historical-payment migration.

## 14. Threshold continuity

The bounded 2-of-3 Risk lane selects `general:a + risk:b` normally and verifies
independent `risk:c`. Removing `general:a` leaves `risk:b + risk:c`, so the
threshold remains satisfiable. Duplicate proofs count once; `risk:a-replica`
counts zero because replicas improve operational availability, not authority
substitutability.

## 15. Bundled-provider continuity

`general:a` supplies Identity, Risk, and Authorization during normal service.
On removal, each role revalidates and fails over independently to its
specialist. Role decomposition does not migrate financial state and does not
fail over unrelated roles.

| Role | Original provider | Replacement provider | Revalidation needed | Settled |
| ---- | ----------------- | -------------------- | ------------------- | ------- |
| Identity | general:a | identity:b | yes | yes |
| Risk | general:a | risk:b | yes | yes |
| Authorization | general:a | authorization:b | yes | yes |

## 16. Metadata minimization

Checks reveal only a synthetic check ID, relationship reference, role,
provider IDs, class, amount band, and logical time. Alternates receive no live
transfer ID, actual purchase amount, application content, full BSO profile, or
transaction history. The amount band proves scope without revealing a real
payment amount.

## 17. Privacy cost

Continuity improves failure readiness by exposing small compatibility records
to three specialists. This spreads some metadata beyond the one live provider,
but it does not spread live transaction metadata. Provider retention is a
dedicated bounded continuity record, not a fake payment or transfer-like
history. The 600-relationship workload retains exactly 1,800 provider-side
check records, one per role and relationship; the table below is per recurring
relationship.

| Strategy | Providers seeing live transaction metadata | Providers seeing continuity metadata | Retained continuity records |
| -------- | -----------------------------------------: | -----------------------------------: | --------------------------: |
| Live primary + synthetic alternate check | 1 | 3 | 3 |
| Lazy alternate activation | 1 | 3 | 3 |
| Multi-provider hot path (comparison only) | 4 | 0 | 0 |

## 18. Healthy-dominant workload

All 600 relationships settle through `general:a`; none use an alternate on the
ordinary path. One initial check per role verifies specialist alternatives.
Primary usage is 100%, coverage 100%, and the initial maintenance pass takes
4,522 microseconds in the recorded run.

## 19. Monopoly rerun

The M2 workload is fixed before provider removal: 600 three-role relationships,
one dominant bundled primary, and two policy-admissible specialists per role.
Removing the primary after checks preserves 600/600, versus M1's 0/600.

## 20. Stale-escape workload

`risk:b` starts valid, then changes policy/schema version and becomes
unavailable without another check. Its proof expires before `general:a` fails.
Restore-day revalidation rejects it; with `risk:c` unavailable, the transaction
does not settle. Stale evidence never broadens trust or authorizes settlement.

| Scenario | Proof current? | Alternate actually valid? | Primary fails | Result |
| -------- | -------------- | ------------------------- | ------------- | ------ |
| Silent rot before restore | no | no | yes | fail; revalidate and reject |
| Periodic check before restore | yes | yes (`risk:c`) | yes | settle through remediated alternate |

## 21. Periodic-check workload

The matching proactive lane runs maintenance at logical time 75, after the
50-unit Risk interval but before the 100-unit proof lifetime. The check detects
invalid `risk:b`, deterministically tries the next already-admissible alternate,
and records `risk:c`. Primary failure at time 80 then succeeds without surprise.

## 22. Patreon-like recurring workload

| Workload | Payments | Primary provider calls | Continuity calls | Extra durable records | Extra time |
| -------- | -------: | ---------------------: | ---------------: | --------------------: | ---------: |
| 600 dominant relationships | 600 | 1,800 | 1,800 | 1,800 | 4,522 us |
| Patreon-like recurring | 100 | 102 | 3 | 3 | 0 us measured |

The first row is the one-time establishment tax across 600 relationships, not
per-transaction alternate use. It emits 573,600 canonical proof bytes, or 956
bytes per three-role relationship in this representation.

## 23. Bundled-provider failover

All three roles replace `general:a` with specialists. The modeled coordination
cost is three check calls/rounds instead of one bundled contact; live metadata
remains role-shaped. All three role resolutions settle.

## 24. Threshold failover

The one 2-of-3 workload settles after losing one active authority. Independent
provider identities are counted exactly once and operational replicas do not
fill the spare-authority slot.

## 25. No-shared-alternate workload

The primary remains available, so advisory ordinary work can continue, but
continuity is already `Degraded/NoSharedAlternate` and the relationship is
counted captive. This makes the blast radius visible before an outage.

## 26. Advisory versus strict continuity

The same degraded state admits one low-value advisory transaction and blocks
one strict high-value transaction. The blocked transaction causes zero
financial mutation. Strictness is local and proportional; it is not a global
continuity requirement.

## 27. Direct trust

All 100 direct proposals settle with zero provider calls and zero continuity
checks. M2 does not invent intermediary escape paths for direct relationships.

## 28. Continuity coverage

`ContinuityCoverage = relationships with at least one currently verified
alternate for every required role / relationships dependent on an external
provider`. The dominant lane measures 600/600, or 100%.

| Relationship class | Primary | Verified alternates | Health | Last check | Captive |
| ------------------ | ------- | ------------------: | ------ | ---------- | ------- |
| Dominant recurring | general:a | identity:b, risk:b, authorization:b | Healthy | 10 | no |
| No shared alternate | general:a | 0 | Degraded | 0 | yes |
| Direct trust | none | N/A | Healthy | 0 | no |
| Threshold 2-of-3 | general:a + risk:b | risk:c | Healthy | 0 | no |

## 29. Captive relationships

`CaptiveRelationships` counts relationships whose primary can be removed and
which have no currently admissible verified replacement path. It falls from
600 in the M1 monopoly lane to zero in the measured M2 dominant lane. The
separate no-shared-alternate specimen remains correctly classified captive.
Continuity debt is zero immediately after the measured maintenance pass.

## 30. Failover success

`FailoverSuccess = transactions settling after primary removal / transactions
settling before removal`. M1 measures 0/600 = 0%; M2 measures 600/600 = 100%.
The adverse stale lane is intentionally outside that healthy-coverage ratio and
demonstrates why current evidence matters.

## 31. Continuity tax

Normal transactions remain at three provider calls. Initial readiness adds
three calls, three rounds, three records, and 956 canonical bytes per
three-role relationship. Across recurring operation, 100 payments need only
three continuity calls. The tax therefore resembles periodic restore testing,
not multi-provider hot-path consensus.

## 32. Blast radius

| Provider removed | Relationships depending on it | Recovered | Captive | Unrelated touched |
| ---------------- | ----------------------------: | --------: | ------: | ----------------: |
| general:a | 600 | 600 | 0 | 0 |

Continuity failure, policy revision, and provider invalidation remain scoped to
the affected relationship and role. Financial and relationship state remain in
the BSOs.

## 33. M1 versus M2

| Metric | TRUST-M1 monopoly | TRUST-M2 continuity |
| ------ | -----------------: | ------------------: |
| Pre-removal settled | 600 | 600 |
| Post-removal settled | 0 | 600 |
| Failover success | 0% | 100% |
| Captive relationships | 600 | 0 |
| Provider calls per ordinary transaction | 3 | 3 |
| Continuity maintenance calls | N/A | 1,800 initial / 3 per 100-payment relationship |
| Financial errors | 0 | 0 |

M1's 1,800 monopoly provider calls and zero post-removal settlements are taken
from the repository's actual `provider monopoly` result, not reconstructed.

## 34. Correctness invariants

Verified invariants are:

- checks mutate zero balance, reservation, or transfer state;
- stale proofs trigger revalidation and never directly authorize failover;
- BSO policy and provider policy/schema version changes invalidate proofs;
- provider replicas count as zero independent alternates;
- alternate ordering is deterministic and never broadens policy;
- no-shared-alternate and continuity failures remain relationship/role-local;
- duplicate proof IDs do not inflate alternate count;
- 2-of-3 continuity counts independent identities exactly once;
- direct trust performs zero continuity-provider work;
- advisory degradation may settle, while strict degradation blocks before any
  financial mutation;
- M1/M0 conservation, idempotency, migration, and financial-authority paths
  remain green;
- the complete Go repository suite passes;
- the Oct contract runs four compiled cases with zero interpreted fallback and
  zero diagnostics in 595 ms;
- the bridge restores continuity with zero hot-path calls;
- the normal experiment completes in 82 ms, below the 60-second budget.

## 35. Architecture decision

**A. Latent substitutability preserves federation even under heavy provider
concentration.**

## 36. Failover decision

**F1. Periodically tested alternates materially improve provider-removal
resilience.**

## 37. Concentration decision

**C1. High provider concentration is compatible with low captivity.**

## 38. Recurring-platform decision

**P1. Recurring relationships remain portable with low amortized continuity
cost.**

## 39. Privacy decision

**R1. Continuity readiness can be maintained with minimal additional metadata
exposure.** Alternates learn bounded compatibility metadata, but not live
transaction IDs, amounts, content, or history.

## 40. Experiment decision

**E1. Trust continuity is robust enough to move on to richer
application/service experiments.**

## 41. Exactly one next recommendation

Test application-level service continuity while retaining relationship-local,
role-scoped continuity proofs.
