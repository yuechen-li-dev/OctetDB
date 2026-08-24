# BSO-TRUST-M1 — Provider Federation, Competition, and Concentration

## 1. Verdict

**Meaningful progression.** Provider plurality remains operational in balanced,
preferred, outage, recurring, bridge, bundled, and separation workloads, but a
deliberately sticky policy population exposes one bounded structural weakness:
when 90% of BSOs accept only the popular provider family, removing it loses all
600 proposals even though other providers remain registered. That exceeds the
predeclared de facto-mandatory threshold of more than 80% loss.

The ordinary preferred-provider lane is healthier: all 600 proposals settle;
removing all three role-primary providers leaves 420 successful (70%
resilience), and saturation produces 1,110 fallbacks rather than a throughput
cliff. The experiment remains deterministic, preserves the unchanged M0
financial path and conservation, and completes in about 80 ms.

## 2. TRUST-M0 baseline

M1 executes the unchanged `experiments/BSOTrust/M0` suite before federation
measurements. Its real BSO path reports eight successful settlements, one
pre-reservation rejection, conservation true, and correctness true. M1 neither
copies nor replaces balance mutation. Providers and the federation directory
hold no `durableBSO`, OctetDB transaction, scheduler, or financial-state handle.

M0's expiry, revocation, exact threshold, idempotency, stable attestation
identity, checkpoint migration, and typed Octagon assertions remain the
baseline contracts rather than being weakened in M1.

## 3. Federation question

Plural provider IDs are not sufficient evidence. M1 asks whether local policy
intersection, user preference, bounded service capacity, caching, and outage
fallback leave providers replaceable under network-effect pressure. Popularity
is measured separately from authority: no provider owns money or settlement.

## 4. Provider attributes

`ProviderProfileV1` carries a stable ProviderID, declared roles, availability,
latency cost, abstract service cost, reliability from 0 to 100, policy version,
and requests-per-logical-round capacity. The population has three specialists
for each of Identity, Risk, and Authorization plus one bundled provider, for ten
provider profiles. Costs are small deterministic integers; there are no prices,
fees, auctions, rewards, tokens, or profit model.

## 5. Provider preference

Each `TrustPolicyV2` stores ordered `TrustPreferenceV1` values per role. A
provider must appear in both local policies, declare the requested role, remain
unrevoked, and satisfy separation rules before preference is considered.
Unavailable or saturated preferred providers yield to lower-ranked compatible
providers. No diversity quota changes the result.

## 6. Provider selection

The deterministic order is:

1. exact sender/receiver policy intersection;
2. declared role authority and local revocation;
3. optional provider separation;
4. summed sender/receiver preference rank;
5. bounded score `rank*1000 + serviceCost*10 + latency + (100-reliability)`;
6. stable ProviderID tie-break;
7. availability and remaining per-round capacity, with fallback in score order.

The 1,000-point rank weight makes the bounded cost dimensions incapable of
overriding policy preference. The compiled Oct contract tests this explicitly.

## 7. Provider capacity

Specialists have capacities of seven or eight requests per logical round. The
bundled provider has capacity 60, enough for twenty three-role proposals while
remaining bounded. Preferred-provider saturation causes 1,110 fallback
selections across 600 proposals; all 600 still settle. Balanced preference
causes only six capacity fallbacks.

Capacity is operational service state, not trust consensus. The experiment
does not count service replicas as distinct trust authorities.

## 8. Provider outage/recovery

One provider, all role primaries, a mid-workload outage, a bundled-provider
outage, and a deliberately monopolized provider family are measured. An
unavailable provider issues no evidence. Failed admission has
`FinancialMutation=false`; trust collection precedes the unchanged M0 reserve
path.

When a provider recovers, a later resolution can rank it again. A resolution
already admitted through a fallback retains its selected provider; selection is
not migrated mid-transaction. This follows from immutable resolution results
and fresh per-proposal selection state.

## 9. Trust cache/reuse

The cache key is exactly `(ProviderID, Role, Subject, Scope, PolicyVersion)` and
each entry includes validity. Identity and Authorization use the recurring
application reference as scope and remain reusable for eight logical rounds.
Risk uses the exact TransferID and therefore refreshes for every payment.
Unavailable, revoked, expired, wrong-subject, wrong-scope, or wrong-version
evidence is not reused.

## 10. Recurring portability

Five creator/patron payments settle. Identity A disappears and is revoked
before payment 4. Identity B is selected for future evidence without moving
balances or rewriting payments 1–3. The switch costs one extra fresh call on
payment 4; financial interruption is zero.

## 11. Provider concentration

The preferred-provider lane records every selected attestation by role. Top-1
share is 40% for Identity and Authorization and 35% for Risk. Capacity, rather
than an artificial quota, prevents the first-ranked providers from serving all
requests and makes fallback visible.

| Role | Provider | Attestations | Share |
| ---- | -------- | -----------: | ----: |
| Identity | identity:a | 240 | 40% |
| Identity | identity:b | 240 | 40% |
| Identity | identity:c | 120 | 20% |
| Risk | risk:a | 210 | 35% |
| Risk | risk:b | 210 | 35% |
| Risk | risk:c | 180 | 30% |
| Authorization | authorization:a | 240 | 40% |
| Authorization | authorization:b | 240 | 40% |
| Authorization | authorization:c | 120 | 20% |

| Role | Top-1 share | Top-3 share | HHI | Active providers |
| ---- | ----------: | ----------: | --: | ---------------: |
| Identity | 40% | 100% | 0.360 | 3 |
| Risk | 35% | 100% | 0.335 | 3 |
| Authorization | 40% | 100% | 0.360 | 3 |

## 12. Network effects

Acceptance of provider family `:a` increases from 20% to 100%. Compatibility
improves, popular-provider share increases, and outage resilience falls. The
share stops below 40% because bounded capacity forces honest fallback; it is not
randomized or quota-limited.

| Acceptance | Compatibility rate | Popular-provider share | Outage resilience |
| ---------: | -----------------: | ---------------------: | ----------------: |
| 20% | 75% | 6.4% | 93.3% |
| 40% | 82% | 17.5% | 85.4% |
| 60% | 88% | 25.7% | 79.5% |
| 80% | 94% | 32.4% | 74.5% |
| 100% | 100% | 38.3% | 70.0% |

The useful compatibility network effect therefore has a measured resilience
cost. In the separate monopoly lane, 90% of policies accept only `:a`; removing
`:a` loses 600/600 proposals. This is de facto mandatory under the threshold
defined before measurement, even though the provider still has no financial
authority.

## 13. Bundled providers

Bundling reduces distinct provider contacts from 300 to 100 and modeled
logical coordination rounds from 200 to 100 for 100 proposals. All proposals
settle in either model. Removing the bundle also leaves 100/100 successful via
specialists, demonstrating that operational convenience need not imply policy
captivity when alternatives are accepted and provisioned.

| Model | Providers contacted | Logical rounds | Metadata concentration | Settled |
| ----- | ------------------: | -------------: | ---------------------: | ------: |
| Specialist | 300 | 200 | 7 fields at one provider | 100/100 |
| Bundled | 100 | 100 | 16 fields at one provider | 100/100 |

## 14. Specialist-provider metadata separation

`Fields received` is provider request-schema width, not cumulative field
events. Identity and Authorization see subjects but no transfer IDs. Risk sees
transfer IDs but no subject profile. The bundle sees the union. Retained records
are attestations; no application payload is sent or retained.

| Provider type | Fields received | Subjects seen | Transfers seen | Retained records |
| ------------- | --------------: | ------------: | -------------: | ---------------: |
| Specialist Identity | 4 | 100 | 0 | 100 |
| Specialist Risk | 7 | 0 | 100 | 100 |
| Specialist Authorization | 5 | 100 | 0 | 100 |
| Bundled | 16 | 100 | 100 | 300 |

## 15. Policy islands

Two 50-BSO groups accept disjoint Identity providers. All 200 deterministic
cross-group proposals fail trust admission, create no M1 financial mutation,
and never reach M0 reservation.

## 16. Bridge providers

Adding Identity C to both local policy communities restores all 200 cross-group
proposals. The bridge has bounded capacity equal to one 20-proposal logical
round so this lane isolates compatibility; overload is measured in the
preferred-provider workload. Identity C gains no Risk, Authorization, ledger,
or global-policy authority.

| Scenario | Compatible proposals | Incompatible | Bridge/fallback resolved | Compatibility rate |
| -------- | -------------------: | -----------: | -----------------------: | -----------------: |
| Incompatible trust islands | 0 | 200 | 0 | 0% |
| Bridge provider added | 200 | 0 | 200 | 100% |

## 17. Direct trust

All 100 direct proposals settle with zero provider calls, zero attestations,
and zero provider metadata. Providers are not consulted merely because they
exist; `direct` is not assigned a default provider.

## 18. High-value threshold/separation

The 100 high-value proposals require Identity, Risk, and Authorization and set
`SeparateProviders=true`. All settle, with no provider reused across roles in a
single proposal. The M0 2-of-3 Risk workload remains green and supplies the
exact distinct-attestation threshold invariant; M1 adds provider scoring and
separation without redefining threshold semantics.

## 19. Scheduler/TransactionAgent integration

Federation selection remains TransactionAgent admission work. The Scheduler
places workers; the coordinator receives no provider directory, cache, or
financial message-routing responsibility. M1 creates no `TrustCoordinator`,
provider goroutine per request, background service, or scheduler hot-path
dependency.

## 20. Octagon DTO additions

The typed Oct contract adds `ProviderDirectoryEntryV1`,
`ProviderSelectionV1`, `ProviderAvailabilityV1`, and `TrustCacheEntryV1`.
Provider and cache records are data only. Existing M0 attestations and
resolution Octagon remain canonical; there is no JSON machine-protocol
regression.

The adjacent Oct checkout ran the contract with execution identity
`gooct-cli`, Oct version `dev`, requested execution `compiled`, three compiled
cases, zero interpreted fallbacks, zero diagnostics, and 549 ms tool timing.

## 21. Balanced federation workload

One hundred BSOs rotate first preference across three providers per role. All
600 proposals settle. The lane records 1,806 provider calls, six capacity
fallbacks, and nine eligible provider-role candidates per proposal.

## 22. Preferred-provider workload

All 100 BSOs rank provider A first for each role while retaining B and C. All
600 proposals settle. Bounded primary capacity produces 1,110 fallbacks and
3,330 provider calls, demonstrating graceful overload rather than forced equal
share.

## 23. Outage workload

| Provider removed | Pre-removal success | Post-removal success | Fallbacks | Resilience |
| ---------------- | ------------------: | -------------------: | --------: | ---------: |
| All role-primary `:a` providers | 600 | 420 | 1,320 | 70% |
| Risk A | 600 | 420 | 960 | 70% |
| Risk A midway | 600 | 510 | 480 | 85% |
| Bundled provider A | 100 | 100 | 300 | 100% |
| Monopoly `:a` family | 600 | 0 | 0 | 0% |

Fallback succeeds where local policies accept alternatives and those
alternatives have capacity. It cannot repair an explicit one-provider policy.
Direct and unrelated-provider transactions affected: zero.

## 24. Patreon recurring workload

| Payment # | Fresh provider calls | Cached attestations | Provider changes | Settled |
| --------: | -------------------: | ------------------: | ---------------: | ------- |
| 1 | 3 | 0 | 0 | yes |
| 2 | 1 | 2 | 0 | yes |
| 3 | 1 | 2 | 0 | yes |
| 4 | 2 | 1 | 1 | yes |
| 5 | 1 | 2 | 0 | yes |

Fresh calls per payment fall from three to one during stable operation. The
payment-4 provider replacement temporarily requires two fresh calls but no
financial interruption.

## 25. Provider-portability workload

The relationship begins with Identity A. A is unavailable and locally revoked
before payment 4; compatible Identity B supplies future evidence. Historical
payments retain A provenance through immutable prior resolutions. No financial
state, subscription identity, or TransactionAgent history migrates to B.

## 26. Bundling workload

The bundled profile declares exactly Identity, Risk, and Authorization. It may
be selected only for those roles. The separation lane prevents it from serving
multiple roles in the same high-value proposal. Its failure falls back to
specialists with 100% availability in the measured workload.

## 27. Trust-island workload

The island lane distinguishes provider existence from usable intersection.
Three Identity providers exist throughout, but disjoint local acceptance yields
zero compatible paths until the two communities independently accept the
scoped bridge.

## 28. Concentration results

Normal preference produces measurable but not mandatory concentration: top-1
share reaches 35–40%, top-3 reaches 100%, and saturation activates competitors.
The monopoly lane proves that local policy convergence can still collapse
nominal federation. Concentration alone is not failure; removal loss is the
deciding evidence.

## 29. Resilience results

The ordinary popular-provider family has 70% removal resilience, partial
mid-workload loss has 85%, and the bundle has 100%. The monopoly lane has 0%
and therefore crosses the de facto-mandatory threshold. Provider failure blast
radius follows policy dependence: direct and unrelated transactions touched
remain zero.

## 30. Metadata results

Specialists preserve role-shaped views. The bundle cuts coordination in half
but observes both subject and transfer identifiers plus all 16 modeled trust
fields. This supports a convenience/concentration tradeoff, not a universal
privacy score.

## 31. Correctness invariants

Verified invariants are:

- deterministic selection and stable ProviderID tie-break;
- hard policy, role authority, revocation, separation, availability, and
  capacity precede scoring;
- outage and incompatibility cannot mutate money;
- direct proposals consult no provider;
- exact cache scope and validity; Risk never reuses across TransferIDs;
- fallback does not count unavailable providers as attestations;
- provider replacement does not rewrite historical provenance;
- a bundle cannot serve undeclared roles or bypass separation;
- policy islands cannot settle without intersection;
- one provider's failure touches zero direct/unrelated transactions;
- local policy objects change no unrelated policy (`PolicyLocalityTouches=0`);
- M0 conservation, idempotency, threshold, migration, and financial-authority
  invariants remain green;
- normal measured runtime is about 80 ms, well below 60 seconds.

## 32. Architecture decision

**B. Federation works, but network effects produce one clearly dominant
dependency that deserves mitigation.**

## 33. Concentration decision

**C2. Concentration materially harms availability even though alternatives
exist.**

## 34. Recurring-platform decision

**P1. Recurring creator/patron relationships remain portable across providers
with bounded switching cost.**

## 35. Data decision

**R2. Bundled providers are operationally attractive but materially increase
metadata concentration.**

## 36. Experiment decision

**E2. Improve one measured federation resilience/selection weakness first.**

## 37. Exactly one next recommendation

Add a local policy-lint and bridge-discovery warning when deterministic provider
removal simulation predicts more than 80% proposal loss, without allowing the
directory to attest, rewrite policy, or mutate financial state.
