# BSO-TRUST-M0 — Federated Trust Roles, Attestations, and Policy Matching

## 1. Verdict

**Success.** Two independently durable BSOs resolve local, role-scoped policies, collect a minimal mutually accepted set of typed attestations, and then use the unchanged BSO settlement protocol. The experiment admits eight workloads, rejects the one incompatible workload before reservation, preserves 800,000 total units, and records zero double debits, double credits, unresolved transfers, or settlement-before-trust violations.

## 2. Motivation

BSO-SIM-M0 through M2 established durable financial authorities and migratable transaction protocol agents. M0 asks whether useful intermediaries can be added without turning one intermediary into the bank, ledger, scheduler, or canonical transaction database. The answer is tested at the admission boundary before `durableBSO.reserve`.

## 3. Federated-trust thesis

Trust is explicit bounded application data. A BSO says which provider identities it accepts for a closed role, transaction class, amount ceiling, threshold, policy version, and lifetime. The TransactionAgent intersects the two local policies and collects attestations. Providers are replaceable because provider identity is separate from placement and transport. Direct transfers under both BSOs' local limit need no provider.

This is federated trust, not trustlessness. It is also not a generic executable policy language.

## 4. Authority separation

| Concern | Authority |
| --- | --- |
| money/balance | BSO |
| transaction protocol | TransactionAgent |
| placement | Scheduler |
| identity fact | Identity provider |
| risk fact | Risk provider |
| authorization fact | Authorization provider |
| dispute decision | Dispute provider if implemented |

`durableBSO` alone holds an OctetDB database and balance-mutating methods. `TransactionAgent` holds protocol/trust state but no database transaction. `TrustRegistry` holds providers but no BSO or scheduler reference. Provider input is only `TrustRequestV1`; a reflection-backed isolation test rejects forbidden provider authority fields.

## 5. Trust roles

`TrustRole` is closed over `Identity`, `Risk`, `Authorization`, `Escrow`, and `Dispute`. M0 executes Identity, Risk, and Authorization. Escrow and Dispute have typed DTOs but no custody or adjudication workflow; enabling those workflows without a motivating case would exceed the bounded milestone.

`TransactionClass` is closed over Direct, Purchase, Subscription, Donation, Marketplace, and InstitutionSettlement.

## 6. Provider model

Stable semantic IDs include `trust:identity:acme`, `trust:risk:safepay`, and `trust:authorization:patron`. `TrustProviderCapabilitiesV1` declares a provider's roles and policy version. Issuance rejects a provider asked to attest an undeclared role. Availability is a deterministic registry fact; Marketplace makes Acme Identity unavailable to exercise fallback.

Repeated requests use the semantic key `(provider, role, subject-or-transfer, class, policy version, scoped application reference/amount)`. The resulting AttestationID is stable. A retry returns the cached result while valid and refreshes expired evidence without proliferating identities.

## 7. Attestation DTOs

The implementation defines typed `IdentityAttestationV1`, `RiskAttestationV1`, `AuthorizationAttestationV1`, `EscrowAttestationV1`, and `DisputeAttestationV1`. Identity retains subject and level; risk retains transfer decision/reason; authorization retains subject, class, maximum amount, and opaque application reference.

Risk decisions are Approve, Reject, or Challenge. Challenge is terminal-unsatisfied in M0; there is no MFA workflow. Dispute decisions are typed, but no dispute workload is enabled.

## 8. TrustPolicy / TrustRule

Each `BSOState` durably embeds its own `TrustPolicyV1`. A rule carries role, accepted provider IDs, threshold, maximum amount, transaction class, and validity. The BSO database persists replacements only when their version advances. A test closes and reopens a BSO and observes its version-2 policy and revocation.

The Oct contract uses refined Concepts for `PositiveThreshold`, `NonNegativeMaxAmount`, and `ValidPolicyVersion`. Runtime Go validation enforces the same bounded input constraints at the persistence boundary.

## 9. Policy intersection

Intersection is deterministic and deliberately boring:

1. Validate both policy lifetimes.
2. Admit Direct only if the amount is below both local direct limits.
3. For each closed role in fixed order, select the class/amount rule on both sides.
4. Reject if only one side requires the role.
5. Preserve sender provider order while intersecting receiver acceptance, revocations, and declared capabilities.
6. Use the stronger of the two thresholds and reject if the intersection is too small.

| Case | Sender policy | Receiver policy | Intersection | Result |
| --- | --- | --- | --- | --- |
| shared identity | Acme, Backup | Acme, Backup | Acme, Backup | Acme selected |
| incompatible donation | Acme only | Other only | none | rejected before reserve |
| Marketplace outage | Acme, Backup | Acme, Backup | both; Acme unavailable | Backup selected |
| institutional risk | A, B, C; 2 required | A, B, C; 2 required | A, B, C | A and C approvals admit |

There is no global trust authority and no global policy store.

## 10. Threshold rules

Duplicate provider identities and duplicate AttestationIDs cannot increase the count. Only valid approvals from distinct accepted providers count.

| Providers | Decisions | Threshold | Result |
| --- | --- | ---: | --- |
| A, B, C | approve, reject, approve | 2 | admitted |
| A, B, C | approve, reject, reject | 2 | rejected |
| A | challenge | 1 | unsatisfied |

Two-of-three risk is not consensus. It means two acceptable attestations satisfy these two BSOs' local policy for one transfer. It establishes no global fact or replicated log.

## 11. Provider fallback

Candidate order is policy order. An unavailable provider produces no attestation and collection advances to the next mutually accepted provider. The Marketplace workload consults Acme, observes outage, selects Backup, and records exactly one fallback use. Threshold collection is not mislabeled as fallback: risk C is a second required approval, not a fallback for A.

## 12. Expiry/revocation

Attestations carry logical `IssuedAt` and `ValidUntil`. Registry reuse checks validity before returning evidence; the expiry test advances beyond `ValidUntil`, observes one expiry rejection, and receives refreshed evidence. A BSO revocation removes that provider from future intersections. A captured version-1 admitted resolution remains unchanged after a version-2 policy revokes its providers.

Completed financial history is never retroactively invalidated.

## 13. TransactionAgent integration

The migratable agent explicitly stores transaction class, application reference, required roles, per-role thresholds, deterministic candidates, selected providers, collected AttestationIDs, trust cursor, TrustResolutionID, policy versions, and per-transfer provider/reuse counters.

State proceeds through `Created -> TrustCollect -> TrustAdmitted -> existing reserve/offer/commit/acknowledge`. `PhaseTrustAdmitted` is the only trust phase allowed to call `reserve`; an absent resolution increments a hard invariant counter and fails. Obvious incompatibility terminates from `Created` without any BSO transfer record.

## 14. Octagon representation

Go emits data-only Octagon for `TrustPolicyV1`, `TrustRuleV1`, `TrustProviderCapabilitiesV1`, the three active attestations, and `TrustResolutionV1`; optional Escrow and Dispute records are declared in Oct. Resolution `.octagon` files are persisted before settlement. Checkpoint Octagon includes pending trust state.

Authentication stubs hash the canonical unsigned Octagon bytes plus a deterministic provider secret. They never stringify JSON. Stable hashes provide an explicit future signature seam.

The executable `trust_policy.octest` defines all mandatory typed/versioned DTOs and implements intersection counting, exact approval thresholding, and expiry. The real Oct CLI (`octVersion=dev`, execution identity `gooct-cli`) ran five compiled cases with zero interpreted fallbacks in 864 ms.

## 15. Data minimization

Provider-facing `TrustRequestV1` contains transfer IDs, participant BSO IDs, amount, class, logical time, subject where relevant, and an opaque application reference. It has no application content or BSO history.

Retention is role-shaped:

- Identity/Authorization retain subject metadata but no TransferID.
- Risk retains TransferID and decision/reason but no subject profile.
- All retain only provider/attestation identity, policy version, and bounded timestamps.

The retention test walks every retained record. A service can only provide metadata it retained. No legal-request workflow is modeled.

## 16. Direct-trust workload

`trust:direct` transfers 3 units under both BSOs' 5-unit Direct limit. It consults zero providers, uses zero attestations, persists a direct-admission resolution, then executes the ordinary BSO protocol.

## 17. Purchase workload

`trust:purchase` transfers 20 units with shared Acme Identity and SafePay Risk rules. It consults two providers, uses two attestations, captures both policy versions, and settles.

## 18. Patreon-like recurring workload

Patron `bso:000002` pays creator `bso:000003` 10 units three times for `subscription:alice:bob`. Each recurrence begins trust resolution after the previous one is admitted. Identity and recurring Authorization AttestationIDs remain stable; risk remains transfer-specific.

| Payment # | Fresh provider calls | Reused attestations | Settled |
| ---: | ---: | ---: | --- |
| 1 | 3 | 0 | yes |
| 2 | 1 | 2 | yes |
| 3 | 1 | 2 | yes |

The provider set is already known from local policy, so recurrence performs no discovery. Cached identity/authorization evidence avoids fresh provider work; only risk is fresh per transfer.

## 19. High-value workload

`trust:high-value` transfers 1,000 units, above ordinary purchase scope. InstitutionSettlement requires Identity, Risk 2-of-3, and Authorization. Risk A approves, B rejects, C approves. Four attestations are used after five provider consultations, and the same BSO settlement protocol follows admission.

## 20. Incompatible-policy workload

`trust:incompatible` has Acme-only sender Identity policy and Other-only receiver policy. Resolution fails with `role identity has no sufficient provider intersection`. Tests reopen both durable BSOs and prove no outgoing or incoming transfer record exists. Money reserved: no. Money moved: no.

## 21. Provider-outage workload

`trust:fallback` consults unavailable Acme Identity, obtains Backup Identity, obtains SafePay Risk, and settles. There is no unbounded reservation because collection occurs before reserve.

| Failure | Trust resolution result | Money reserved? | Money moved? | Recovery |
| --- | --- | ---: | ---: | --- |
| incompatible provider sets | rejected | no | no | change a future local policy/provider set |
| primary outage with fallback | admitted through Backup | no, until admitted | yes, after admission | deterministic fallback |
| expired evidence | not counted; refreshed with a new AttestationID | no | no | re-attest |
| risk reject/challenge below threshold | rejected | no | no | future explicit proposal/challenge workflow |
| worker death while trust pending | pending state restored | no at death seam | yes after resumed admission | checkpoint migration |

## 22. Worker migration during trust resolution

The migration lane kills worker 1 in logical round 2, after agents can hold one collected attestation. The scheduler encodes `TransactionAgentCheckpointV1`, restores it on a live worker, and resumes from the saved cursor. The normal migration run reports two agents migrated, zero duplicate attestations counted, zero double debits/credits, conservation true, and correctness true.

## 23. Optional dispute/refund

`DisputeAttestationV1` and its closed decisions exist as a typed semantic boundary, but M0 does not execute adjudication. If enabled later, RefundApproved must propose a new transfer; it may not rewrite the original audit history. No escrow custody is implemented.

## 24. Coordination cost

The no-fault smoke run measures the unchanged successful settlement core at four protocol messages and five financial durable transitions per successful transfer. Every attempt adds one durable typed trust resolution. Logical rounds include admission and bounded worker scheduling.

| Case | Settlement messages | Provider calls | Financial durable transitions | Resolution transitions | Logical rounds |
| --- | ---: | ---: | ---: | ---: | ---: |
| direct BSO settlement | 4 | 0 | 5 | 1 | 9 |
| ordinary purchase | 4 | 2 | 5 | 1 | 12 |
| fallback purchase | 4 | 3 | 5 | 1 | 12 |
| high-value threshold | 4 | 5 | 5 | 1 | 15 |
| incompatible | 0 | 0 | 0 | 1 | 1 |

Trust increases only admission work; it does not add financial settlement messages or financial transitions.

The measured smoke lane completed in 85 ms. A standalone normal lane with the `fun` transport profile and a trust-pending worker migration completed in 65 ms, far below the 60-second budget.

## 25. Trust/provider reuse

Smoke totals are 21 provider consultations, 16 fresh attestations issued, 19 valid attestations used, four cached uses, one fallback, and one intersection failure. Used can exceed issued because stable valid evidence is reused. Providers consulted and attestations required are recorded per resolution.

| Workload | Required roles | Providers consulted | Attestations used | Admitted | Financial mutation |
| --- | --- | ---: | ---: | --- | --- |
| direct micro-transfer | none | 0 | 0 | yes | yes, after admission |
| purchase | Identity + Risk | 2 | 2 | yes | yes |
| subscription #1 | Identity + Risk + Authorization | 3 | 3 | yes | yes |
| subscription #2 | Identity + Risk + Authorization | 3 | 3 | yes | yes |
| subscription #3 | Identity + Risk + Authorization | 3 | 3 | yes | yes |
| incompatible | Identity | 0 | 0 | no | no |
| outage/fallback | Identity + Risk | 3 | 2 | yes | yes |
| high-value | Identity + Risk 2-of-3 + Authorization | 5 | 4 | yes | yes |
| migration | Identity + Risk | 2 | 2 | yes | yes |

## 26. Correctness invariants

The experiment and tests preserve:

- 800,000 initial and final value; no negative balance.
- Zero lost value, double debit, double credit, unresolved transfer, or unstable transaction identity.
- No settlement before required trust; incompatible policies create zero transfer state.
- Expired attestations do not authorize; revoked providers do not satisfy future policy.
- Resolutions capture both policy versions.
- Thresholds count exact distinct approvals; rejected/challenge evidence and duplicates do not count.
- Migration resumes pending trust evidence without endless new attestations.

## 27. Authority audit

The provider API is compile-time separated from `octetdb.Tx`, `durableBSO`, and `SchedulerCoordinator`. Provider retention lives outside BSO databases. The scheduler still only places/migrates TransactionAgents. The coordinator remains out of settlement message routing and is not the trust resolver. Providers cannot call a balance mutation method because no such capability crosses their API.

## 28. What intermediaries actually contribute

A Patreon-like service remains useful as a collection of separable functions: identity evidence, per-payment risk decisions, persistent recurring authorization, and a future dispute policy. It may also retain the opaque subscription reference needed to relate those facts. It does not need to own patron or creator balances, become the transaction log, place agents, or aggregate unrelated application content.

## 29. What this still does not model

M0 is deterministic simulation, not real KYC, AML, fraud detection, escrow, legal disputes, PKI, networking, service discovery, or clock synchronization. It introduces no blockchain, Lightning comparison, global consensus, smart contracts, anonymity, mixing, zero-knowledge privacy, onion routing, or wallet UI. Participants are attributable BSOs; privacy comes from collecting less and separating roles.

## 30. Architecture decision

**A. Federated typed trust composes cleanly with BSO settlement and preserves authority separation.**

## 31. Federation decision

**F1. Role-scoped interchangeable providers are a practical trust model.**

## 32. Patreon decision

**P1. A Patreon-like service can be decomposed into useful trust/policy services without owning participant financial state.**

## 33. Retention decision

**R1. Role-scoped metadata minimization is sufficient for the M0 trust architecture.**

## 34. Experiment decision

**E1. Trust federation is promising enough for deeper service-market/intermediary experiments.**

## 35. Exactly one next recommendation

Build one bounded provider-discovery/service-market experiment that resolves the same typed capability and policy intersection across independently registered provider endpoints, while retaining the current local-policy, pre-reservation, and provider-no-financial-authority invariants.
