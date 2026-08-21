# DBSCHED-M3: persistent utility priority/fairness

## 1. Verdict

**Success**

The optimized compiler is consumed correctly, F0 removes the old allocation pathology, H and F1 implement bounded priority plus aging above E's conflict-safety mechanism, and deterministic tests prove persistence and starvation protection. Utility scoring is viable but not preferable here: F1 is application-level equivalent to H, about 2.7x slower in an isolated nanosecond-scale policy step, and more complex to author across the Oct/Go boundary.

## 2. Questions tested

- Optimized persistent F rematch: yes; F0 is an oldest-legal parity controller owned once per scheduler.
- Priority/fairness scheduling: yes; H and F1 favor static priority while a 10 ms bound in the configured scheduler forces aged work.
- Utility-policy authoring: yes; H and F1 use the same actions and scores, allowing structure and runtime to be compared separately.

## 3. Compiler remediation consumed

Artifacts were generated with Oct `a997285fc006b29143c31f6391ca6f21a950adb2` using `experimental/octemit`. One concrete controller is constructed per scheduler, and every dispatcher decision mutates its typed board then calls `__octStep` on the same pointer. Construction count and board decision count are independently asserted.

F0 measures 6.298-6.340 ns/decision and F1 7.215-7.284 ns/decision, both at 0 B and 0 allocations per decision. Ordinary Go execution has no Oct runtime dependency.

## 4. F0 rematch

Old M2 F built a generic flow for every candidate decision and measured 3.8x allocations, 5.8x bytes, and 11.4% higher p99 than E. F0 instead calls one persistent specialized controller once per scheduler selection.

Across three rotated quick runs, E versus F0 was: 965.57 versus 965.51 ops/s, 3.451 versus 3.462 ms p99, 3.152 versus 3.182 ms queue p99, 2.085 versus 2.068 ms DB-service p99, 35.69 versus 35.66 alloc/op, and 2170.4 versus 2169.5 B/op. The old catastrophic tax is gone. Exact old/new performance is not claimed because M2 and M3 evidence durations differ.

## 5. Priority/fairness model

`PriorityClass` remains static-plan metadata: reads are class 0 and writes class 1. Go first removes candidates whose conflict token is owned. Among the legal set it derives oldest, highest-priority, aged, and best-batch candidates. Ordinary scores are high priority `300 + bounded age`, batch `200 + 25 * batch count`, and oldest `100 + bounded age`. Age contributes at most 100 points so it refines ordinary ordering without silently replacing the explicit starvation rule.

The starvation threshold is `max(5 * batch_wait, 5 ms)`, therefore 10 ms for the committed 2 ms configuration. An aged candidate is an emergency eligibility-preserving override. Equal scores use action source order; requests use age then sequence/FIFO ties.

Priority inversion is structurally absent from the current static plan: every command that can exclusively own a conflict token is PriorityClass 1. M3 therefore adds no priority inheritance or owner boost.

## 6. Conventional H policy

H is a conventional typed controller with four mutable fields: current action, current score, commitment age, and presence. It applies the same emergency override, two-decision minimum commit, 25-point hysteresis, and deterministic selection inputs as F1. The explicit imperative implementation contains separate emergency, eligibility, commitment/hysteresis, switch, and state-update branches.

## 7. Utility F1 policy

F1's bounded candidates are `Oldest`, `HighPriority`, `Aged`, `Batch`, and `Wait`. Conflict safety is not scored; its board contains summaries of legal candidates only. Three `when policy` cases express high-priority, batch, and oldest scores. An outer aged guard overrides commitment. `hysteresis: 25` and `min_commit: 2` persist in one typed policy-site field. Source order and request FIFO resolve ties deterministically.

## 8. Authoring comparison

Classification: **More complex**.

H has four controller-state fields and keeps observation derivation and state update in Go. F1 makes the three competing score rules visually compact, but requires nine board observation/result fields, generated-controller ownership, board mutation, and cross-language trace/instrumentation. Adding a score consideration is localized in Oct; adding a new candidate still requires Go eligibility derivation and action projection. In this bounded policy the result is clearer at the score site but less clear end-to-end.

## 9. Correctness/fairness evidence

Deterministic tests cover:

- a low-priority request losing before, then winning at, the documented starvation bound under both H and F1;
- an aged singleton overriding a batch opportunity;
- unsafe high-priority work being excluded before policy;
- four decisions on one F1 pointer, with one construction and persistent decision count;
- minimum-commit hold, hysteresis hold, and later switch on one generated controller;
- zero controller allocations per decision;
- same-token serialization, independent-token concurrency, no duplicate release, failure release, and cancellation release;
- final PostgreSQL equivalence for E, F0, H, and F1 (and retained historical lanes) in every run.

## 10. Benchmark methodology

Three independent quick runs used PostgreSQL 17, seed 20260821, eight connections/workers, queue 128, maximum batch 8, and a 2 ms batch wait. Each lane warmed with 5,000 operations, reset/reseeded PostgreSQL, then ran identical low, mixed, normal-before, hot-key overload, and normal-after phases totaling 6,750 attempts. Orders rotated E/F0/H/F1, F0/H/F1/E, and H/F1/E/F0. Tracing was captured outside timing. These short runs establish parity and allocation behavior; they are not a capacity-limit study because all lanes admitted every arrival.

## 11. Results

Arithmetic means of three runs:

| lane | completed | rejected | ops/s | p50 ms | p95 ms | p99 ms | p99.9 ms | queue p99 ms | DB p99 ms | alloc/op | B/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| E | 6750 | 0 | 965.57 | 1.538 | 3.049 | 3.451 | 5.642 | 3.152 | 2.085 | 35.69 | 2170.4 |
| F0 | 6750 | 0 | 965.51 | 1.534 | 3.050 | 3.462 | 4.715 | 3.182 | 2.068 | 35.66 | 2169.5 |
| H | 6750 | 0 | 965.51 | 1.530 | 3.046 | 3.571 | 5.144 | 3.176 | 2.067 | 35.66 | 2169.1 |
| F1 | 6750 | 0 | 965.50 | 1.530 | 3.041 | 3.349 | 4.485 | 3.120 | 2.053 | 35.65 | 2164.2 |

| lane | class-0 wait p95/p99 ms | class-1 wait p95/p99 ms | switches | hysteresis holds | min-commit holds | fairness overrides | overtakes |
|---|---:|---:|---:|---:|---:|---:|---:|
| E | 3.070 / 3.342 | 1.039 / 1.803 | 0 | 0 | 0 | 0 | 0 |
| F0 | 3.069 / 3.390 | 1.031 / 1.774 | 0 | 0 | 0 | 0 | 0 |
| H | 3.066 / 3.287 | 1.027 / 1.993 | 412.7 | 27.3 | 58.0 | 0 | 1953 |
| F1 | 3.063 / 3.260 | 1.015 / 1.587 | 422.7 | 22.7 | 54.0 | 0 | 1953 |

Maximum waits varied by timer noise; per-run values remain in evidence. No statistical run crossed the 10 ms starvation threshold, so fairness override count is correctly zero there; the adversarial deterministic test is the authority for activation. Policy microbenchmarks are F0 6.298-6.340 ns, H 2.655-2.671 ns, and F1 7.215-7.284 ns, all 0 B/0 alloc per decision. Scheduler CPU accumulation uses Windows wall-clock sampling and is too quantized for per-decision inference; the Go benchmark is authoritative.

## 12. Ablation

- E to H: priority policy produced 1,953 overtakes/run and exercised commitment/hysteresis, but no repeatable throughput or latency improvement in the arrival-limited workload. It adds deterministic starvation protection.
- H to F1: application behavior, allocations, bytes, batching, and throughput are equivalent. Isolated F1 costs about 4.6 ns more per decision and is more complex end-to-end.
- E to F0: throughput, p99, allocations, and bytes are effectively equal. The optimized-controller tax is approximately 6.3 ns and zero allocations.

## 13. Generated-Go audit

The committed artifact is 13,789 bytes and 563 lines for the admission flow plus both persistent controllers. It has zero imported packages (an empty import block), two typed `__octScalarUtilitySiteState[int]` fields, and no `reflect`, clone helper, range helper, generic utility map, `any` policy state, generic history storage, or resume storage. Freshness is checked with the exact Oct revision above.

## 14. Oct feature findings

| feature | classification | finding |
|---|---|---|
| persistent flow | Useful | correct scheduler lifetime and state persistence at zero allocation |
| `when policy` | Neutral | behaves correctly; no application advantage over H |
| hysteresis | Useful | deterministic holds and reduced small-score switching |
| minimum commit | Useful | deterministic two-decision service preference without blocking the age override |
| utility scoring | Inconclusive | compact local score rules, but more complex whole integration and no runtime gain |
| static `PriorityClass` | Useful | removes runtime registry construction and drives actual ranking |
| Concepts/validation | Neutral | M3 adds fixed typed constants, not new configurable static policy structure |

## 15. Interpretation

1. Priority/fairness is useful as a correctness property: it creates bounded starvation behavior and observable priority overtakes. This short workload does not show a repeatable performance benefit.
2. Utility scoring is not useful enough for this policy. It is viable and faithful, but conventional H is simpler and faster in isolation.
3. The optimized flow compiler is adequate for hot scheduling policy: 0 B/0 alloc and 6-7 ns decisions remove M2's representation pathology.
4. Persistent state expresses hysteresis and commitment cleanly at the Oct score site, but ordinary imperative H expresses the same behavior without difficulty at this policy size.

## 16. Limitations

Runs are deliberately short, arrival-limited, and produce no rejections; small latency differences are timer noise, not superiority claims. Priority classes are currently only two levels, and every exclusive token owner is in the same class, so real cross-priority inversion is not represented. The benchmark's wall-clock CPU counter is quantized on Windows. The fixed 10 ms bound is derived from batch wait rather than tuned under sustained saturation. F1 trace projection is application-authored because the generated internal interface exposes board snapshots but no public scored-candidate trace ABI.

## 17. Exactly one next recommendation

**Stop utility-policy work.** Keep E's mechanism, H's bounded aging semantics, and the now-validated compiler capability. Revisit `when policy` only if a future scheduler has enough genuinely competing considerations to offset the cross-language integration cost.
