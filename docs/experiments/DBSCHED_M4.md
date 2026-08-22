# DBSCHED-M4: delayed observation and short-horizon prediction

## 1. Verdict

**Success**

M4 measured the database control loop before designing a predictor, implemented a bounded Oct observer in shadow mode, compared prediction with persistence, tested one safe reactive actuator, and preserved PostgreSQL correctness. The result is negative for predictive control: at the measured 2 ms horizon, persistence beat bounded linear extrapolation in every workload regime. K therefore remained shadow-only. Filtering reduced queue-signal variance, but J's filtered exposure control did not improve H and increased mean p99. A negative hypothesis result is the successful outcome of this milestone.

## 2. Historical Prometheus reconstruction

The source inventory and concise reconstruction are retained in [`experiments/M4/reconnaissance/prometheus-sources.md`](../../experiments/M4/reconnaissance/prometheus-sources.md). Repository evidence and M4 inference are distinguished below.

Repository evidence:

1. M49/M49b Shadow-HSFM addressed deterministic numerical drift across a repeated four-block transformer stack. Its question was whether a selected reduced-precision/backend realization remained inside a bounded discrepancy envelope relative to a semantically matched all-A2x4 FP32 witness.
2. It observed sampled final-output coordinates, identities, finite agreement, L2/L-infinity discrepancy, signed projections, disagreement count, and confidence. Earlier M49 work also used bounded intermediate readbacks for research.
3. It did not filter a database-like queue. It summarized numerical discrepancy and used confirmation hysteresis, bounded history, confidence, cooldown, and quarantine. The distinct P14 timing path filtered raw GPU duration using bounded rolling facts and EMA/median/hybrid candidates.
4. Shadow state retained numerical regime, confirmation streaks, discrepancy projections/history, confidence, parameter/evidence identity, cooldown, rollout stage, and quarantine/audit state.
5. The P15 Smith-predictor work was separate from M49b. It predicted future GPU slot/shape/variant readiness and future-lease timing by tick.
6. It compensated for decision/resource preparation latency between requesting a future lease and physical readiness.
7. Its actuator path could request cancellable future reservations and, later, consume a matured matching reservation in agree-and-confirm mode. M49b instead recommended bounded precision/path stages.
8. Useful evidence included bitwise-stable GPU paths, bounded A2x4 discrepancy, real paired RTX witness captures, deterministic prediction maturation/correction tests, and lifecycle-safe default-off diagnostics.
9. Weak evidence included one RTX device, only 16 sampled final-output coordinates in M49b, no full Stage-2/3 hardware matrix, synthetic Oct mitigation selection, no broad DVT calibration, and no production override evidence.
10. Subsequent use stayed conservative because calibration and transfer remained incomplete and integration was expensive. P15 M10-M12 remained diagnostic/default-off; M13 found that production reservation maturation/expiry had not been continuously advanced and retained agree-and-confirm rather than override authority.
11. GPU-specific parts are activation/tensor discrepancy, precision promotion, backend fallback, shader/shape/variant identity, slot lifecycle, transfer/prestage, and future GPU leases.
12. General control ideas are raw/filtered truth separation, bounded persistent state, shadow-first deployment, delayed prediction maturation, correction/error accounting, confidence decay, hard safety gates, hysteresis, and cancellable bounded actuation.

M4 inference: the two historical systems should not be fused into a database “Shadow-HSFM.” Their transferable value is experimental discipline and bounded control structure, not their state vocabulary.

## 3. Database-system mapping

| historical idea | database adoption | decision |
|---|---|---|
| raw and interpreted evidence remain separate | raw queue/rates/delay and EMA estimates are distinct | adopted |
| bounded persistent observer | one fixed Oct record updated every 1 ms | adopted |
| delayed ring and correction at maturity | fixed three-entry ring matures 2 ms predictions exactly once | adopted |
| shadow authority first | K0 logs estimates, predictions, actuals, error, and would-limit decisions | adopted |
| confidence/authority gates | prediction must beat persistence before any actuator is possible | adopted as a hard experimental gate |
| tensor discrepancy HFSM | no database analogue with evidentiary support | rejected |
| precision/backend promotion | H has no such actuator | rejected |
| slot/shape/variant future lease | PostgreSQL workers turn over in roughly 1.5-2.1 ms and no preparatory resource lease exists | rejected |
| Smith dead-time compensation | no sufficiently stable, dominant dead time was measured | rejected |

Static command semantics, conflict ownership, capacity, and H's starvation override remain authoritative. Observer output never participates in legal eligibility.

## 4. Delay/plant characterization

H was instrumented outside authoritative timing at arrival, dispatch, service start, completion, and conflict release. Four runs used the real pgx/PostgreSQL path.

| regime | dispatch→completion p50/p95/p99 ms | queue p95 ms | mean peak pending |
|---|---:|---:|---:|
| steady low | 0.350 / 1.537 / 1.575 | 3.089 | 2.0 |
| linear ramp up | 0.048 / 1.555 / 1.642 | 2.871 | 6.5 |
| sharp burst | 1.019 / 2.005 / 2.206 | 2.323 | 11.2 |
| sustained overload | 1.043 / 2.075 / 2.167 | 8.869 | 29.8 |
| linear ramp down | 0.109 / 1.532 / 1.583 | 2.914 | 10.8 |
| hot-key relief | 0.324 / 1.553 / 1.788 | 3.006 | 2.0 |
| adversarial burst | 1.057 / 2.171 / 2.697 | 7.710 | 23.5 |
| adversarial relief | 0.327 / 1.551 / 1.843 | 3.089 | 11.5 |

Dispatch feedback is delayed, but only briefly. The p95 change from low load to overload is about 0.54 ms, while queue residence amplifies by about 5.8 ms. This justified evaluating a 2 ms predictor, not a long-horizon plant model. Load decrease recovered to low-load dispatch delay in the next phase; all authoritative lanes met the benchmark's two-window recovery criterion in 1.0 s.

## 5. Observable state

The selected state is intentionally minimal:

- pending legal/unresolved requests;
- arrivals during the last 1 ms tick;
- completions during the last tick;
- mean dispatch-to-completion delay for completed requests;
- active workers for physical diagnostics.

Queue-by-command, priority, conflict domain, pool wait, and hot-key identity were not added to the predictor because aggregate queue and completion delay already exposed the transition and a richer model had not earned complexity. Static worker maximum and command/conflict semantics come from the M1 plan.

## 6. Filtering

Queue and dispatch delay use EMA alpha 0.25; arrival and completion counts use alpha 0.5. Queue/delay alpha was selected as the smallest conventional smoother that visibly reduced 1 ms tick noise without a long memory relative to the measured plant. Its nominal low-frequency group delay is `(1-alpha)/alpha = 3` samples, or approximately 3 ms. Rate estimates use the faster filter to follow transitions.

Across K0 runs, raw queue variance averaged 23.39 and filtered variance 18.98, an 18.9% reduction. The lag is longer than the 2 ms prediction horizon and is one reason filtering was useful diagnostically but weak for fast control. The scheduler needs the filtered delay only to avoid toggling exposure on individual completions; it does not use filtered state for safety.

## 7. Shadow model

M4 does not implement Shadow-HSFM. It uses a database-specific `M4ObserverState` generated from Oct:

```text
queueEMA[t] = (3*queueEMA[t-1] + 1000*queue[t]) / 4
arrivalEMA[t] = (arrivalEMA[t-1] + 1000*arrivals[t]) / 2
completionEMA[t] = (completionEMA[t-1] + 1000*completions[t]) / 2
delayEMA[t] = (3*delayEMA[t-1] + delay[t]) / 4
```

Queue/rates are fixed-point x1000. Delay is `Float<s>` in Oct, using the language's SI dimension; Go converts at the pgx/diagnostic boundary. The state contains no maps, heap rings, request objects, or database handles.

## 8. Predictor

Targets are queue depth and mean dispatch-to-completion delay after 2 ms. The horizon is the rounded overload p95 service-turnover time and equals two 1 ms controller ticks.

Baselines:

- queue persistence: current raw queue remains;
- delay persistence: current filtered delay remains.

Challenger:

```text
queueLinear = max(0, queueEMA + 2*(arrivalEMA-completionEMA))
delayLinear = max(0, delayEMA + 2*(delayEMA-previousDelayEMA))
```

This is honestly named bounded linear extrapolation. It is neither Shadow-HSFM nor a Smith predictor. No Smith structure was implemented because there is no distinct delay-free plant model plus known dead time or future resource-preparation process.

## 9. Prediction accuracy

Four K0 runs produced the aggregate result:

| target | persistence MAE | linear MAE | result |
|---|---:|---:|---|
| queue at +2 ms | 1.382 requests | 1.701 requests | persistence 23.1% lower |
| dispatch delay at +2 ms | 342.4 us | 380.6 us | persistence 10.0% lower |

One held trace shows the same result in every regime:

| regime | queue MAE persistence/linear | delay MAE persistence/linear us |
|---|---:|---:|
| steady low | 0.421 / 0.526 | 290.9 / 300.0 |
| ramp up | 1.015 / 1.137 | 472.1 / 526.8 |
| sharp burst | 1.695 / 2.008 | 344.4 / 398.8 |
| sustained overload | 2.072 / 2.537 | 299.0 / 348.7 |
| ramp down | 1.047 / 1.190 | 399.5 / 440.9 |
| hot-key relief | 0.471 / 0.564 | 319.3 / 323.2 |
| adversarial burst | 1.921 / 2.390 | 336.4 / 392.9 |
| adversarial relief | 0.430 / 0.523 | 315.3 / 324.6 |

Bias was small for persistence; linear queue prediction had positive overshoot bias. At the proposed 1.8 ms action threshold, linear prediction produced TP/FP/FN/TN = 0/9/43/5802: precision 0 and recall 0. Relative queue error is not reported because zero/near-zero queues make it misleading.

## 10. Shadow counterfactual evidence

K0 recorded what would have changed without changing H. Representative false positives predicted 2.15 ms from a 1.23 ms filtered state and observed 1.29 ms, and predicted 2.55 ms before an observed 0.82 ms result. All nine predicted threshold crossings in the held trace were false positives, while 43 actual crossings were missed. A predictive exposure reduction would therefore have reduced available workers at the wrong times and missed every measured high-delay event.

Persistence also missed those sparse threshold events, but unlike extrapolation it did not manufacture false positives. Counterfactual traces establish decision quality only; no performance claim is made from them.

## 11. Control action

J is H plus one filtered reactive actuator. If filtered delay reaches 1.8 ms, exposure changes from 8 to 6; it returns to 8 at or below 1.4 ms and holds inside the band. Thresholds bracket the measured low-load p95 (1.54 ms) and overload p95 (2.08 ms). No parameter sweep was used, avoiding benchmark overfit; sensitivity is visible in the sparse action count.

Across four runs J averaged two target changes and 11.75 limited ticks (11.75 ms) per run. The target remained within `[6,8]`. It never changed admission capacity, conflict eligibility, transaction behavior, priority choice, aging, or the static worker maximum. K received no actuator because it failed the prediction gate.

## 12. Stability

- Overshoot: linear queue prediction overshot persistence and had higher MAE in every regime.
- Oscillation/chatter: none; J averaged two target changes per run.
- Undershoot: the delay classifier missed all 43 held-trace threshold events.
- Recovery: H, J, and K0 each recovered in 1.0 s in every rotated run.
- Underutilization: no persistent limiting; J was limited for only 11.75 ms/run.
- Wind-up: impossible because the observer has no integral term.
- Failure behavior: deterministic tests force extreme prediction error, clamp negative estimates, enforce exposure `[1, static max]`, and restore full exposure after an unrelated latency spike.
- Safety: all runs report zero ownership leaks and double releases.

## 13. Benchmark methodology

Four rotated runs used PostgreSQL 17.11, Go 1.26.2, pgx 5.7.6, seed 20260821, eight connections/workers, queue 128, maximum batch 8, and 2 ms batch wait. Orders were H/J/K0, J/K0/H, K0/H/J, and J/H/K0. Each lane warmed with 5,000 operations and PostgreSQL was reset before timing.

The deterministic 12.5 s workload offered 21,225 operations:

| phase | duration | offered rate/regime |
|---|---:|---|
| steady low | 2 s | 250/s low contention |
| ramp up | 2 s | linear 250→1800/s, mixed contention, 100 ms buckets |
| sharp burst | 1 s | 3500/s hot key |
| sustained overload | 2 s | 5000/s hot key |
| ramp down | 2 s | linear 1800→250/s, mixed contention, 100 ms buckets |
| hot-key relief | 2 s | 250/s low contention |
| adversarial burst | 0.5 s | sudden 4500/s hot key after relief |
| adversarial relief | 1.5 s | abrupt end at 250/s low contention |

Raw plant and observer events are local ignored CSV tables. Compact `plant-summary.json`, `observer-summary.json`, lane results, config, and correctness evidence are retained. Trace capture is outside timed lanes.

## 14. Results

Means of four rotated runs:

| lane | completed | ops/s | p99 ms | queue p99 ms | DB p99 ms | alloc/op |
|---|---:|---:|---:|---:|---:|---:|
| H | 21,225 | 1634.06 | 12.540 | 11.690 | 2.147 | 37.349 |
| J | 21,225 | 1634.11 | 14.392 | 13.440 | 2.162 | 37.373 |
| K0 shadow | 21,225 | 1634.11 | 12.567 | 11.652 | 2.132 | 37.323 |

Tail ordering varied by run, so the tiny H/K0 differences are noise rather than a shadow benefit. J did not improve throughput or DB service tail and its mean application p99 was 14.8% higher than H.

| regime | H p99 ms | J p99 ms | K0 p99 ms |
|---|---:|---:|---:|
| steady low | 3.500 | 3.437 | 3.333 |
| ramp up | 3.100 | 3.102 | 3.097 |
| sharp burst | 4.338 | 6.816 | 4.963 |
| sustained overload | 13.751 | 16.334 | 14.582 |
| ramp down | 3.159 | 3.133 | 3.096 |
| hot-key relief | 3.343 | 3.223 | 3.433 |
| adversarial burst | 11.064 | 12.461 | 8.509 |
| adversarial relief | 3.276 | 3.444 | 3.262 |

No lane rejected or failed an operation. All final PostgreSQL states were identical in every run. The workload did not saturate admission capacity; overload manifests as queue/conflict delay rather than rejection.

## 15. Ablation

H→J: filtering reduced signal variance, but the bounded reactive exposure action was too rare to help and produced no repeatable runtime benefit. J is not retained.

J→K0: K0 removes control and adds linear shadow prediction. Its runtime is parity with H, but prediction error is worse than persistence; it cannot advance to K1.

Persistence→linear: persistence wins aggregate MAE and every regime-specific MAE. No Shadow-HSFM or Smith implementation is justified merely to fill an ablation table.

## 16. Oct feature findings

| feature | classification | finding |
|---|---|---|
| typed records/generated Go | Useful | compact persistent observer state with ordinary Go integration |
| SI `Float<s>` delay | Useful | compiler-checked seconds internally; no microsecond magic in Oct |
| fixed-point queue/rate state | Useful | deterministic, allocation-free bounded arithmetic |
| compiled pure observer | Useful | 30.94-31.93 ns/update, 0 B and 0 alloc |
| richer P14 filter selection | Neutral | no evidence that multiple filters are needed for this plant |
| flow/`when policy` | Neutral | one explicit actuator remains clearer in Go, consistent with M3 |
| new compiler work | Inconclusive | none was required; the existing backend handled records and SI values |

## 17. Shadow-HSFM verdict

**Useful inspiration only.**

Its bounded evidence, shadow authority, truth separation, and quarantine discipline transfer. Its numerical state, witness relationship, and staged precision/backend actions do not represent this database plant. Calling M4's EMA observer a Shadow-HSFM would be inaccurate.

## 18. Smith predictor verdict

**Not useful for databases in the measured scheduler/plant.**

The current system lacks the stable, separable dead time and cancellable future-resource preparation process that made P15 conceptually plausible. The smallest trend predictor already loses to persistence; implementing a Smith model would add assumptions without evidence.

## 19. Interpretation

1. Database feedback is delayed, but only about 1.5-2.1 ms at p95; this is not enough to justify model-based prediction here.
2. Current queue state is the most predictable signal. Its best predictor over 2 ms is persistence.
3. Filtering helps independently as diagnostics, reducing queue variance 18.9%, but adds about 3 ms nominal lag.
4. Prediction does not improve scheduling because it fails before control.
5. The more sophisticated candidate does not beat the boring baseline in aggregate or any regime.
6. Reactive control remains stable, but stability alone does not make it useful.
7. Observer cost is approximately 31 ns/tick update, zero allocation. End-to-end allocations remain about 37.3/op in all lanes.

## 20. Limitations

Evidence covers one PostgreSQL version, host, schema, pool size, worker bound, queue bound, and deterministic workload family. Windows timestamp granularity produces some zero-duration sub-millisecond service samples, so dispatch-to-completion is the primary plant delay. The benchmark did not reach admission rejection. The threshold policy was derived rather than swept and activated sparsely. A different database, network hop, storage regime, connection pool, or seconds-long query workload could have a longer and more predictable plant. This result therefore rejects prediction for the measured scheduler/configuration, not all database admission control.

## 21. Exactly one next recommendation

**Stop prediction work and retain H unchanged.**
