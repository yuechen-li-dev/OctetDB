# OCT-DB-TEMPLATES-M0 — Composable Semantic Specialization Templates

## 1. Verdict

Success

The prerequisite was sufficient: JobQueue and Inventory are small typed compositions of one canonical Oct catalog, webhook correctly remains default, and W5 produces equivalent ordinary FLOW/Go without a template runtime. Categories resolve to real Concepts, and existing refined Concepts/Require reject invalid value configuration before lowering. OCT-TEMPLATE-CODEGEN-M0 established structurally identical normalized MIR, byte-identical normalized FLOW Go, and the same Go compiler inlining/escape profile. The earlier 11.7% W5 result was a benchmark/code-layout attribution error: moving only benchmark helpers reversed the apparent winner. The remaining callback/assertion findings are bounded usability work, not a milestone blocker.

## 2. Product motivation

PERF-M4 showed that a profile-directed immutable read representation can remove repeated JSON decode/allocation, but each bespoke specialization required an expert to design the structure. This milestone pays that design cost once in typed semantic patterns. The workflow remains default Go application → profile → explicit catalog selection → typed `with` customization → benchmark → retain only positive ROI.

## 3. PERF-M4 bespoke W5 baseline

The control is unchanged: `docs/product/evidence/PERF_M4/specialized/query.oct` is 34 physical/26 nonblank lines and 472 bytes; its adapter is 208/199 Go lines; generated Go is 1,473/1,339 lines and 46,598 bytes. First generation was 448.69 ms and warm generation 172–205 ms. Runtime materialization initialization was 0.44/3.89/45.72 ms at 1k/10k/100k. The primary 10k mixed lane measured 775/s default versus 170,279/s specialized (219.7×), 87 B/op, preserving result order and early stop. Those historical numbers were not rerun or rewritten.

## 4. Template W5 implementation

`w5/query.template.oct` retains a concrete Job and application predicate/rebuild function, then selects `RebuildPublication<Job>`, `ReadMostlyDataset<Job>`, `MaterializedFilter<Job>`, and `FilteredView<Job>`. It explicitly imports `DatabaseTemplateContracts`; two `with` expressions customize Concept-admitted expected reads and Limit. The generation script assembles canonical Oct source only in a temporary package and uses the real existing generator. Generated Go is never hand-edited.

## 5. JobQueue proof

The 59/55-line proof selects nine catalog declarations including typed ID identity, a Concept-checked 100,000 bound, explicit publication, read-mostly Ready materialization, finite states `[0,1,2]`, JobQueue composition, and FilteredView Take behavior. Three `with` overrides customize bound, read ratio, and limit. Source order and early stop pass interpreted and compiled with zero fallback. Application-specific logic remains the Job record, Ready predicate, rebuild function, and transition policy.

## 6. Inventory proof

The independent 49/45-line proof selects eight declarations for SKU identity, a Concept-checked 50,000 bound, publication version 4, `Available < 5`, Inventory composition, and FilteredView limit five. Two `with` overrides customize bound and limit. Its InventoryItem selector and predicate are nominally distinct from Job. It passes interpreted and compiled with zero fallback.

## 7. Negative/no-specialization proof

Webhook/event ingest is synchronization, exact durable replay, and point-lookup dominated. The correct catalog decision is no template: use default OctetDB's durable command result and Dataset lookup. The fresh agent independently made that decision and authored no Oct specialization. No EventDedupe template was added merely to fill a catalog slot.

## 8. Default Go independence

No OctetDB Go API, storage, WAL, query planner, Dataset behavior, or default build path changed. A Go user can ignore Oct, FLOW, selectors, and templates indefinitely. This repository contains examples, generation adapters, and evidence only; canonical definitions live once in Oct.

## 9. Coherence/rebuild boundary

The durable Dataset remains authority. MaterializedFilter requires ReadMostlyDataset, which requires explicit RebuildPublication source/version/callback evidence. W5 and the examples use immutable or application-rebuilt snapshots. The catalog does not claim transactionally maintained secondary indexes, mixed mutable-view coherence, or automatic restart publication; rebuilding after open/source change remains application-owned and visible.

## 10. Authoring-cost comparison

| Proof | Application Oct (physical/nonblank) | Reused catalog | Selected declarations | `with` overrides | Go integration | Compiler corrections |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| W5 template | 47/41, 1,476 B | 144/128 template/category lines + 31/25 contract/API lines | 4 | 2 | 116 nonblank benchmark/evidence adapter including parity controls; retained product adapter unchanged | 0 in final proof |
| JobQueue | 59/55, 2,189 B including assertions | same catalog | 9 | 3 | 0 | 0 in final proof |
| Inventory | 49/45, 1,999 B including assertions | same catalog | 8 | 2 | 0 | 0 in final proof |

Template W5 is longer than the narrow 26-nonblank bespoke control because it makes publication/workload assumptions explicit; LOC alone does not show the gain. Covered applications require zero new specialization architecture and zero backend knowledge. Fresh-agent time ranged from 2m26s for Inventory to about 10 minutes for JobQueue, with one or two diagnostic loops and no human correction.

## 11. Design-work comparison

Application authors still own records, predicates, state transitions, rebuild timing, and profile evidence. They explicitly select named patterns and customize typed selectors/values. They do not redesign bounded identity, publication semantics, materialized filtering, finite-state composition, FLOW execution, MIR, generated Go, or storage layout. For covered shapes, “new specialization architecture” and “compiler/backend knowledge” fall to zero; generated code need not be read to understand the application configuration.

## 12. Runtime/performance comparison

Result parity passes at limits 1, 10, 2500, and 5000. The focused follow-up proved exact normalized FLOW emission and identical optimizer decisions. Changing only benchmark/helper placement changed the old template result from 11.7% slower to 9.1% faster in forward order (66,754 versus 60,708 ns/op medians) and 8.7% faster in reverse order (66,398 versus 60,630 ns/op). Both lanes remain 0 B/op and 0 allocs/op. This direction reversal invalidates the earlier attribution to template codegen; it does not establish a template speedup. W5 supports result/codegen parity and no meaningful attributable runtime tax, while absolute nanoseconds remain layout-sensitive.

## 13. Generated-code audit

The generated artifact is 1,314 physical/1,206 nonblank lines and 47,641 bytes, SHA-256 `63d9ea2fc878db4a15f90afff89c08dd00b4d21d666453d300671c9682885ad1`. It is ordinary safe Go using the same FLOW class, has no unsafe/cgo/template registry/generic dispatch, and contains four specialization provenance comments plus two `with` override comments. Refined fields lower to Int/String aliases; category and value Concepts also emit ordinary general fallible admission helpers, which are uncalled for these compile-time-known values and removable by normal Go dead-code elimination. Declaration lowering is name-sorted and a repeated-load regression proves byte-identical output. The sidecar binds compiler/application base revisions, exact catalog/contract/source hashes, type arguments, overrides, artifact hash, and zero manual edits.

## 14. Build/compile cost

Catalog discovery median is 0.315 ms; canonical elaboration ranged from the timer floor to 0.520 ms. The final W5 generation series measured 346.91 ms first and 249.77–292.83 ms thereafter, above the retained bespoke 144.00–175.08 ms warm control. Template application source is 1,476 bytes and generated source 47,641 bytes versus bespoke 472 and 46,598 bytes. Category/value Concepts and admission APIs make the generated source 2.2% larger than bespoke even though their helpers are uncalled on this static path. These numbers expose authoring, constraint, and monomorphization costs rather than hiding the code-size tax.

## 15. Template discovery ergonomics

`oct templates list` returns the ten declarations deterministically in human or JSON form. Each Category must resolve to a String-refining Concept with `Require`; discovery rejects unknown/non-constrained categories. `oct templates describe MaterializedFilter` exposes Concept-qualified fields and classifies every requirement as `Require`, `Type`, `Structure`, or `Application`, preventing lifecycle prose from masquerading as a compile-time assertion. The package README teaches profile-first use, the explicit companion import, and when not to specialize.

## 16. LLM trial methodology

Four isolated fresh agents received only the public catalog/docs, a normal application requirement, and a brief profile. They were forbidden compiler internals and production-file access. Trials covered jobs, inventory, webhook/no specialization, and a deliberately invalid owner composition. Final programs were tested in both execution modes; metrics include choice accuracy, unnecessary specialization, corrections, human intervention, LOC, time, and backend hallucinations.

## 17. LLM trial results

Jobs and Inventory selected all relevant layers and no irrelevant application template; both passed interpreted and compiled with zero fallback, then re-passed after adding the documented contracts import. Webhook correctly selected default OctetDB. The invalid trial received an exact expected `fn(InventoryItem)->String` versus actual `fn(Job)->String` diagnostic and recovered in one pass. Human corrections and backend hallucinations were zero. Agents did encounter exact assertion typing, direct invocation of configuration-held callbacks, and formatter readability friction. The durable table is in Oct's `docs/internal/evidence/OCT_DB_TEMPLATES_M0/llm/trials.md`.

## 18. Product decision

P1. Template composition materially reduces specialization authoring/design cost and should become the standard OctetDB tuning path.

This means the opt-in advanced tuning path after profiling, never the default Go path.

## 19. LLM decision

L2. Agents can use templates but catalog/diagnostics need one more usability pass.

Selection and recovery were reliable across four trials; the callback/assertion/formatter seams justify the bounded qualification.

## 20. Runtime decision

R1. No meaningful runtime tax; specialization cost is compile-time/authoring only.

Normalized MIR and normalized FLOW Go are identical, the Go compiler profile is identical, and allocations are unchanged at zero. The former 11.7% result reverses when only benchmark/helper layout changes and therefore is not an attributable template tax. Generated source is still 2.2% larger because the imported category/value Concepts carry ordinary admission helpers; those helpers are outside the equivalent FLOW boundary and uncalled on W5's static path.

## 21. What templates do NOT solve

They do not add indexes, transactional materialized views, mutation specialization, WAL changes, a query planner, joins, profiling, AI selection, automatic workload inference, a workflow engine, physical-layout knobs, or runtime adaptability. They also do not make an unjustified workload worth specializing.

## 22. Remaining product gaps

Applications still own snapshot rebuild/publication. Consumers must explicitly import the Concept companion because template imports are not re-exported into the specialization package. Configuration-held callbacks are exactly typed but not uniformly callable directly, exact generic assertions can surprise authors, and general refinement-admission helpers add generated source even when static values require no runtime check. Benchmark absolute timings remain sensitive to ordinary Go symbol/helper placement, so W5 should be used as a parity guard rather than a claim that either spelling is faster.

## 23. Exactly one next recommendation

Pilot this catalog as the documented opt-in tuning path and use that single pilot to tighten callback invocation and exact-typed assertion diagnostics before marking the templates stable.
