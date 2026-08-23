# OCT-DB-TEMPLATES-M0 — Composable Semantic Specialization Templates

## 1. Verdict

Honest stop

## 2. Product specialization problem

PERF-M4 proved that application-specific compiled Oct can remove repeated JSON
decode/allocation from a measured read query. This milestone asked whether that
design expertise could be reused through typed semantic templates rather than
reimplemented for each application.

## 3. Existing bespoke W5 baseline

The retained baseline is
`docs/product/evidence/PERF_M4/specialized/query.oct`: 26 nonblank Oct LOC and
472 bytes. It defines a concrete `Job`, concrete `IsReady`/`JobID` functions,
and four concrete queries. OctetDB remains durable authority; the adapter builds
an immutable read materialization at setup. PERF-M4 records exact ordering,
early stopping, zero query allocations, and the existing benchmark results.

## 4. Template-based W5

No template-based W5 is claimed. A `with` record can carry `MaxRecords`, but it
cannot replace a record type, key field, or exact predicate signature. The W5
query would therefore remain the same bespoke 26 LOC with unused metadata
beside it. Calling that template-authored would game the authoring-cost metric.

## 5. JobQueue proof

The existing OCT-QUERY-M0 ready-job application remains a valid concrete proof,
but `JobQueue.template.oct` cannot accept a caller-owned Job record/status
domain through current Oct types. No misleading renamed copy was added.

## 6. Inventory proof

The existing OCT-QUERY-M0 inventory application proves distinct low-stock
semantics, including point updates and deterministic bounded scans. It cannot
share a typed MaterializedFilter declaration with JobQueue because
`fn(Job)->Bool` and `fn(Item)->Bool` are exact, incompatible types and there is
no parameter for their common record position.

## 7. Negative/no-specialization proof

PERF-M4 W1 is the retained negative result: the specialized control deliberately
uses the default path because profiling did not justify a specialization. This
supports the product rule “profile first,” but not template assembly by itself.

## 8. Application integration model

The valid boundary remains: OctetDB `Dataset` is authoritative durable state;
an application may build and publish an immutable typed snapshot; generated
safe Go consumes that snapshot. A template would be authoring-time material,
not a Dataset instance or runtime registry.

## 9. Coherence/rebuild semantics

PERF-M4 W5 is read-only after setup. Lifecycle is explicit: build after durable
load, publish by assigning the snapshot, use for read queries, and rebuild only
under application control. Arbitrary mutation coherence and transactionally
maintained secondary indexes remain unsupported; W6 must not be hidden by a
template name.

## 10. Authoring-cost comparison

| Proof | Bespoke application Oct | Template override Oct | Reused template Oct | Result |
| --- | ---: | ---: | ---: | --- |
| W5 | 26 nonblank LOC | not expressible | 0 | baseline retained |
| Jobs | concrete application exists | not expressible | 0 | no reusable typed assembly |
| Inventory | concrete application exists | not expressible | 0 | no reusable typed assembly |

Metadata-only configuration does not reduce specialization design work, so no
LOC reduction is claimed.

## 11. Performance comparison

No new benchmark was run because there is no semantically distinct
template-authored artifact. The existing PERF-M4 baseline remains authoritative;
rebenchmarking an identical bespoke file under a template label would not test
abstraction overhead or authoring reuse.

## 12. Generated-code audit

The convention probe compiles through ordinary Oct and introduces no template
runtime. No generated W5 artifact was produced or repaired. Existing PERF-M4
generated Go remains safe Go with GC enabled and no `unsafe`/cgo requirement.

## 13. LLM trial methodology

The requested fresh-agent trials require a public, usable catalog. Running them
against metadata that cannot compile typed record/key/predicate overrides would
measure prompt compliance, not template composition, so trials were not
fabricated. The compiler probes instead establish the prerequisite failures a
fresh agent would encounter.

## 14. LLM trial results

No valid assembly trial could begin. An agent can correctly choose “do not
specialize” for W1/webhook-like no-hot-scan evidence, but Jobs and Inventory
cannot be assembled from the requested typed catalog in the current language.

## 15. Product decision

**P4. Template abstraction adds more complexity than it removes.**

This applies to the current Oct type surface, not to the product idea in
principle. Five concrete copies plus a manual registry would add names and
documentation without amortizing the typed query design.

## 16. LLM decision

**L4. Insufficient evidence.**

There is no honest catalog on which to judge fresh-agent selection or
composition reliability.

## 17. What templates should NOT encode

Templates must not encode physical layout, cache lines, search algorithms,
buffer sizes, backend strategy strings, hidden Go structures, speed claims,
stale mutable views, runtime registries, text substitution, or unrestricted
compile-time execution. The failed probes do not weaken those boundaries.

## 18. Remaining product gaps

Oct cannot express application record types and field references as typed
template inputs, cannot parameterize query predicates over those records, and
cannot tie publication/coherence facts to a parameterized source identity.
Discovery, provenance, compatibility policy, five-template documentation,
generated equivalence, and LLM trials should follow only after that semantic
gap closes.

## 19. Exactly one next recommendation

Complete the prerequisite Oct parametric-record/function/query and typed-field-
selector milestone, then rerun this product milestone against W5, JobQueue,
Inventory, and the negative workload without changing the PERF-M4 coherence
boundary.
