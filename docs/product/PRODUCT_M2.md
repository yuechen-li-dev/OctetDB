# OCTETDB-PRODUCT-M2 — Golden Integrations and Sane Default Workflow

## 1. Verdict

Success

## 2. Research question

What must OctetDB implement next to become useful to ordinary Go applications,
without requiring users to adopt the full Oct/semantic-specialization worldview
immediately?

The evidence says OctetDB needs one conventional, bounded keyed-state on-ramp:
ordinary Go structs encoded as JSON and one durable, idempotent, atomic validated
mutation boundary. Documentation alone cannot turn the fixed v0.1 account model
into an order, webhook, or job model.

## 3. v0.1 external baseline

Before product edits, a clean module outside the checkout required exactly
`github.com/yuechen-li-dev/octetdb v0.1.0`, had no `replace`, and resolved the
dependency from the public module cache. Four public-API-only tests ran. There
were zero internal imports, no research package, no Oct, and no source-tree
shortcut.

Inventory could map item IDs to `AccountID`, stock to `Balance`, reservation to
`Withdraw`, and release to `Deposit`. Webhooks could hash an external ID into an
account and use account existence as a processed marker. Those mappings lost
domain names and data. More importantly, an order encoded in `Balance` accepted
two distinct pay commands, and a job encoded the same way accepted claims by two
workers. `Get` followed by a fixed blind mutation is not an atomic validated
state transition.

The executable method and pre-fix friction are preserved in
[`evidence/PRODUCT_M2_V01_BASELINE.md`](evidence/PRODUCT_M2_V01_BASELINE.md).

## 4. Golden app 1

Inventory reservation uses an `Item` Go struct and an application store
interface. `Create`, `Reserve`, and `Release` run inside keyed mutations. The
reserve handler reads current stock, rejects non-positive or excessive
quantities, updates stock, and returns a durable reservation result. Tests prove
over-reservation rejection, exact retry, release, and stock after restart.

| Metric | Result |
| --- | --- |
| OctetDB-specific integration LOC | 14 direct public-API lines |
| Public OctetDB concepts required | 8 |
| Configuration values chosen | 1 directory; 0 bounds |
| Restart/idempotency correctness | pass / pass, including original result |
| Internal imports | 0 |
| Oct required | no |

## 5. Golden app 2

The webhook processor stores the external string ID, `processed` status, and
processing result directly. The event ID supplies stable retry identity. An
exact duplicate does not rerun the callback and returns the original result.
Close/reopen retains the complete event without hashing it into a numeric key.

| Metric | Result |
| --- | --- |
| OctetDB-specific integration LOC | 5 direct public-API lines |
| Public OctetDB concepts required | 7 |
| Configuration values chosen | 1 directory; 0 bounds |
| Restart/idempotency correctness | pass / pass, including original result |
| Internal imports | 0 |
| Oct required | no |

## 6. Golden app 3

The order app stores explicit `Created`, `Paid`, `Shipped`, and `Cancelled`
states. Its mutation validates a small transition table and writes the new state
inside the same serialized command. Tests prove valid transitions, invalid
double-pay rejection, exact command retry, and `Shipped` after restart.

| Metric | Result |
| --- | --- |
| OctetDB-specific integration LOC | 9 direct public-API lines |
| Public OctetDB concepts required | 8 |
| Configuration values chosen | 1 directory; 0 bounds |
| Restart/idempotency correctness | pass / pass; invalid transition retained |
| Internal imports | 0 |
| Oct required | no |

## 7. Golden app 4

The job app stores status, owner, attempt count, and failure reason. A claim is
accepted only from `Ready` or `Failed`; completion/failure requires the current
owner. Tests prove single ownership, exact claim retry, fail/reclaim identity,
completion, attempt count, and restart state.

| Metric | Result |
| --- | --- |
| OctetDB-specific integration LOC | 11 direct public-API lines |
| Public OctetDB concepts required | 8 |
| Configuration values chosen | 1 directory; 0 bounds |
| Restart/idempotency correctness | pass / pass, including retry attempt identity |
| Internal imports | 0 |
| Oct required | no |

LOC counts are lines directly naming an OctetDB public symbol in each persistence
adapter; they exclude domain validation, HTTP, service interfaces, and braces.
Concepts include open/defaults, directory lifecycle, command identity, mutation,
transaction reads/writes, decision/rejection, result decoding, and point reads
where used. The candidate apps form a separate Go module with the requested
`cmd/server`, `internal/httpapi`, `internal/service`, and `internal/store`
layers. It requires candidate `v0.2.0` and uses a temporary local `replace`
because this milestone deliberately does not tag a release.

## 8. Friction matrix

| Friction | Apps | Classification | M2 response |
| --- | ---: | --- | --- |
| Financial model names and validation | 4 | missing product feature | application-defined keyed Go state |
| No place for status/result/owner/domain fields | 3 | missing product feature | JSON-encoded ordinary structs |
| Read-validate-write race | 3 | missing product feature | serialized atomic mutation callback |
| Numeric-only entity key | 3 | API ergonomics | application string keys |
| Exact caller command ID | 4 | intentional advanced requirement | preserve; explain stable retry identity |
| Choosing capacity/dedupe/value/transaction bounds to try product | 4 | missing default | safe zero-value defaults |
| Manual snapshot or unbounded WAL | 4 | missing default | deterministic close-time snapshot plus explicit maintenance API |
| Closed versus missing point read | 4 | API ergonomics | contextual typed-error `GetKeyed` |
| HTTP status mapping | 4 | documentation gap | adapter examples and documented convention |
| General listing/filtering | 0 | missing product feature, not yet evidenced | deliberately not implemented |

No observed friction was classified as a fundamental OctetDB tradeoff. Bounded
dedupe identity and explicit production domain bounds remain intentional
tradeoffs, but safe trial defaults remove premature tuning.

## 9. Sane-default design

The new additive path is:

```go
db, err := octetdb.OpenKeyed(ctx, path, octetdb.DefaultKeyedOptions())
decision, err := db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.KeyedTx) (any, error) {
    // tx.Get, validate ordinary Go state, tx.Put/Delete
})
```

The default stores application-defined Go structs as bounded JSON records and
persists authoritative post-validation mutations rather than replaying old Go
callbacks during recovery. The callback is an explicit command handler without
generated code. `Reject` and `RejectWithResult` distinguish expected durable
domain decisions from operational callback errors, which abort without
consuming retry identity.

This chooses a combination of I2 and I4: generic keyed records are useful only
with application-defined typed mutation functions. It rejects I3 blob CRUD as
insufficient because opaque bytes would not solve the order/job race, and I1 is
the already-proven narrow advanced model.

## 10. Product changes implemented

- `OpenKeyed`, `DefaultKeyedOptions`, and a product-owned keyed database directory.
- Ordinary JSON Go values with point reads and transaction-local `Get`, `Put`, and `Delete`.
- Atomic serialized multi-record writes and validation.
- Stable command IDs with exact bounded accepted and rejected decision replay.
- JSON result decoding and typed domain rejection codes.
- Checksummed synchronized WAL frames, fail-closed snapshot/recovery validation,
  incomplete-final-tail truncation, and write poisoning after durability failure.
- Deterministic explicit and close-time snapshots.
- Four external layered Go apps, a compact runnable example, and public onboarding docs.

Product-feature decisions:

| Candidate | Seen in how many golden apps? | User friction removed | Complexity added | Implement now? |
| --------- | ----------------------------: | --------------------- | ---------------- | -------------- |
| Application Go structs as keyed JSON | 4 | removes fixed financial representation | separate bounded record format | yes |
| Atomic validated mutation callback | 3 | prevents double-pay/double-claim/over-reserve | serialized transaction overlay and durable effects | yes |
| Exact accepted/rejected result dedupe | 4 | safe network retries | bounded persisted decision window | yes |
| Zero-value product bounds | 4 | removes trial-time tuning | normalization only | yes |
| Product-owned directory | 4 | removes file/layout knowledge | format marker and owned files | yes |
| Close-time snapshot | 4 | removes basic maintenance decision | deterministic close work | yes |
| HTTP idempotency/error convention | 4 | removes adapter ambiguity | documentation/examples only | yes |
| Prefix scan or status filtering | 0 | no demonstrated required flow | public read contract and allocation behavior | no |
| Random command-ID helper | 0 | saves mechanical generation only | risks hiding retry identity | no |
| Background snapshot scheduler | 0 | bounds long-running WAL automatically | lifecycle, timer, and failure policy | no |
| Generic batch API | 0 | none in these apps | ordering/result surface | no |
| Directory lock implementation | 0 | operational misuse protection | stale/crash/platform policy | no; limitation documented |

## 11. Features deliberately rejected

M2 adds no SQL, LINQ/ORM operators, joins, dynamic expressions, query planner,
frontend, GraphQL, gRPC, DI framework, generated code, compiler feature,
replication, background service, or performance chase. It also rejects opaque
blob CRUD, automatic random retry IDs, an unevidenced listing API, and automatic
periodic snapshot scheduling.

## 12. Progressive adoption ladder

```text
Level 0  default bounded Go keyed state
Level 1  explicit application command/state model
Level 2  semantic bounds, batches, idempotency, invariants, workflows
Level 3  Oct-defined behavior and compiler specialization
```

M2 implements Level 0 and a direct Level 1 bridge. The fixed v0.1 account model
demonstrates a specialized path rather than defining every application's
vocabulary. The keyed path is an on-ramp, not a replacement for semantic
specialization.

## 13. Oct requirement boundary

The keyed apps require no Oct source, Oct compiler, generated code, MIR, FLOW,
C2, `LayoutContract`, semantic WAL delta knowledge, buffer selection, or layout
tuning. Oct begins only when a developer deliberately chooses Level 3 after the
Go domain and workload justify specialization.

## 14. Read-path findings

All four apps need point lookup, and one reusable contextual `GetKeyed` satisfies
them. Current-state snapshots are internally necessary for restart but are not
an application read API. None of the required flows demonstrated small listing,
status filtering, or projection enumeration, so M2 does not manufacture a scan
or general planner. This means a queue cannot yet discover arbitrary ready jobs;
the golden job requirement covers known-job status and deliberately records
discovery as a remaining limitation.

## 15. Mutation/transaction findings

Normal developers can state intent as ordinary code inside one mutation:
read current structs, validate a transition or invariant, update one or more
keys, and return a result. The engine serializes callbacks and durably commits
their effects together. A domain rejection is a durable decision; an unexpected
error is abort/retry. Distinct accepted commands have a deterministic order,
while duplicate IDs return the retained original decision without invoking the
callback.

This is not a SQL transaction abstraction. There are no isolation levels,
begin/commit handles, dynamic conflicts, or callbacks during recovery. External
network side effects must remain outside callbacks.

## 16. Defaults/bounds findings

| Bound | Default | Classification |
| --- | ---: | --- |
| live records | 100,000 | safe trial default; production population is a semantic bound |
| retained decisions | 100,000 | safe trial default; production retry horizon is semantic |
| JSON value/result | 1 MiB | safe default; large blobs are not the product path |
| writes per command | 4 MiB | safe default; advanced tuning knob |
| key and command ID | 4 KiB fixed | mechanical safety bound |
| rejection code | 1 KiB fixed | mechanical safety bound |

The user chooses only a directory for first use. Zero values select defaults;
negative or internally inconsistent bounds return `ErrorInvalidInput`.
Capacity failures return `ErrorCapacity`. Exact dedupe is never silently
weakened inside the configured horizon, and OctetDB never silently generates an
ID that a caller could not reproduce on retry.

## 17. Snapshot/recovery ergonomics

`OpenKeyed` creates its format marker, WAL, and snapshot beneath the supplied
directory. Users never name those files. Successful submission synchronizes a
checksummed decision frame. Recovery validates the latest snapshot, replays
complete subsequent frames, truncates only an incomplete final append, and
fails closed on checksum/sequence/format violations.

`Close` installs a deterministic snapshot and resets the WAL. Long-running
processes may call `SnapshotKeyed` at an explicit maintenance boundary. M2 adds
no timer, goroutine, adaptive cadence, or background scheduler. Crash-before-
close WAL growth and Windows rename guarantees remain documented limitations.

## 18. LLM legibility sanity check

A fresh coding agent saw only README, package docs, exported symbol docs, and a
webhook requirement. It built a separate public-only adapter and a restart/
retry test. Source compiled on its first attempt; `go mod tidy` was the only
module-housekeeping correction. No API hallucination, incorrect assumption,
internal-doc search, or human intervention occurred. The test proved the
callback was not rerun and the original result returned after restart.

Full bounded evidence is in
[`evidence/PRODUCT_M2_LLM_CHECK.md`](evidence/PRODUCT_M2_LLM_CHECK.md). This is
not substituted for PRODUCT-M3's broader benchmark.

## 19. Public docs/getting started

[`../GETTING_STARTED.md`](../GETTING_STARTED.md) has five independent stages:
open defaults, store/read state, explicit idempotency, atomic invariants, then
advanced specialization. Stage 1 contains no advanced storage/compiler concept.
README and package docs lead with the keyed Go workflow, explain stable HTTP
idempotency keys, state bounded defaults, and include “When not to use OctetDB.”
The compact keyed example runs and demonstrates restart.

## 20. Backward compatibility/release impact

The v0.1 `Open`, `Options`, `DB`, account types, command kinds, results, errors,
durability behavior, and account on-disk format are unchanged. The keyed path is
additive and has a distinct model marker so account and keyed directories cannot
be confused. It promises no automatic conversion between them.

This is substantial new public behavior, so it warrants `v0.2.0`, not a patch.
PRODUCT-M2 does not tag automatically. The golden module's candidate v0.2
requirement and local replacement must become a real public dependency check as
part of release preparation.

## 21. Final product verdict

2. A small sane-default API is required and implemented

## 22. What OctetDB still cannot do

- discover/list arbitrary records or filter ready jobs;
- run ad-hoc queries, SQL, joins, secondary indexes, or dynamic projections;
- coordinate multiple processes, replicas, failover, or distributed ownership;
- migrate application JSON schemas or keyed/account on-disk models;
- provide online backup or automatic periodic snapshot cadence;
- offer unbounded dedupe, records, values, or transaction size;
- store large blobs efficiently;
- atomically coordinate database state with an external network side effect;
- promise macOS verification or fully equivalent Windows directory-rename power-loss behavior;
- turn arbitrary Go callbacks into Oct/compiler specialization automatically.

The keyed path is memory-resident and single-writer. Callback code must be
deterministic, local, and fast enough not to block other mutations. A successful
M2 does not make OctetDB a general database.

## 23. Exactly one next recommendation

**PRODUCT-M3 human/LLM integration benchmark.**

The generality blocker exposed by v0.1 is now removed and four domains pass the
candidate path. The next uncertainty is no longer whether a public Go model
exists; it is whether independent humans and coding agents can discover and use
that model consistently, including its idempotency and rejection boundaries,
without maintainer guidance.
