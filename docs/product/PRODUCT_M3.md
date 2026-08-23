# OCTETDB-PRODUCT-M3 — Human/LLM Integration Benchmark

## 1. Verdict

**Meaningful progression**

Three genuinely fresh coding-agent contexts completed all six frozen tasks
against the released artifact, and every participant and hidden test passed.
The remaining major uncertainty is independent human usability: no eligible
human participant was available, so no human result is claimed.

## 2. Research questions

| Research question | Evidence-backed answer |
| --- | --- |
| RQ1 mental model | Yes for agents: 3/3 independently used Database → Bucket → Dataset → records without intervention. |
| RQ2 mutation semantics | Yes: every state-dependent write used `Database.Mutate` with `Tx.Get`/`Tx.Put`; no read-validate-write split appeared. |
| RQ3 idempotency | Yes: all three used stable, database-wide command identity and distinguished it from dataset record keys. |
| RQ4 domain rejection | Yes: Tasks 1, 3, and 5 used durable rejection; Task 5 also proved ordinary callback errors leave the ID retryable. |
| RQ5 query/discovery | Yes: Tasks 4 and 6 found typed Dataset scans, ascending-key determinism, filtering, and `ScanStop`; no SQL or shadow index was invented. |
| RQ6 side effects | Yes in the explicit probe: Participant B committed payment first and invoked email afterward, using a durable application outbox. |
| RQ7 durability/restart | Yes: all participants tested close/reopen state and retained decisions; withheld restart tests also passed. |
| RQ8 missing features | No task-blocking feature repeated. Predicate indexes appeared as a two-task scale convenience; outbox support appeared once. Operational gaps were recognized when explicitly probed. |

These answers establish strong agent legibility, not human legibility.

## 3. Frozen product under test

| Item | Frozen value |
| --- | --- |
| Module | `github.com/yuechen-li-dev/octetdb v0.2.0` |
| Tag/commit | `v0.2.0` / `76a0659ae9125c8c9b689fe155f2ff30a4b30fc9` |
| Module sum | `h1:AJFkUyS6GbM46L0QvF3UqgiIJA6EDSYooC0TfbZFJOU=` |
| Origin | Git tag `refs/tags/v0.2.0` at the commit above |
| Benchmark Go runtime | `go1.26.2 windows/amd64` |
| Participant module language versions | A: Go 1.23.0; B/C: Go 1.26.2 |
| Public docs supplied | README, GETTING_STARTED, DURABILITY, RECOVERY, CHANGELOG, v0.2.0 release notes, exported GoDoc/source, public examples |

Every participant `go.mod` requires exactly v0.2.0, contains no `replace`, and
imports no OctetDB internal/research package. The downloaded immutable module's
full test suite passed independently. No product code or feature changed during
the trial.

## 4. Benchmark methodology

Three agents ran simultaneously in isolated directories and clean model
contexts. Each received two tasks, the released-module constraint, and the
public-information allowlist. They could not see other submissions or hidden
tests and received no maintainer correction. A shared read-only Go module cache
was used; application modules and source directories were independent.

Participants first authored their own tests and notes. After submission, the
coordinator added a separate black-box harness that imported their public
application APIs. The harness did not modify participant code. It supplied new
inputs and exercised conflicts, restart, accepted/rejected retry, callback
abort, conservation, scan bounds/order, and dataset-scoped identity. All
participant suites were rerun uncached with `go vet`; the hidden suite was also
run under the race detector.

Task pairs were disjoint rather than rotated: A received 1/4, B received 2/5,
and C received 3/6. Learning between the two tasks in one context is therefore
a limitation. Prompts, source, notes, and hidden adapters are preserved under
`docs/product/evidence/M3/`.

## 5. Participant classes

| Participant | Class | Model configuration | Tasks | Independence |
| --- | --- | --- | --- | --- |
| A | P1 fresh coding agent | GPT-5.6-sol | 1, 4 | Clean context; zero interventions |
| B | P2 fresh coding agent | GPT-5.6-terra | 2, 5 | Clean context; zero interventions |
| C | P3 additional fresh coding agent | GPT-5.5 | 3, 6 | Clean context; zero interventions |
| Human | P4 | N/A | N/A | Human trial unavailable |

No meaningfully different provider was available in this environment, so three
different model configurations from the available provider were used. The
maintainer and architecture-primed agents were not counted as humans.

## 6. Scoring rubric

The 0–5 rubric was frozen before results were inspected and is preserved in
`evidence/M3/tasks/SCORING_RUBRIC.md`. Five means immediate discovery or full
hidden-edge correctness; four means unaided public-doc discovery or complete
self-corrected behavior; three means substantial source dependence or one
hidden edge failure; two and below indicate correction needs or core failures.
No composite is used.

| Participant/task | Discoverability | Correctness | Go idiomaticity | Conceptual simplicity | Operational clarity | Query discoverability | Idempotency clarity |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A / Task 1 | 4 | 5 | 4 | 4 | 5 | N/A | 5 |
| A / Task 4 | 4 | 5 | 4 | 4 | 5 | 5 | 4 |
| B / Task 2 | 4 | 5 | 4 | 4 | 5 | N/A | 5 |
| B / Task 5 | 4 | 5 | 4 | 4 | 5 | N/A | 5 |
| C / Task 3 | 4 | 5 | 4 | 4 | 4 | N/A | 5 |
| C / Task 6 | 4 | 5 | 4 | 4 | 4 | 5 | 4 |

Discoverability is 4, not 5, because participants performed deliberate doc and
signature exploration. C's operational score is 4 because optional-field
evolution remained an inference rather than a direct documented workflow.

## 7. Task 1 results

Participant A implemented `inventory/items` with create, point read, reserve,
release, typed accepted/rejected results, and stable caller command IDs. Its
submitted tests caught negative initialization, over-reservation, over-release,
duplicate reservation, state loss, and dedupe loss after reopen.

The hidden two-goroutine 7-of-10 reservation conflict produced exactly one
accept and one `insufficient_stock` rejection; final state was Available 3 and
Reserved 7. No callback correction was required.

## 8. Task 2 results

Participant B used the external event ID to derive stable command ID
`webhook/v1/event/<eventID>` while storing the record under the event ID in a
Dataset. A callback counter remained one across duplicate processing,
close/reopen, and retry. The exact stored result returned on retry.

## 9. Task 3 results

Participant C opened `commerce/orders` and `inventory/items` before mutation
and used one `Database.Mutate` callback for inventory validation, decrement,
and order create/update. A hidden two-order conflict against stock 5 accepted
one quantity-4 order, rejected the other, left stock 1, and persisted no losing
order. Retrying the accepted command after reopen did not decrement again.

## 10. Task 4 results

Participant A scanned `workers/jobs` directly with `ScanDataset`, filtered on
durable Ready status, and returned `ScanStop` at N. It created records out of
order but returned ascending job IDs, excluded claimed/terminal records, and
retained the same results after reopen. A hidden simultaneous claim accepted
exactly one worker and preserved ordered Ready results. No shadow list existed.

## 11. Task 5 results

Participant B implemented Created/Paid/Shipped/Cancelled transitions. Domain
violations used `RejectWithResult`; a repeated ship-before-pay decision remained
the same durable rejection after reopen. An injected ordinary callback error
did not consume the payment command ID, and retry accepted it. Conflicting pay
commands preserved one transition.

Email sending occurred after the payment/outbox mutation returned. A stable
message ID and sent marker made the ordinary tested path one-send; the
participant correctly documented that a crash between provider send and sent
mark is at-least-once and needs provider deduplication.

## 12. Task 6 results

Participant C used separate typed scans for low-stock items and Paid orders,
`Dataset.Get` for the point read, and a later `Database.Mutate` for restock.
With 25 matching records inserted in reverse order, hidden tests returned the
first 20 ascending keys for both datasets. The same key existed independently
in both datasets. Reopening and retrying the restock ID returned a duplicate
without a second stock increase.

## 13. Human results

Human trial unavailable. No human completion time, quote, preference, or
sentiment is inferred from agent behavior.

## 14. Agent results

| Participant | Task | Compile without correction | Correct first design | Interventions | API hallucinations | Restart pass | Idempotency pass | Final pass |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A | 1 | Yes | Yes | 0 | 0 | Yes | Yes | Yes |
| A | 4 | Yes | Yes | 0 | 0 | Yes | Yes | Yes |
| B | 2 | Yes | Yes | 0 | 0 | Yes | Yes | Yes |
| B | 5 | Yes | Yes | 0 | 0 | Yes | Yes | Yes |
| C | 3 | Yes | Yes | 0 | 0 | Yes | Yes | Yes |
| C | 6 | Yes | Yes | 0 | 0 | Yes | Yes | Yes |

| Participant | Session elapsed | First compiling implementation | Compile errors | Runtime/test failures | Public docs consulted | Edit/test cycles | Agent turns | Tool calls | Production/test lines | Approx. OctetDB-specific LOC |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| A | 5m07s | First executed compile passed; exact timestamp N/A | 0 | 0 | 6 plus GoDoc/source/examples | 4 | 1 | N/A | 376 / 254 | ~150 |
| B | 3m33s | First executed compile passed; exact timestamp N/A | 0 | 0 | 6 plus GoDoc/source/example | 7 | 1 | N/A | 336 / 229 | ~336 participant estimate |
| C | 4m28s | First actual package compile passed; exact timestamp N/A | 0 | 0 | 6 plus GoDoc/examples | 5 | 1 | N/A | 389 / 322 | ~45–60 |

Agent tool-call telemetry was unavailable to the coordinator and is truthfully
reported N/A. C had two pre-compilation filesystem/path setup failures; these
are recorded in its notes and not counted as OctetDB compile errors. Every
uncached participant suite, every `go vet`, the hidden suite, and hidden
`go test -race` passed.

## 15. Semantic-error taxonomy

| Category | Meaningful errors | Evidence |
| --- | ---: | --- |
| catalog topology | 0 | All used the requested Bucket/Dataset topology. |
| dataset identity | 0 | Same-key cross-Dataset test passed. |
| command identity | 0 | Stable database-wide IDs throughout. |
| transaction boundary | 0 | No unsafe split reads. |
| idempotency | 0 | Accepted and rejected restart retries passed. |
| durable rejection | 0 | Rejections decoded and retained correctly. |
| query/scan | 0 | Deterministic typed scans and early stop used. |
| context/cancellation | 0 observed | No incorrect cancellation claim; cancellation was not a dedicated app test. |
| external side effects | 0 | Email remained outside callback. |
| restart/durability | 0 | All restart checks passed. |
| error handling | 0 | Ordinary callback abort remained retryable. |
| API discoverability | 0 semantic; 1 harmless exploration | A guessed a source filename `scan.go`, then found exported scan docs in `query.go`. |
| missing feature assumption | 0 | Limitations were treated as limitations, not invented APIs. |
| Go/module packaging | 1 non-product setup incident | C initially targeted a nonexistent directory and misplaced files, then corrected before compilation. |

No attempt used `db.Query`, `Where`, SQL, `BeginTransaction`, `Commit`, an
index API, `OpenKeyed`, or Oct. Ordinary typos were not reclassified as
architecture failures.

## 16. Catalog mental-model findings

The public hierarchy was legible to all three agent contexts. Dataset handles
were opened once during adapter setup, names were treated as semantic catalog
names rather than paths, and record identity was kept at `(Dataset, key)`.
There was no drift toward Collection, table, or filesystem abstractions.

## 17. Transaction-boundary findings

The strongest evidence is Task 3: Participant C naturally placed both Dataset
reads and writes inside one `Database.Mutate`, and the withheld conflict test
showed no partial order. A and B did the same for one-Dataset invariants. The
right authority was discovered without intervention.

## 18. Idempotency findings

Participants consistently treated caller/external request identity as stable
across retries. B made the distinction especially explicit with separate event
record and webhook command identities. Database-wide dedupe survived reopen,
and both accepted and rejected exact decisions were observed. No random ID or
Dataset-scoped command-ID strategy appeared.

## 19. Durable-rejection findings

Agents distinguished expected domain decisions from operational errors.
Insufficient stock, invalid lifecycle transitions, and job conflicts were
durable rejections. The injected Task 5 operational error returned normally as
an error and the same ID later applied, proving it had not been consumed.

## 20. Query/discovery findings

Both scan participants found the normal Go query surface without Oct. They
relied on ascending record keys, stopped synchronously at the requested count,
and understood scans as enumeration rather than a planner. The only repeated
query pressure was future efficiency for selective scans, not inability to
complete a task.

## 21. External-side-effect findings

The explicit email trap did not trigger callback-side network work. Participant
B chose durable mutation → returned decision → sender, augmented by an
application outbox. It also articulated the crash window accurately. One trial
is evidence of legibility, not enough evidence to prioritize a built-in outbox.

## 22. Restart/durability findings

All participants closed and reopened the same directory, then checked records,
catalog topology through reopening, scan results, and retained command
decisions. Operational answers correctly described complete WAL replay,
incomplete-tail truncation, fail-closed corruption, close-time snapshots, and
application-chosen snapshots. A hidden test flipped a byte in a closed
snapshot; Participant A's adapter propagated `ErrorCorruption` on open. No
salvage or callback replay assumption appeared.

## 23. Documentation effectiveness

| Public doc | Consulted by | Helpful? | Confusion remaining |
| --- | --- | --- | --- |
| README | A, B, C | Yes: model, mutation, scan, retry, limits | None for basic integration |
| GETTING_STARTED | A, B, C | Yes: exact progressive implementation patterns | Optional-field evolution not explicit |
| DURABILITY | A, B, C | Yes: sync, decisions, snapshots, crash | None observed |
| RECOVERY | A, B, C | Yes: ownership, backup, corruption, locking | None observed |
| CHANGELOG | A, B, C | Confirmed release surface | Mostly confirmation, not issue resolution |
| v0.2.0 release notes | A, B, C | Confirmed version/features/limits | None observed |
| Exported GoDoc | A, B, C | Yes: exact symbols/signatures | A/B also inspected exported implementation source |
| Public quickstart | A, B, C | Yes: runnable reopen/retry shape | None observed |

A and B read exported root-package implementation to verify exact signatures
and decision fields; C used GoDoc without implementation reading. Their notes
do not establish that source was necessary for the basic mental model, but
repeated exact-signature checking is a mild documentation-search signal worth
watching in human trials.

## 24. Boring-Go architecture findings

All submissions isolated OctetDB in cohesive Store/Queue/Processor/Service
types and used ordinary domain structs. No transport was required, but the
adapters are ready to sit under normal service/HTTP layers. Product decisions
did not leak into unrelated packages. B's `EmailSender` interface is a
conventional external adapter; its extra outbox machinery remained local.

## 25. Missing-feature requests

| Requested/missing feature | Independent hits | Required vs convenience | Existing workaround | Recommendation |
| --- | ---: | --- | --- | --- |
| Secondary/predicate index | 2 task contexts (A4, C6) | Scale convenience; not required | Deterministic Dataset scan plus early stop | Watch in real workloads; do not implement from M3 alone |
| Built-in outbox/effect dispatcher | 1 (B5) | Reliability convenience beyond task minimum | Durable application outbox plus stable provider ID | Weak evidence; document pattern only if humans need it |
| Schema/model evolution | 3 prompted answers, 0 task blockers | Future operational need | Optional JSON fields plus application-owned type identity/migration | Clarify compatibility docs before designing migration |
| Directory locking/multi-process safety | 3 prompted answers, 0 task blockers | Operational safety feature | Application ensures one owner | Prompted evidence only; no priority conclusion |
| Snapshot/online backup operations | 3 prompted answers, 0 task blockers | Operational convenience | Close, or quiesce + snapshot + whole-directory copy | Prompted evidence only; no priority conclusion |

Prompted operational answers are not counted as spontaneous demand. No task
requested joins, SQL, pagination, schema registration, batch mutation, or Oct.

## 26. Workaround patterns

| Friction | Participants/tasks affected | Severity | Workaround | Product action candidate |
| --- | ---: | --- | --- | --- |
| Selective discovery scans all preceding records | A4, C6 | Low now; scale-sensitive | Typed scan, application predicate, `ScanStop` | Collect workload evidence before index design |
| Exact signature/field confirmation | A, B; C used extensive GoDoc | Low | Exported GoDoc/source | Improve cross-links/examples only if humans repeat it |
| Optional-field policy is inferred | A, B, C operational probe | Low–medium | JSON zero values and deliberate `TypeIdentity` policy | Add a concise public compatibility example |
| Reliable email delivery needs application machinery | B5 | Medium for that application | Local durable outbox and stable message ID | Future outbox candidate only after repeated independent need |
| Module/path scaffolding mistake | C | Trivial, not product-related | Correct working directory | No OctetDB action |

The scan workaround is simple application logic for these bounded tasks. The
outbox is closer to rebuilding database-adjacent machinery, but it occurred in
only one trial and should not set the roadmap yet.

## 27. Optional PostgreSQL integration-context result

Not run. PRODUCT-M3 did not need a database comparison to answer the frozen
legibility questions, and adding PostgreSQL setup would not reduce the human
evidence gap.

## 28. Product usability decision

**A. v0.2 public model is broadly legible and usable**

This decision is bounded to the coding-agent evidence. All six integrations
compiled independently and passed behavioral checks with zero correction.

## 29. Next database-feature decision

**8. No major product feature yet; documentation/API hardening first**

No missing feature blocked a task or reached three spontaneous independent
hits. Indexes had two convenience hits and outbox support one. Prompted
operational limitations should not be mistaken for roadmap votes.

## 30. Oct adoption-ladder decision

**IV. Insufficient evidence**

All participants correctly understood that Oct was optional and completed the
Go tasks without it, so there is no evidence of premature mandatory-Oct
confusion. The advanced-Oct bonus trial was not run, however, so the
acceleration/upskill part of the ladder was not measured.

## 31. What surprised us

- Three different fresh model configurations produced zero OctetDB API
  hallucinations despite conventional database vocabulary being available.
- Every task used the intended authority on its first compiled design, and the
  withheld race/restart suite found no semantic defect.
- The side-effect participant independently built and accurately bounded an
  application outbox instead of putting email inside the callback.
- All agents read most of the public documentation. That shows the answers are
  present, but not yet how quickly a human finds the minimum necessary page.

## 32. What not to implement

Do not implement secondary indexes, built-in outbox, schema migration,
directory locking, online backup, scheduled snapshots, SQL, joins, sorting, or
pagination from this round. None was a repeated task blocker. Do not change the
mutation, rejection, or scan APIs that all measured integrations discovered
correctly. Documentation clarification for optional compatible fields is a
small hardening candidate, not authorization for a migration subsystem.

## 33. Exactly one next recommendation

Run one independent-human round against the same frozen v0.2.0 tasks and hidden
harness, including the separate advanced-Oct bonus after the Go tasks, before
making any product-surface or feature-roadmap change.
