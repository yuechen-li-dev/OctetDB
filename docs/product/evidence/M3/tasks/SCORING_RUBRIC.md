# Frozen scoring rubric

This rubric was fixed before participant results were inspected. Each dimension
is scored from 0 through 5. Scores describe demonstrated task behavior, not
maintainer intent.

| Score | Discoverability | Correctness | Go idiomaticity | Conceptual simplicity | Operational clarity | Query discoverability | Idempotency clarity |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 0 | Cannot locate the public model | Does not compile or cannot exercise the task | Product concerns dominate unrelated layers | Invents an incompatible database model | Claims unsafe/unsupported operations as guaranteed | Cannot discover records | No stable retry identity |
| 1 | Requires code correction or internal knowledge | Compiles only after correction; core invariants fail | Heavy leakage or brittle adapter | Repeated incompatible abstractions | Major durability/recovery misconceptions | Invents SQL/index API and does not recover | Duplicate retry reapplies or IDs are random |
| 2 | Requires conceptual/API correction | Multiple semantic failures remain | Noticeable leakage or nonstandard packaging | Correct pieces with a confused authority model | Understands persistence but misses major limits | Uses a workaround that breaks determinism/restart | Stable ID in places but wrong scope/decision behavior |
| 3 | Finds model with documentation pointer or substantial source reading | Core path passes; at least one hidden edge fails | Conventional adapter with minor leakage | Mostly correct, with avoidable machinery | Correct restart model; incomplete backup/locking/snapshot answer | Finds scan but misses stable order or early stop | Stable database-wide IDs; one rejection/error nuance missed |
| 4 | Finds model unaided from public docs with minor exploration | All required invariants pass after self-correction | Clean store/service boundary and ordinary Go types | Direct use of product authorities with small incidental complexity | Correct documented operations and limitations, minor omission | Correct deterministic scan and bounded result | Correct accepted/rejected retry and abort distinction |
| 5 | Immediate, accurate discovery with no hallucination or source dependency | First design passes all hidden restart, retry, concurrency, and conservation checks | Minimal, cohesive, reusable boring-Go adapter | Right path is both direct and clearly explained | Complete crash, backup, locking, snapshot, and compatibility reasoning | Scan semantics, early stop, snapshots, and limitations are explicit | Stable database-wide identity and exact decision semantics are explicit and tested |

## Evaluation rules

- A participant task can earn at most Correctness 2 if its submitted module does
  not compile independently against the released module.
- A hidden-test failure caps Correctness at 3; failure of a core invariant caps
  it at 2.
- Reading exported implementation source is allowed but caps Discoverability at
  3 when it is necessary for basic usage.
- An intervention at level 2 or above caps Discoverability at 2. Level 3 or 4
  also caps Correctness at 2 for the corrected behavior.
- Scores are not averaged into a single composite; dimension values retain the
  diagnostic signal.

