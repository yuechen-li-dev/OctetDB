# OCTETDB-PRODUCT-M0 — Product Extraction and Public Contract

## 1. Verdict

Success

Product readiness: **Ready for v0.1 packaging**. This milestone does not create
or tag `v0.1.0`.

## 2. Product identity

OctetDB is an embeddable, bounded, single-process OLTP engine for an explicit
account/transfer domain. It durably decides uniquely identified commands,
maintains authoritative keyed account state, snapshots that state, and recovers
snapshot plus WAL after restart. It is not SQL, a generic record engine, a
network service, or a replicated database.

M0 chooses command/model option A: v0.1 is intentionally an account/transfer
OLTP engine. The implementation does not support a truthful generic command and
state API, so the public package does not pretend otherwise.

## 3. Repository before/after map

Before M0, there was no public Go package. The latest durable paths, generated
Oct code, controls, benchmarks, and PostgreSQL integration were combined under
milestone-named internal packages. The README began with research chronology.

After M0, the root is the public product package and `internal/core` is its sole
repository dependency. `internal/m7write` became the semantically named but
non-production `internal/researchengine` research compatibility area;
`internal/m7generated` became `internal/model`. Historical evidence was not
deleted or rewritten. The exhaustive classification is in
[`REPOSITORY_MAP.md`](REPOSITORY_MAP.md).

## 4. Canonical production engine decision

The canonical v0.1 path is the compact single-owner safe-Go batch engine
formerly measured as the C2 layout lane. It was selected because its real code
path already combined ordered overlapping-key batch semantics, one synchronized
binary WAL frame per offered batch, dense authoritative state, bounded exact
dedupe, deterministic snapshots, snapshot-plus-tail recovery, incomplete-tail
handling, and checksum validation.

The older resident-Oct engine, direct-Go controls, M1/M2 segmented formats,
PostgreSQL control, and benchmark adapters remain reproducible in
`internal/researchengine` and `cmd`, but are not constructors or selectable formats in
the product API. Public names contain no M7, M8, M9, C2, layout, or Tiger terms.

## 5. Public API

The exported production identifiers are deliberately one package:

- lifecycle: `DB`, `Open`, `(*DB).Close`;
- commands: `Command` (`ID`, `Kind`, `AccountID`, `OtherAccountID`, `Amount`),
  `CommandKind`, `Create`, `Deposit`, `Withdraw`, `Transfer`, `Freeze`,
  `Unfreeze`, `BeginTransfer`, `Confirm`, `Cancel`;
- results: `Result` (`Sequence`, `CommandID`, `Accepted`, `Reason`,
  `Duplicate`), `Reason`, `ReasonApplied`, `ReasonAwaitingConfirmation`,
  `ReasonCancelled`, `ReasonInvalidAmount`, `ReasonAccountMissing`,
  `ReasonAccountExists`, `ReasonAccountFrozen`, `ReasonInsufficientFunds`,
  `ReasonInvalidWorkflow`;
- writes and reads: `(*DB).Submit`, `(*DB).SubmitBatch`, `Account` (`ID`,
  `Balance`, `Frozen`, `Version`), `(*DB).Get`;
- maintenance/observability: `(*DB).Snapshot`, `Stats`
  (`CommittedSequence`, `WALBytesWritten`, `SnapshotSequence`,
  `DedupeEntries`, `AccountCount`), `(*DB).Stats`;
- configuration/versioning: `Options` (`Path`, `MaxAccounts`,
  `DedupeHorizon`, `BatchMax`), `FormatVersion`, `Version`;
- errors: `Error` (`Kind`, `Op`, `Err`), `(*Error).Error`,
  `(*Error).Unwrap`, `ErrorKind`, `ErrorInvalidInput`, `ErrorCapacity`,
  `ErrorStorage`, `ErrorCorruption`, `ErrorIncompatible`, `ErrorClosed`, and
  `ErrorPoisoned`.

Every exported identifier has GoDoc. Internal transition counts, effect tags,
layout estimates, benchmark counters, recovery timing details, and alternate
constructors are not public.

## 6. Oct integration boundary

Users do not write Oct source in v0.1. Oct is required neither at application
runtime nor to build the public Go package. The production engine executes the
safe-Go materialization of the fixed `accounts-v1` behavior; it does not accept
user-defined generated behavior.

The historical semantic source is
`experiments/M7/runtime/oct/account_agent.oct`. The last compiler revision used
for the validated model lineage is pinned offline as
`309da01b60ec0f7917d4fd5efd1707bd71d2d40f` in the database `FORMAT` marker and
the M7 evidence. Committed generated Go remains in `internal/model` for research
reproduction, but the production dependency graph does not consume it.

Compatibility is owned at the product boundary: the marker names
`model=accounts-v1`, and incompatible format/model markers fail open with
`ErrorIncompatible`. User-defined Oct behavior is explicitly out of scope until
a future milestone can define generation, artifact identity, and compatibility
without exposing compiler internals.

## 7. Storage format decision

Production format 1 is the compact binary WAL and deterministic binary snapshot
used by `internal/core`. It is the only public format and is not configurable.
The product directory owns `FORMAT`, `wal.oct`, optional `snapshot.oct`, and a
transient `snapshot.oct.tmp`. WAL and snapshot have independent magic/version
headers and integrity checks; `FORMAT` records the product format, model,
engine, and Oct provenance.

M1/M2 JSON/segmented and old single-file formats remain experimental. Pre-1.0
format changes are allowed, but silent reinterpretation is not. There is no
upgrade mechanism in M0. Details are in [`../RECOVERY.md`](../RECOVERY.md).

## 8. Durability contract

Success from `Submit` or `SubmitBatch` means every new decision in the batch is
in one complete WAL frame and the WAL `Sync` succeeded before acknowledgement.
Memory state is applied after that frontier. Rejected domain decisions are also
durable. Process crash, OS/power-loss qualifications, storage failure,
snapshot ordering, restart, incomplete tails, and duplicate retry are specified
in [`../DURABILITY.md`](../DURABILITY.md). Replication availability is not
provided.

## 9. Recovery contract

`Open` validates `FORMAT`, restores a valid installed snapshot if present,
validates the WAL, truncates an incomplete final append, and replays complete
records after the snapshot sequence. Complete-frame corruption, malformed
state, or sequence violations fail closed. Unsupported storage/model versions
are incompatible errors. Filesystem errors remain storage errors. There is no
best-effort salvage path.

## 10. Configuration/defaults

`Options.Path` is required. Zero values select safe bounded defaults:

| Option | Default | Meaning |
| --- | ---: | --- |
| `MaxAccounts` | 100,000 | maximum dense account slots admitted |
| `DedupeHorizon` | 100,000 | exact retained command decisions |
| `BatchMax` | 512 | maximum commands in one offered batch |

Negative bounds are invalid. Durable batch sync is the only public mode.
Account and WAL allocation hints, sync strategies, group waits, failure
injection, full-checkpoint ablations, and benchmark seeds are not public.

## 11. Error model

Callers use `errors.As(err, *Error)` and inspect `Error.Kind`. Categories cover
invalid input, capacity/admission, storage/durability, corruption,
incompatibility, closed handles, and poisoned handles. Diagnostic text and
wrapped OS errors are descriptive but not contracts. Domain rejections are
successful durable `Result` values with a typed `Reason`. Idempotent duplicates
are successful results with `Duplicate=true`, not errors.

## 12. Context/cancellation behavior

`Open` observes cancellation before recovery starts; once recovery starts it
finishes or fails and never exposes partial state. `Submit`, `SubmitBatch`, and
`Snapshot` observe cancellation while waiting for the local single-owner
admission token. After admission they run through their durability frontier and
return a definitive result. Cancellation therefore may abort before durable
commit; after admission/commit the operation may be durable even if the caller
disconnects, so callers retry the same command ID. `Close` is not cancellable.

## 13. Public examples

`examples/minimal` shows open, durable create, keyed read, and close.
`examples/restart` writes a batch, closes and reopens, retries the same IDs, and
verifies duplicate results and unchanged balance. Both are separate `main`
packages importing only `github.com/yuechen-li-dev/octetdb`, and `go test ./...`
compiles them as external consumers.

## 14. Experiment separation

Historical artifacts remain under `experiments/` and `docs/experiments/`, with
an index warning that they are not API documentation and their package names
are historical. Benchmark commands remain under `cmd/`. Research compatibility
code remains under non-production internal packages. The public package does
not expose old constructors or variants.

## 15. Dependency audit

`go list -deps .` resolves the repository edge only to `internal/core`, whose
imports are exclusively Go standard-library packages. It does not resolve
`internal/researchengine`, `internal/model`, experiments, benchmark tools, pgx, or
TigerBeetle. `TestProductionDependencyDirection` fails if the production root
acquires experiment, command, or benchmark imports.

## 16. Test matrix

| Required behavior | Product-path evidence |
| --- | --- |
| open new DB / reopen existing DB | `TestPublicLifecycleSingleAndBatch` |
| single write / batch write | `TestPublicLifecycleSingleAndBatch` |
| duplicate retry | `TestBatchIdempotencyAcrossRestart` |
| hot-key serial correctness | `TestHotKeySerialCorrectness` |
| snapshot | `TestSnapshotAndWALTailRecovery` |
| snapshot + WAL tail | `TestSnapshotAndWALTailRecovery` |
| incomplete tail | `TestIncompleteWALTailIsDetectedAndDiscarded` |
| corruption failure | `TestCorruptionAndFormatIncompatibilityFailClosed` covers WAL, snapshot, and format marker failures |
| capacity bound | `TestCapacityCancellationAndClose` |
| close behavior | `TestCapacityCancellationAndClose` |
| dependency direction | `TestProductionDependencyDirection` |

These tests use the public package externally; retained low-level fault and
snapshot-codec tests in `internal/researchengine` continue to protect research replay.

## 17. pkg.go.dev readiness

The module path is `github.com/yuechen-li-dev/octetdb`; the root has package
documentation, every exported name has GoDoc, the README begins with the
product, and examples compile. The repository is ready for v0.1 packaging.
Before tagging, PRODUCT-M1 must prove installation from a clean external module,
confirm the repository/module publication path, set the final version metadata,
and run the release checks against the published commit.

## 18. Known limitations

- The only domain is accounts/transfers; arbitrary Oct models are unsupported.
- One process and one replica; there is no lock file, server, failover, or replication.
- Dedupe is bounded, so an ID older than the horizon can execute again.
- No migrations or repair/salvage tooling exists.
- No online backup coordination API exists.
- Snapshot directory-rename durability is weaker on Windows.
- A storage failure poisons the handle and requires close/diagnosis/reopen.
- Recovery/open cannot be interrupted after validation begins.
- `Stats.WALBytesWritten` is process-local write volume, not current WAL file size.
- Research commands keep their external benchmark dependencies in this module,
  although the production package dependency graph does not use them.

## 19. Exactly one next recommendation

**PRODUCT-M1 — external installation, clean-module consumer proof, and v0.1.0 tag.**
