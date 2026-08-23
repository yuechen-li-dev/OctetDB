# OCTETDB-PRODUCT-M1 — External Install, Clean Consumer Proof, and v0.1.0 Release

## 1. Verdict

Success

## 2. Product/module identity

The canonical product and repository are **OctetDB** at
`https://github.com/yuechen-li-dev/OctetDB`. The canonical Go module is
`github.com/yuechen-li-dev/octetdb`; lowercase module spelling is conventional
and resolves to the renamed public GitHub repository. The stale local remote was
updated from the old `Database-Scheduler` URL to the canonical repository URL
before release.

The module name, package name, README, examples, documentation, GitHub tag, and
GitHub release now agree. No public product path contains a research milestone
or implementation-lane name.

## 3. Public import path

Consumers import:

```go
import "github.com/yuechen-li-dev/octetdb"
```

The package is at the module root. Consumers do not import `internal/core` or
any research, experiment, benchmark, or generated package.

## 4. Exported API audit

`go doc -all .` was inspected before tagging. Every exported package, type,
constant, field, function, and method has a user-facing GoDoc comment. An
executable `Example` renders with the package documentation and uses only the
public API.

| Surface | Exported identifiers | v0.1 decision |
| --- | --- | --- |
| Lifecycle | `DB`, `Open`, `Close` | Required product lifecycle; semantic names with no implementation history. |
| Writes/reads | `Submit`, `SubmitBatch`, `Get`, `Snapshot`, `Stats` | Required durable operation, batch, keyed read, maintenance, and bounded observability surface. |
| Commands | `Command`, `ID`, `Kind`, `AccountID`, `OtherAccountID`, `Amount`; `CommandKind`; `Create`, `Deposit`, `Withdraw`, `Transfer`, `Freeze`, `Unfreeze`, `BeginTransfer`, `Confirm`, `Cancel` | Required fixed account/transfer domain. Names are product-semantic. |
| Results | `Result`, `Sequence`, `CommandID`, `Accepted`, `Reason`, `Duplicate`; `Reason`; `ReasonApplied`, `ReasonAwaitingConfirmation`, `ReasonCancelled`, `ReasonInvalidAmount`, `ReasonAccountMissing`, `ReasonAccountExists`, `ReasonAccountFrozen`, `ReasonInsufficientFunds`, `ReasonInvalidWorkflow` | Required durable decisions and typed domain rejection/idempotency results. |
| State | `Account`, `ID`, `Balance`, `Frozen`, `Version` | Required authoritative read model. `Account.Version` is state revision, not a marketing version. |
| Options | `Options`, `Path`, `MaxAccounts`, `DedupeHorizon`, `BatchMax` | Required product bounds only. Zero values select 100,000 accounts, 100,000 retained decisions, and 512 offered commands. Durable batch sync is not configurable. |
| Stats | `Stats`, `CommittedSequence`, `WALBytesWritten`, `SnapshotSequence`, `DedupeEntries`, `AccountCount` | Small operational surface; no benchmark counters or research lane terminology. |
| Errors | `Error`, `Kind`, `Op`, `Error`, `Unwrap`; `ErrorKind`; `ErrorInvalidInput`, `ErrorCapacity`, `ErrorStorage`, `ErrorCorruption`, `ErrorIncompatible`, `ErrorClosed`, `ErrorPoisoned` | Small programmatic taxonomy used through `errors.As`; wrapped causes remain reachable through `errors.Is`/`errors.Unwrap`. |
| Format | `FormatVersion` | Operationally useful database format identity, kept separate from module version. |

The pre-release `Version` constant was removed: Go build/module metadata already
answers module-version questions. `Error.Err` was made private because callers
need the stable category and standard unwrapping, not a constructible error
implementation. No convenience API was added.

## 5. External clean-module test

The retained fixture is `testdata/external-consumer/main.go`. The opt-in
`TestExternalModule` creates an unrelated temporary module, runs `go mod init`,
downloads a requested version with `go get`, copies only the ordinary consumer
source, runs it, audits `go list -deps`, and rejects any `replace` directive.

Pre-tag validation downloaded pushed candidate
`v0.0.0-20260823015501-59084c9426a6` from outside the checkout with fresh build
and module caches. It compiled and printed:

```text
external-ok balances=75,75 duplicate=true files=[FORMAT wal.oct] format=1
```

The consumer used no relative repository path, internal import, fixture from the
module, generated artifact, Oct source, submodule, or undocumented environment
variable.

## 6. External lifecycle proof

The external program opens a default-configured database, creates accounts 1
and 2, transfers 25 from the first to the second, closes, reopens, reads balances
75 and 75, retries command ID `transfer-1`, observes `Duplicate=true`, and
observes no second transfer. It then closes cleanly. This is the authoritative
external-use proof; repository examples are supplemental.

## 7. Runtime/build-time Oct dependency

- **Is Oct required at runtime?** No.
- **Is Oct required at build time?** No.
- **Is Oct required only for model generation?** Oct was used historically to
  establish model lineage, but v0.1 consumers perform no generation.
- **Does v0.1 expose user-authored Oct models?** No.

The v0.1 product is the fixed account/transfer engine implemented in safe Go.
Oct, PostgreSQL, and TigerBeetle are not install prerequisites.

## 8. Module ZIP/package audit

The candidate and tag package the same commit. The downloaded candidate ZIP was
1,550,057 compressed bytes, 6,572,186 uncompressed bytes, and 614 entries. It
had zero case-colliding paths. The ignored local `bench.exe`, database
directories, source-tree caches, and untracked generated output were absent.

The single module retains historical experiment evidence and research code. The
largest entries are a 1.39 MB generated research snapshot source file and
TigerCompare profile/evidence files. At 1.55 MB compressed this is acceptable
for v0.1 and does not justify a speculative multi-module/archive restructure or
deleting research evidence.

`go list -deps` for the product resolves only the public package,
`internal/core`, and the Go standard library. It excludes pgx, TigerBeetle,
benchmark packages, `internal/researchengine`, and generated Oct model packages.
The repository module file still declares pgx and TigerBeetle for retained
research commands, so they appear in `go mod graph`; they are not compiled or
downloaded as production package dependencies by the clean consumer.

Tagged module checksums are:

```text
module: h1:Xf6qPfesi7evdfv7EeSwuJ3DPxMtvZOUIDNhN/B5Zpw=
go.mod: h1:TOohw9iQ47F7awwbGG/pwDnDWaijFB46E6DcaVwNhqE=
```

## 9. Platform/Go version policy

The minimum supported Go version is 1.23. The full suite passed with Go
1.23.12 on Windows. Release verification also passed with Go 1.26.2 on
Windows/amd64 and with Go 1.27.0-X on Linux/amd64 under WSL2. Race tests passed
on Windows and Linux/WSL2. Native Linux outside WSL2 and macOS are unverified;
the release does not claim they were tested.

## 10. Error/cancellation external proof

The external consumer proves `ErrorInvalidInput`, `ErrorCapacity`,
`ErrorCorruption`, and `ErrorClosed` with `errors.As`; duplicate retry is a
successful typed result rather than an error. Error text remains diagnostic,
not a programmatic contract.

An already-cancelled context returns an error matching `context.Canceled`. The
consumer proves the rejected command changes neither current state nor state
after another close/reopen. Documentation states the complementary durable
rule: cancellation is honored before admission; after admission an operation
runs to a definitive synchronized result, and an uncertain caller retries the
same command ID. No timing-sensitive cancellation test is used.

## 11. Storage directory proof

The canonical external database directory contained exactly:

```text
FORMAT
wal.oct
```

No snapshot was requested, so `snapshot.oct` was correctly absent. A requested
snapshot may add `snapshot.oct` and a transient `snapshot.oct.tmp`. No product
artifact contains an experiment, benchmark, C2, or milestone name.

## 12. Format compatibility policy

Format version 1 is independent of module version v0.1.0. Before 1.0, database
formats may change between minor releases. Automatic migration is not promised.
OctetDB does promise that incompatible format/model data is detected and
rejected rather than silently interpreted as current data.

## 13. Release notes/changelog

`CHANGELOG.md` defines the pre-1.0 semantic-version policy and records v0.1.0.
`docs/product/V0.1.0_RELEASE_NOTES.md` describes OctetDB, install command,
minimal code, v0.1 scope, durability, limitations, platform state, and bounded
performance evidence. The GitHub release uses those notes. Public performance
text identifies the production engine relationship, the one-replica
TigerBeetle control, non-equivalent guarantees, and non-general benchmark scope.

## 14. Release gate table

| Gate | Status |
| --- | --- |
| clean module download | Green |
| clean external compile | Green |
| clean external run | Green |
| restart durability | Green |
| duplicate retry | Green |
| public docs | Green |
| API audit | Green |
| module ZIP audit | Green |
| tests | Green |
| race | Green |
| vet | Green |
| format compatibility docs | Green |

Additional checks passed: `git diff --check`, module case-collision inspection,
public examples, Go 1.23 minimum-toolchain tests, Windows tests, and Linux/WSL2
tests. No failure was ignored.

## 15. Tag result

```text
commit SHA:             59084c9426a64e267d585e1608213dead3658e5e
tag:                    v0.1.0 (annotated and pushed)
module path:            github.com/yuechen-li-dev/octetdb
release Go version:     go1.26.2 windows/amd64
minimum Go version:     1.23
database format:        1 (accounts-v1, safe-go)
GitHub release:         https://github.com/yuechen-li-dev/OctetDB/releases/tag/v0.1.0
```

The tag was created only after the clean candidate, static-analysis, race,
documentation, API, and ZIP gates were green. The tag was not moved.

## 16. Post-tag clean install

Mandatory post-tag validation used completely fresh module and build caches,
`GOPROXY=https://proxy.golang.org,direct`, and the exact command dependency
`github.com/yuechen-li-dev/octetdb@v0.1.0`. The clean unrelated module compiled
and ran the canonical lifecycle fixture with no `replace` and produced the same
external success line. The Go proxy reports `v0.1.0` at the exact tag commit.

## 17. pkg.go.dev readiness/index status

The public package has a package comment, complete exported-name documentation,
a runnable public example, a recognized GPL-3.0 license, a valid public module
tag, and a module resolvable through `proxy.golang.org`. A direct pkg.go.dev
fetch was requested after publication. Indexing completed successfully: the
public v0.1.0 page returns HTTP 200 and renders the package synopsis, exported
API including `FormatVersion` and `Open`, version, and GPL-3.0 license.

## 18. Known v0.1 limitations

- Fixed account/transfer domain; no user-authored Oct model surface.
- Single process and replica; no directory lock, replication, or failover.
- No SQL, network protocol/server, migrations, or online backup API.
- Exact dedupe only within `DedupeHorizon`; older IDs may apply again.
- Pre-1.0 format changes may require explicit offline upgrade tooling later.
- Snapshot rename power-loss guarantees are weaker on Windows than POSIX.
- macOS and native Linux outside WSL2 are unverified.
- Historical research files and their declared tool dependencies remain in the
  same small module ZIP, but are absent from the product import graph.

## 19. Exactly one next recommendation

**PRODUCT-M2 — golden real-world integrations using a conventional Go HTTP stack.**
