# Participant B integration notes

- Start UTC: 2026-08-23T05:08:17Z
- End UTC: 2026-08-23T05:11:50Z
- Scope: only this independent Go module. No product-source files, other participant directories, replace directives, local checkout imports, internal packages, or research packages were used.
- Intervention: none.

## Public evidence consulted

- `README.md`: established the canonical `OpenCatalog` / `Bucket` / `Dataset` / `Database.Mutate` route, the difference between dataset record identity and database-wide command identity, duplicate-callback semantics, the no-network-in-callback rule, default bounds, and single-process constraint.
- `docs/GETTING_STARTED.md`: resolved `Tx.Get`/`Tx.Put`, `RejectWithResult`, `DecodeResult`, close/reopen behavior, and the fact that a non-rejection callback error leaves the identity unrecorded.
- `docs/DURABILITY.md`: resolved that accepted and rejected decisions are synchronized WAL frames, that `Close` snapshots retained decisions, and that duplicate retry appends nothing.
- `docs/RECOVERY.md`: resolved the whole-directory/offline-backup requirement, crash replay/truncation behavior, catalog/snapshot/WAL ownership, and absence of a directory lock or online backup API.
- `CHANGELOG.md` and `docs/product/V0.2.0_RELEASE_NOTES.md`: confirmed v0.2.0 is the canonical catalog release and its stated limitations.
- `examples/quickstart/main.go`: supplied the runnable public pattern for catalog opening, dataset declaration, mutation, close/reopen, and duplicate verification.
- Exported GoDoc plus the downloaded `github.com/yuechen-li-dev/octetdb@v0.2.0` root-package source (`keyed.go`, `catalog.go`): verified exact exported type/field names and the public implementation behavior of `KeyedDecision`, `DecodeResult`, `RejectWithResult`, `Tx`, `DatasetOptions`, and `KeyedOptions`. No `internal` or research source was read.

## Implemented applications

- `webhook`: `Processor.Process` maps an external provider event ID to the stable command ID `webhook/v1/event/<eventID>`. The event ID is separately the `events` dataset record key. The accepted callback writes the completed `Delivery` and result in the same transaction. A retry decodes the prior decision and never invokes the callback; the test proves this before and after reopening.
- `lifecycle`: `Service` provides `Create`, `Pay`, `Ship`, and `Cancel` for `Created`, `Paid`, `Shipped`, and `Cancelled`. Its command identity is `order/v1/<orderID>/<action>/<external-requestID>`; the order ID itself remains only the `records` dataset key. Domain rejections use `RejectWithResult`, so repeated rejected requests return precisely the retained result and code.
- Payment email: `Pay` atomically writes `PaymentEmail` to an `payment-emails` outbox dataset with stable message ID `payment-receipt/v1/<orderID>`. Only after the payment decision is durable does `EmailSender.Send` run; a second mutation marks the outbox record sent. This avoids external work inside an OctetDB callback. It is deliberately at-least-once across a crash between provider send and the sent mark, so the stable message ID is the provider-side dedupe key. Tests use an in-memory sender and prove one ordinary successful send, no send for rejected double-pay commands, and no re-send after restart once marked sent.

## Edit and test record

1. Created module with `go mod init`, then `go get github.com/yuechen-li-dev/octetdb@v0.2.0`.
2. Implemented the webhook package and its close/reopen dedupe test.
3. Implemented lifecycle transitions, durable rejection decoding, post-commit outbox delivery, restart coverage, callback-error retry coverage, and concurrent conflicting-payment coverage.
4. Ran `gofmt -w webhook/processor.go webhook/processor_test.go lifecycle/service.go lifecycle/service_test.go`.
5. Ran `go test ./...`: passed on the first executed compile/test cycle (both packages).
6. Ran `go mod tidy`; `go.mod` directly requires exactly `github.com/yuechen-li-dev/octetdb v0.2.0` and has no `replace` directive.
7. Ran `go test -race ./...`: passed for both packages.

There were no executed compile errors, runtime/test failures, or API hallucinations. An initial handwritten test placeholder was corrected before executing tests and therefore was not an API failure. Approximate OctetDB-specific application LOC: 336 production lines and 229 test lines (excluding module metadata and these notes).

## Operational and schema answers discovered

- Backups: copy the complete database directory only while closed, or after application-coordinated quiescence and a snapshot; do not manage individual product files.
- Crashes: recovery replays complete synchronized WAL decisions and truncates only an incomplete final append; checked corruption fails closed.
- Two processes/handles: unsupported. Applications must ensure one process/handle owns a database directory; v0.2 supplies no directory lock.
- Snapshots: `Close` always writes a deterministic snapshot; a long-running process may call `Database.Snapshot` at an application-selected maintenance boundary. There is no background snapshot schedule.
- Adding an optional struct field: ordinary JSON decoding is used for stored values, so an absent newly-added optional field receives its Go zero value when decoding existing records. The dataset's `TypeIdentity` is itself a compatibility contract: keep it stable for backward-compatible additions, and use a deliberate new identity/database migration workflow for incompatible semantic changes. This is an inference from the public JSON encoding path and documented type-identity compatibility checks, not a product migration feature.

## Feature requests/workarounds

- There is no built-in durable external-effect dispatcher or exactly-once email integration. The implemented durable outbox plus stable provider message ID is the local workaround.
- There is no online backup, directory lock, automatic migration, or scheduled snapshots; callers must coordinate these operationally.
