# Participant C notes

## Timing

- Start UTC: 2026-08-23T05:08:04.8925108Z
- End UTC: 2026-08-23T05:12:33.0498414Z

## Scope controls

- Authored files only under `docs/product/evidence/M3/participant-c` after cleanup.
- Independent module: `module participant-c`.
- Dependency: `github.com/yuechen-li-dev/octetdb v0.2.0`.
- No `replace` directives.
- Did not import local checkout paths, internal packages, research packages, or Oct packages.
- Did not inspect `docs/product/PRODUCT_M*` or any other participant directory.

## Public docs and examples consulted

- `README.md`
  - Resolved the canonical API hierarchy: `OpenCatalog`, `Database`, `Bucket`, `Dataset`, `Tx`, `Mutate`, `Get`, `ScanDataset`.
  - Resolved dataset-scoped record identity and database-wide command identity.
  - Resolved that Go users should query with deterministic scans, not SQL.
- `docs/GETTING_STARTED.md`
  - Resolved cross-dataset mutation shape with `Tx.Get`, `Tx.Put`, and `RejectWithResult`.
  - Resolved retry behavior: duplicate command IDs return the retained decision without rerunning the callback.
  - Resolved close/reopen expectations.
- `docs/DURABILITY.md`
  - Resolved that successful mutations write one synced WAL decision and apply atomically.
  - Resolved duplicate retry, snapshot, and close durability behavior.
- `docs/RECOVERY.md`
  - Resolved recovery behavior, incomplete WAL truncation, fail-closed corruption handling, backup guidance, and single-process limitation.
- `CHANGELOG.md`
  - Confirmed v0.2.0 public release features and limitations.
- `docs/product/V0.2.0_RELEASE_NOTES.md`
  - Confirmed catalog-keyed JSON format, exact dedupe horizon, deterministic scans, and known limitations.
- Public examples:
  - `examples/quickstart/main.go`: confirmed real use of `OpenCatalog`, dataset creation, `Mutate`, `DecodeResult`-compatible decisions, retry after restart, and `ScanDataset`.
  - `examples/minimal/main.go` and `examples/restart/main.go`: observed v0.1 compatibility examples only; not used for the task implementation.

## Exported API/source reading

- Used `go doc github.com/yuechen-li-dev/octetdb` after `go get github.com/yuechen-li-dev/octetdb@v0.2.0`.
- Used `go doc` for:
  - `Database.Mutate`
  - `Tx`
  - `KeyedDecision`
  - `KeyedCommand`
  - `KeyedRejection`
  - `Dataset`
  - `Bucket.Dataset`
  - `ScanDataset`
  - `DatasetOptions`
  - `KeyedOptions`
  - `DecodeResult`
- Verified downloaded module metadata with `go list -m -json github.com/yuechen-li-dev/octetdb`.
  - Downloaded module dir reported by Go: `C:\Users\yuech\go\pkg\mod\github.com\yuechen-li-dev\octetdb@v0.2.0`.
  - Module sum: `h1:AJFkUyS6GbM46L0QvF3UqgiIJA6EDSYooC0TfbZFJOU=`.
- No local product source files were opened for implementation details.

## Implemented APIs

- `task3`
  - `Open`, `Close`
  - `PutItem`, `GetItem`, `GetOrder`
  - `PlaceOrder`
  - `RequireApplied`
  - Public structs: `Item`, `Order`, `PlaceOrderRequest`, `PlaceOrderResult`, `Store`
- `task6`
  - `Open`, `Close`
  - `PutItem`, `PutOrder`, `GetItem`, `GetOrder`
  - `RunMixedWorkload`
  - Public structs: `Item`, `Order`, `WorkloadRequest`, `MutationResult`, `WorkloadResult`, `Store`

## Tests authored

- Task 3:
  - Idempotent retry does not decrement inventory twice.
  - Insufficient stock rejection does not create an order or change inventory.
  - Close/reopen preserves order, inventory, and retained duplicate decision.
  - Bounded concurrent conflicting writes: 12 one-unit orders race for stock 5; exactly 5 apply, 7 reject, final stock is 0, and exactly 5 orders exist.
- Task 6:
  - Deterministic first 20 low-stock items by ascending key.
  - Deterministic first 20 Paid orders by ascending key.
  - Point-read specific order.
  - Normal stock mutation after scans and point read.
  - Restart preserves scan/point-read/mutation behavior.
  - Same key in `inventory/items` and `commerce/orders` remains independent.

## Failed attempts and edit/test cycles

1. Created `participant-c` directory and initialized a module.
   - First command attempted to use the participant directory as working directory before it existed.
   - Error: `CreateProcess ... The directory name is invalid. (os error 267)`.
   - Fixed by creating the directory from the repository root, then running `go mod init` and `go get` inside it.
2. Initial patch accidentally created `task3` and `task6` at the repository root rather than under `participant-c`.
   - `go test ./...` from `participant-c` reported:
     - `GetFileAttributesEx task3\task3.go: The system cannot find the file specified.`
     - `go: warning: "./..." matched no packages`
     - `no packages to test`
   - Fixed by moving the four newly authored files into `docs/product/evidence/M3/participant-c/task3` and `task6`, then removing the empty accidental root directories.
3. Ran `gofmt` and `go test ./...`.
   - Result: pass.
4. Ran `go mod tidy` because the initial pre-code `go get` marked OctetDB as indirect.
   - Result: `go.mod` now directly requires `github.com/yuechen-li-dev/octetdb v0.2.0`.
5. Re-ran `go test ./...`.
   - Result: pass.

## API hallucinations attempted

- None for OctetDB symbols. `go doc` showed that there is no `Decision` symbol; the public decision type is `KeyedDecision`.
- No generic SQL/query layer was attempted.

## Interventions

- Zero user interventions.

## Approximate OctetDB-specific LOC

- Authored implementation LOC across `task3` and `task6`: 389.
- Direct `octetdb.` usage sites in implementation files: 38.
- Approximate OctetDB-specific LOC: about 45-60 lines, mostly open/catalog setup, `Mutate`, `Tx.Get`, `Tx.Put`, `RejectWithResult`, `Dataset.Get`, `ScanDataset`, and `DecodeResult`.

## Concepts used

- Catalog topology: database to bucket to dataset.
- Dataset-scoped keys, including same key in different datasets.
- Database-wide stable command IDs.
- Atomic cross-dataset mutation through one `Database.Mutate`.
- Durable accepted and rejected decisions.
- Exact idempotent retry by command ID.
- Typed point reads through `Dataset.Get`.
- Public deterministic typed scans through `ScanDataset`.
- Close/reopen recovery validation.

## Feature requests and workarounds

- No SQL, secondary indexes, joins, or query planner are available in v0.2.0, so Task 6 uses deterministic scans and application predicates.
- No online backup API is available; docs say to copy the whole directory only while closed or after coordinated quiescence and a snapshot.
- No directory lock exists; docs say the application must ensure at most one process or handle opens a database directory.
- No schema reflection or migration API is available. Docs expose `TypeIdentity` as the durable compatibility identity. Optional struct-field evolution is not explicitly documented; because values are ordinary JSON-decoded Go structs, missing fields would naturally decode to zero values, but compatibility policy should still be owned by the application through type identities.

## Operational/schema answers discoverable

- Backup files: do not copy or replace individual files as an application workflow; copy the entire database directory only while closed or after coordinated quiescence plus snapshot.
- Crashes: complete synced decisions replay; incomplete final WAL append is truncated; corrupt complete data fails closed with public error categories.
- Two-process open: unsupported; OctetDB is single-process/single-replica and has no directory lock.
- Snapshot timing: `Close` installs a deterministic snapshot. `Database.Snapshot` is for application-chosen maintenance boundaries; there is no background snapshot scheduler.
- Adding an optional struct field: not directly documented as a schema operation. Public docs say ordinary structs are JSON encoded and there is no schema reflection/migration; durable compatibility is expressed with dataset `TypeIdentity`.

## Final test result

Command:

```powershell
go test ./...
```

Output:

```text
ok  	participant-c/task3	(cached)
ok  	participant-c/task6	(cached)
```
