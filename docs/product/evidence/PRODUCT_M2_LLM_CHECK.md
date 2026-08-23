# PRODUCT-M2 LLM legibility check

A fresh coding-agent context received only `README.md`, `doc.go`, exported
`go doc` output, and this requirement: build an idempotent webhook persistence
adapter that stores status/result, survives close/reopen, and returns the
original result on exact retry. It was explicitly denied implementation files,
internal packages, product reports, getting-started material, and existing
examples/integrations.

The agent created a separate scratch module outside the repository, importing
only `github.com/yuechen-li-dev/octetdb`. It used `OpenKeyed`, stable
`KeyedCommand.ID`, `SubmitKeyed`, `KeyedTx.Put`, `DecodeResult`, `GetKeyed`, and
`Close`. Its test proved that restart preserved the record and that a duplicate
did not run a replacement callback and returned the original result.

| Observation | Result |
| --- | --- |
| Incorrect assumptions | none |
| API hallucinations | none |
| Public docs searched | README, package docs, and symbol docs for keyed options/DB/transaction/command/decision/decode/rejection/error |
| Compile errors | none in source |
| Tooling errors | first `go test` requested module updates; `go mod tidy` resolved them |
| Human intervention | none |
| Final verification | `go test -v ./...` passed |

This is a bounded sanity check, not PRODUCT-M3's comparative human/LLM
benchmark. The only correction was normal Go module housekeeping; no source or
API-name correction was needed.
