# Database Scheduler — DBSCHED-M0 through M7

DBSCHED-M7 begins OctetDB Write M0 and reaches **Meaningful progression**. Real
compiled Oct probes show that typed command/effect topology and persistent
flow-local boards work, but current boards are not shared authoritative state,
flows cannot receive post-construction messages, and compiled flows cannot be
restored. The experiment stops before hiding those gaps in a Go-owned write
engine. See [`docs/experiments/DBSCHED_M7.md`](docs/experiments/DBSCHED_M7.md)
and `experiments/M7/summary.json`.

DBSCHED-M5 begins Phase 2 with a compiled immutable read cache. A deterministic
typed Octagon catalog is validated as an Oct record table and emitted as fixed
Go arrays plus declared indexes. The report, reproducible evidence, Oct gaps,
and publication/runtime tradeoffs are in
[`docs/experiments/DBSCHED_M5.md`](docs/experiments/DBSCHED_M5.md) and
`experiments/M5`.

DBSCHED-M4 measures PostgreSQL feedback delay and evaluates a bounded Oct
observer, persistence, linear extrapolation, and one filtered reactive
concurrency actuator. Prediction did not beat persistence, so predictive
authority remains disabled and H is retained. See
`docs/experiments/DBSCHED_M4.md` and `experiments/M4/summary.json`.

DBSCHED-M3 adds persistent zero-allocation Oct policy controllers and compares
conventional and utility-scored priority/aging above the M2 mechanism. See
`docs/experiments/DBSCHED_M3.md` and `experiments/M3/summary.json`.

DBSCHED-M2 makes the M1 static conflict topology behavioral through typed
runtime keys, centralized conflict ownership, an Oct `when policy` ablation,
and bounded request automata/mailboxes. The report is
[`docs/experiments/DBSCHED_M2.md`](docs/experiments/DBSCHED_M2.md); generated M2
JSON diagnostics are intentionally ignored in favor of its normalized Markdown
evidence.

DBSCHED-M1 adds a four-lane experiment comparing runtime-derived scheduler
metadata with an Oct-derived fixed execution plan. Its report and retained
evidence are in `docs/experiments/DBSCHED_M1.md` and `experiments/M1`.

```powershell
docker compose up -d --wait
go test ./...
go run ./cmd/bench -output experiments/M1/evidence/run-local

# M4 nonstationary characterization and H/J/K0 lanes
go run ./cmd/bench -m4 -lanes h,j,k0 -output experiments/M4/evidence/run-local
```

Regenerate the M1 plan explicitly from the Oct repository:

```powershell
Set-Location ..\oct
go run ./cmd/oct test `
  ..\Database-Scheduler\experiments\M1\static-plan `
  --execution compiled
go run ./cmd/oct artifact `
  ..\Database-Scheduler\experiments\M1\static-plan\plan.octest `
  --output-root ..\Database-Scheduler
```

Ordinary `go build` and `go test` use the committed generated plan and do not
require Oct.

## DBSCHED-M0

DBSCHED-M0 is a bounded research harness comparing conventional Go/pgx access
to PostgreSQL with the same workload routed through an Oct-authored scheduler.
It intentionally tests only bounded admission, explicit control state, and
compatible read batching. Go's ordinary garbage collector and pgxpool remain
in both lanes.

## Run

```powershell
docker compose up -d --wait
go test ./...
go run ./cmd/bench -output experiments/M0/runs/latest
```

Use `-quick` for a roughly ten-second smoke run. The authoritative default run
uses the committed seed, schema, pool size, workload mix, and arrival phases in
`experiments/M0/config.json`. The command resets and seeds the same database
before correctness and before each lane.

## Regenerate the committed Oct artifact

Generation is explicit and is never invoked by `go build` or `go test`:

```powershell
Set-Location ..\oct
go run ./experimental/octemit generate `
  -input ..\Database-Scheduler\internal\scheduled\scheduler.oct `
  -output ..\Database-Scheduler\internal\scheduled\scheduler.generated.go `
  -package scheduled
```

Freshness can be checked by replacing `generate` with `check`. The generated
file is committed and must not be hand-edited.
