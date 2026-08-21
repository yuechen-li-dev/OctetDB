# Database Scheduler — DBSCHED-M0

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

