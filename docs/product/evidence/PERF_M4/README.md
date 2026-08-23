# PERF-M4 evidence

This directory contains the reproducible evidence for the default-versus-
specialized positioning milestone. The primary harness is an independent Go
module pinned to `github.com/yuechen-li-dev/octetdb v0.2.0`; it therefore does
not resolve the current checkout as Lane B.

## Run

On the recorded Windows host, provide a PostgreSQL 17 service with `fsync=on`,
`synchronous_commit=on`, and `full_page_writes=on` at
`PERF_M4_POSTGRES_URL`, then run:

```powershell
./run_windows.ps1 -Suite all -Repetitions 3
```

The script builds once before measurement and runs systems separately. Every
result is JSON and contains configuration, latency distribution, process CPU,
RSS, Go heap/GC/allocation counters, relation/storage bytes, WAL position or
file bytes, records examined, and correctness checks. Setup and warmup are
excluded. Existing result files are never overwritten.

## Lane isolation

- `postgres`: pgx pool, ordinary SQL transactions, obvious primary/unique and
  discovery indexes, TCP to an external PostgreSQL service.
- `default`: released public OctetDB v0.2.0 only, canonical catalog API,
  `DefaultKeyedOptions`, safe Go and normal GC.
- `specialized`: exactly the default store plus generated Oct code. Mutation
  workloads remain S0 when profiling provides no honest compiler-owned hot
  path. W5 is S1: the durable Dataset is materialized on initialization and a
  generated FLOW performs filter/map/take/count. Default and compiled results
  are compared before and after measurement.

The generated file is produced, never hand edited, by running this from the
pinned Oct checkout:

```powershell
go run ./Experiments/PerfM4 <query.oct> <harness/specialized/generated.go>
```

TigerBeetle evidence is separate and W1-only because no other workload has an
honest TigerBeetle domain mapping.
