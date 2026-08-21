# M2 evidence

Generated per-request/lane JSON is intentionally ignored. The normalized
results, methodology, outlier disclosure, and trace excerpts are retained in
[`docs/experiments/DBSCHED_M2.md`](../../../docs/experiments/DBSCHED_M2.md).

Reproduce the rotated authoritative runs with:

```powershell
go run ./cmd/bench -output experiments/M2/evidence/run-1 -lanes static,conflict,utility,agentic,conventional,batch
go run ./cmd/bench -output experiments/M2/evidence/run-2 -lanes conventional,utility,conflict,batch,agentic,static
go run ./cmd/bench -output experiments/M2/evidence/run-4 -lanes conflict,static,agentic,utility,batch,conventional
go run ./cmd/bench -output experiments/M2/evidence/ef-pair-1 -lanes conflict,utility
go run ./cmd/bench -output experiments/M2/evidence/ef-pair-2 -lanes utility,conflict
go run ./cmd/bench -output experiments/M2/evidence/ef-pair-3 -lanes conflict,utility
```

Run 3 (`batch,agentic,static,conflict,conventional,utility`) was retained during
analysis but excluded from aggregation because a machine-wide throughput
collapse began after the second lane; the report documents the evidence.
