# OCT-DB-TEMPLATES-M0 evidence

This directory contains the successful rerun evidence. The original Honest Stop control remains intact in `compiler-probes/` and `probe-results.txt`; it captured the pre-parametrics failures rather than a workaround target.

Current evidence:

- `jobs/` and `inventory/`: typed application compositions;
- `negative/`: webhook decision to keep default OctetDB;
- `w5/`: template/bespoke parity source, deterministic generation, benchmark, generated Go, and provenance;
- `llm/`: durable pointer to four isolated fresh-agent trials;
- `run_app.ps1`: temporary-project interpreted/compiled reproduction.

From this repository:

```powershell
$oct = "C:\path\to\oct"
./docs/product/evidence/OCT_DB_TEMPLATES_M0/run_app.ps1 -OctRepo $oct -Source ./docs/product/evidence/OCT_DB_TEMPLATES_M0/jobs/job_queue.octest -Execution interpreted
./docs/product/evidence/OCT_DB_TEMPLATES_M0/run_app.ps1 -OctRepo $oct -Source ./docs/product/evidence/OCT_DB_TEMPLATES_M0/jobs/job_queue.octest -Execution compiled
./docs/product/evidence/OCT_DB_TEMPLATES_M0/run_app.ps1 -OctRepo $oct -Source ./docs/product/evidence/OCT_DB_TEMPLATES_M0/inventory/inventory.octest -Execution interpreted
./docs/product/evidence/OCT_DB_TEMPLATES_M0/run_app.ps1 -OctRepo $oct -Source ./docs/product/evidence/OCT_DB_TEMPLATES_M0/inventory/inventory.octest -Execution compiled
./docs/product/evidence/OCT_DB_TEMPLATES_M0/w5/generate.ps1 -OctRepo $oct
go test ./docs/product/evidence/OCT_DB_TEMPLATES_M0/w5/generated -run TestTemplateAndBespokeW5ResultParity -v
go test ./docs/product/evidence/OCT_DB_TEMPLATES_M0/w5/generated -run '^$' -bench '^BenchmarkTemplateVersusBespokeW5$' -benchtime=1s -count=5
go test ./docs/product/evidence/OCT_DB_TEMPLATES_M0/w5/generated -run '^$' -bench '^BenchmarkTemplateVersusBespokeW5Reverse$' -benchtime=1s -count=5
```

The scripts copy canonical template source only into safely bounded temporary directories. No generated Go is manually edited and no template/runtime subsystem is involved.

OCT-TEMPLATE-CODEGEN-M0 adds a compiler regression that compares the two FLOWs after normalized MIR and normalized Go emission. W5's forward and reverse benchmark controls show that the former 11.7% apparent template penalty reverses when only helper/code placement changes; this is layout sensitivity, not evidence of either a template penalty or speedup.
