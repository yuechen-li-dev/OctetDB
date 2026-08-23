# PRODUCT-M0 repository map

This map was established before extraction and records both the original role
and the PRODUCT-M0 boundary. No research evidence was deleted.

| Path | Before PRODUCT-M0 | After PRODUCT-M0 |
| --- | --- | --- |
| root Go package | absent | **Production candidate:** public `octetdb` API, docs, and external-package integration tests |
| `internal/core/` | absent | **Production candidate:** canonical stdlib-only engine, WAL, snapshot, recovery, dedupe, and account model |
| `internal/m7write/` | **Research/experiment:** Oct, direct-Go, PostgreSQL, M1/M2, and C2 lanes mixed together | Renamed `internal/researchengine/`; **research/experiment** compatibility area, no production import |
| `internal/m7generated/` | **Generated artifact:** committed Oct output for M7 | Renamed `internal/model/`; **generated historical artifact**, no production import |
| `internal/scheduled/` | **Research/experiment** plus generated M0-M4 scheduler artifacts | unchanged research package |
| `internal/m5/` | **Research/experiment:** immutable publication/cache work | unchanged research package |
| `internal/m5compiled/` | **Research/experiment** and **generated artifact** for compiled publication | unchanged research package |
| `internal/baseline/` | **Research/experiment:** conventional control | unchanged research package |
| `internal/db/` | **Test fixture/research control:** PostgreSQL access and schema | unchanged non-production package |
| `internal/bench/` | **Benchmark tooling** | unchanged benchmark package |
| `internal/workload/` | **Test fixture/benchmark tooling:** deterministic workloads | unchanged non-production package |
| `internal/metrics/` | **Benchmark tooling:** experiment counters | unchanged non-production package |
| `cmd/` | **Benchmark tooling:** M5-M9, layout, recovery, trace, and TigerCompare commands | unchanged tooling; not imported by production |
| `examples/` | absent | **Documentation/test fixture:** two compiling public-API examples |
| `docs/` | **Documentation** dominated by milestone reports | Product README plus durability, recovery, repository-map, and PRODUCT-M0 docs; old reports remain **historical evidence** |
| `docs/experiments/` | **Historical evidence** | indexed explicitly as non-API research history |
| `experiments/M0` … `M9` | **Historical evidence**, configs, fixtures, and generated results | unchanged archive |
| `experiments/LayoutM0/` | **Historical evidence**, profiles, generated artifacts, scripts | unchanged archive |
| `experiments/TigerCompareM0/` | **Historical evidence**, profiles, environments, generated results | unchanged archive |
| `postgres/` | **Test fixture:** benchmark schema | unchanged fixture |
| `compose.yaml` | **Test fixture/tooling:** local PostgreSQL | unchanged fixture |
| `bench.exe` | **Generated artifact:** retained benchmark binary | unchanged; not a production dependency |

Production dependency direction is now:

```text
github.com/yuechen-li-dev/octetdb
    -> internal/core
        -> Go standard library only
```

The historical commands and packages may depend on each other, but neither the
root package nor `internal/core` imports them. Removing `experiments/`, `cmd/`,
and every other `internal/` directory leaves the product package conceptually
coherent; `go list -deps .` and `TestProductionDependencyDirection` enforce the
actual package boundary.
