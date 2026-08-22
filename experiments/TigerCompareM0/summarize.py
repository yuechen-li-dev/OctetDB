#!/usr/bin/env python3
"""Normalize TIGER-COMPARE-M0 evidence into the checked-in summary.json."""

from __future__ import annotations

import glob
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EVIDENCE = ROOT / "evidence"


def load(pattern: str) -> list[dict]:
    return [json.loads(Path(path).read_text()) for path in sorted(glob.glob(str(EVIDENCE / pattern)))]


def value(run: dict, path: str) -> float:
    current = run
    for part in path.split("."):
        current = current[part]
    return float(current)


def summarize(runs: list[dict]) -> dict:
    fields = {
        "operations_per_second": "result.operations_per_second",
        "p50_us": "result.p50_us",
        "p95_us": "result.p95_us",
        "p99_us": "result.p99_us",
        "p99_9_us": "result.p99_9_us",
        "alloc_bytes_per_op": "result.gc.bytes_per_op",
        "allocs_per_op": "result.gc.allocs_per_op",
        "gc_cpu_percent_capacity": "result.gc.gc_cpu_percent_of_go_capacity",
        "gc_cycles": "result.gc.gc_cycles",
        "max_pause_us": "result.gc.max_pause_us",
        "live_heap_bytes": "result.gc.live_heap_bytes",
        "client_rss_bytes": "result.gc.rss_bytes",
        "wal_bytes_per_op": "result.storage.wal_bytes_per_op",
        "commands_per_sync": "result.storage.commands_per_sync",
    }
    out = {"repetitions": len(runs)}
    for name, path in fields.items():
        values = [value(run, path) for run in runs]
        out[name] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
    cores = [value(run, "result.process_cpu_cores") + value(run, "result.external_process.utilized_cores") for run in runs]
    rss = [value(run, "result.gc.rss_bytes") + value(run, "result.external_process.rss_bytes") for run in runs]
    gc_process = []
    for run in runs:
        process_cpu = value(run, "result.process_cpu_seconds")
        gc_process.append(100 * value(run, "result.gc.gc_cpu_seconds") / process_cpu if process_cpu else 0)
    out["utilized_cores"] = {"median": statistics.median(cores), "min": min(cores), "max": max(cores)}
    out["operations_per_second_per_utilized_core"] = statistics.median(
        value(run, "result.operations_per_second") / core if core else 0 for run, core in zip(runs, cores)
    )
    out["total_rss_bytes"] = {"median": statistics.median(rss), "min": min(rss), "max": max(rss)}
    out["gc_cpu_percent_process_cpu"] = {"median": statistics.median(gc_process), "min": min(gc_process), "max": max(gc_process)}
    out["all_correct"] = all(run["result"]["correctness"]["conserved"] and run["result"]["correctness"]["duplicate_suppressed"] and run["result"]["correctness"]["rejected"] == 0 for run in runs)
    out["evidence"] = [Path(run.get("_path", "")).name for run in runs if run.get("_path")]
    return out


def with_paths(pattern: str) -> list[dict]:
    runs = []
    for path in sorted(glob.glob(str(EVIDENCE / pattern))):
        run = json.loads(Path(path).read_text())
        run["_path"] = path
        runs.append(run)
    return runs


def grouped(pattern: str, keys: tuple[str, ...]) -> dict:
    groups: dict[tuple, list[dict]] = {}
    for run in with_paths(pattern):
        key = tuple(run["config"][part] for part in keys)
        groups.setdefault(key, []).append(run)
    return {"/".join(map(str, key)): summarize(runs) for key, runs in sorted(groups.items())}


def parse_perf(path: Path) -> dict:
    out = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split(",")
        if len(fields) >= 3 and fields[0].isdigit():
            out[fields[2].split(":")[0]] = int(fields[0])
    if out.get("cycles"):
        out["instructions_per_cycle"] = out.get("instructions", 0) / out["cycles"]
        out["cache_misses_per_1000_instructions"] = 1000 * out.get("cache-misses", 0) / out.get("instructions", 1)
        out["branch_misses_per_1000_instructions"] = 1000 * out.get("branch-misses", 0) / out.get("instructions", 1)
    return out


batch = grouped("batch-*.json", ("lane", "offered_batch"))
primary = {lane: batch[f"{lane}/64"] for lane in ("tiger", "oct", "go")}
primary["postgres"] = summarize(with_paths("primary-postgres-*.json"))
memory = grouped("memory-*.json", ("lane", "durability"))
gc = {}
for run in with_paths("gc-*.json"):
    name = Path(run["_path"]).name
    gogc = name.split("gogc", 1)[1].split("-", 1)[0]
    gc.setdefault((run["config"]["lane"], gogc), []).append(run)
gc = {f"{key[0]}/GOGC={key[1]}": summarize(runs) for key, runs in sorted(gc.items())}

longrun = load("longrun-oct-300s.json")[0]
points = longrun["result"]["time_series"]
previous_gc = 0
gc_intervals, other_intervals = [], []
for point in points:
    target = gc_intervals if point["gc_cycles"] > previous_gc else other_intervals
    target.append(point["p99_us"])
    previous_gc = point["gc_cycles"]

storage_100k = json.loads((EVIDENCE / "octetdb-storage-100k.json").read_text())
recovery = json.loads((EVIDENCE / "octetdb-recovery.json").read_text())
perf = {path.stem: parse_perf(path) for path in sorted(EVIDENCE.glob("perf-*.csv"))}

go_memory = memory["go/memory"]["operations_per_second"]["median"]
oct_memory = memory["oct/memory"]["operations_per_second"]["median"]

summary = {
    "milestone": "TIGER-COMPARE-M0",
    "verdict": "Success",
    "manual_memory_thesis_verdict": "Partially supported / narrowed",
    "systems": {
        "tigerbeetle": {"version": "0.17.9", "commit": "cc1c06a924e49b11089c521b2209d34c92caaf18", "client": "tigerbeetle-go v0.17.9", "build": "official x86_64-linux release"},
        "octetdb": {"repository_commit": "3bfa3a7f3a71ec51a9cd3432f18c7b10c6e29f29", "architecture": "M2 frozen"},
        "go": "go1.27.0-X:nodwarf5 linux/amd64",
        "postgresql": "18.6 x86_64-pc-linux-gnu",
    },
    "primary_durable_batch_64": primary,
    "batch_curve": batch,
    "memory_diagnostics": memory,
    "gc_sensitivity": gc,
    "contention": grouped("contention-*.json", ("lane", "workload")),
    "population": grouped("population-*.json", ("lane", "accounts")),
    "large_population_gc": grouped("largegc-*.json", ("lane", "accounts")),
    "oct_semantic_tax": {
        "memory_throughput_shortfall_percent": 100 * (go_memory - oct_memory) / go_memory,
        "memory_reciprocal_service_time_oct_ns": 1e9 / oct_memory,
        "memory_reciprocal_service_time_go_ns": 1e9 / go_memory,
        "durable_batch_64_throughput_shortfall_percent": 100 * (primary["go"]["operations_per_second"]["median"] - primary["oct"]["operations_per_second"]["median"]) / primary["go"]["operations_per_second"]["median"],
    },
    "longrun": {
        "aggregate": longrun["result"],
        "points": len(points),
        "interval_ops_min": min(point["operations_per_second"] for point in points),
        "interval_ops_max": max(point["operations_per_second"] for point in points),
        "interval_p99_max_us": max(point["p99_us"] for point in points),
        "gc_interval_average_p99_us": statistics.mean(gc_intervals),
        "non_gc_interval_average_p99_us": statistics.mean(other_intervals),
    },
    "storage": {
        "octetdb_100k_ablation": storage_100k,
        "tigerbeetle_100k": (EVIDENCE / "storage-tiger-100k.storage.txt").read_text(),
        "tigerbeetle_1m": (EVIDENCE / "storage-tiger-1m.storage.txt").read_text(),
        "postgresql_100k": {"database_bytes_before": 7993023, "database_bytes_after": 51672767, "wal_bytes_including_1000_account_setup": 113259232},
    },
    "recovery": {
        "octetdb": recovery,
        "tigerbeetle_100k_forced_kill": (EVIDENCE / "recovery-tiger.txt").read_text(),
    },
    "hardware_counters": perf,
    "correctness": {"all_grouped_runs_passed": all(section["all_correct"] for section in list(batch.values()) + list(grouped("contention-*.json", ("lane", "workload")).values()) + list(grouped("population-*.json", ("lane", "accounts")).values()))},
}

(ROOT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
